/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Distributed under BSD 3-Clause license.                                   *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Illinois Institute of Technology.                        *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Hermes. The full Hermes copyright notice, including  *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the top directory. If you do not  *
 * have access to the file, you may request a copy from help@hdfgroup.org.   *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#include "prefetcher_factory.h"
#include "metadata_manager.h"
#include "singleton.h"

using hermes::api::Hermes;

namespace hermes {

bool Prefetcher::LogIoStat(Hermes *hermes, IoLogEntry &entry) {
  RpcContext *rpc = &hermes->rpc_;
  u32 target_node = HashToNode(hermes, entry);
  bool result = false;
  if (target_node == rpc->node_id) {
    auto prefetcher = Singleton<Prefetcher>::GetInstance();
    prefetcher->Log(entry);
    result = true;
  } else {
    result = RpcCall<bool>(rpc, target_node,
                           "LogIoStat", entry);
  }
  return result;
}

/**
 * HashToNode
 *
 * Determines which node an IoLogEntry should be keyed to.
 * We hash IoLogEntries to nodes using only the BucketId.
 * This way, IoLogEntries regarding a particular blob are
 * always placed on the same node.
 *
 * This approach assumes that prefetching decisions are based
 * solely on the bucket's access pattern, not patterns across-buckets.
 * */

size_t Prefetcher::HashToNode(Hermes *hermes, IoLogEntry &entry) {
  CommunicationContext *comm = &hermes->comm_;
  size_t hash = std::hash<u64>{}(entry.bkt_id_.as_int);
  return (hash % comm->num_nodes) + 1;
}

void Prefetcher::Log(IoLogEntry &entry) {
  LOG(INFO) << "Logging I/O stat" << std::endl;
  lock_.lock();
  if (log_.size() == max_length_) {
    log_.pop_front();
  }
  timespec_get(&entry.timestamp_, TIME_UTC);
  log_.emplace_back(entry);
  lock_.unlock();
}

float Prefetcher::EstimateBlobMovementTime(BlobId blob_id) {
  Arena arena = InitArenaAndAllocate(MEGABYTES(1));
  u32 *buffer_sizes = 0;
  BufferIdArray id_array = GetBufferIdsFromBlobId(&arena,
                                                  &hermes_->context_,
                                                  &hermes_->rpc_,
                                                  blob_id,
                                                  &buffer_sizes);
  std::vector<BufferID> buffer_ids(id_array.ids,
                                   id_array.ids + id_array.length);
  DestroyArena(&arena);
  std::vector<BufferInfo> buffer_info = GetBufferInfo(
      &hermes_->context_, &hermes_->rpc_, buffer_ids);

  float xfer_time = 0;
  for (auto &info : buffer_info) {
    xfer_time += static_cast<float>(info.size) / MEGABYTES(info.bandwidth_mbps);
    /*LOG(INFO) << "size: " << info.size / MEGABYTES(1)
              << " bw: " << info.bandwidth_mbps
              << " current total: " << xfer_time << std::endl;*/
  }
  xfer_time *= 2;  // x2 because movement reads, and then writes data
  return xfer_time;
}

bool HasOnlyDemotions(PrefetchDecision &decision, float &decay) {
  decay = std::numeric_limits<float>::infinity();
  if (decision.stats_.size() == 0) return false;
  for (auto &prefetch_stat : decision.stats_) {
    if (prefetch_stat.decay_ < 0) {
      return false;
    }
    decay = std::min(decay, prefetch_stat.decay_);
  }
  return true;
}

void Prefetcher::CalculateBlobScore(struct timespec &ts,
                                    PrefetchDecision &decision) {
  float est_xfer_time = decision.est_xfer_time_;
  decision.new_score_ = -1;
  decision.queue_later_ = false;

  float decay;
  if (HasOnlyDemotions(decision, decay)) {
    decision.new_score_ = GetBlobImportanceScore(&hermes_->context_,
                                                 &hermes_->rpc_,
                                                 decision.blob_id_);
    decision.new_score_ *= decay;
    if (decision.new_score_ < 0) {
      decision.new_score_ = 0;
    }
    decision.decay_ = true;
    return;
  }

  for (auto &prefetch_stat : decision.stats_) {
    if (prefetch_stat.decay_ >= 0) continue;
    // Wait until the I/O for this Get seems to have completed
    /*float time_left_on_io = prefetch_stat.TimeLeftOnIo(
        est_xfer_time, &ts);
    LOG(INFO) << "Blob id: " << decision.blob_id_.as_int
              << " Time left before starting prefetch: "
              << time_left_on_io << std::endl;
    if (time_left_on_io > 0) {
      decision.queue_later_ = true;
      continue;
    }*/
    float next_access_sec = prefetch_stat.TimeToNextIo(&ts);
    LOG(INFO) << "Next access sec: " << next_access_sec << std::endl;
    LOG(INFO) << "Est xfer time : " << est_xfer_time << std::endl;
    // if (next_access_sec < est_xfer_time) continue;
    float max_access_wait = std::max(max_wait_xfer_*est_xfer_time,
                                    max_wait_sec_);
    LOG(INFO) << "Max access wait: " << max_access_wait << std::endl;
    float est_wait = (next_access_sec - est_xfer_time);
    if (est_wait > max_access_wait) {
      decision.queue_later_ = true;
      continue;
    }
    float score = (max_access_wait - est_wait) / max_access_wait;
    if (score > decision.new_score_) {
      decision.new_score_ = score;
    }
  }
}

void Prefetcher::Process() {
  // Group log by I/O hint
  lock_.lock();
  std::unordered_map<PrefetchHint, std::list<IoLogEntry>> hint_logs;
  for (auto &entry : log_) {
    hint_logs[entry.pctx_.hint_].emplace_back(entry);
  }
  log_.erase(log_.begin(), log_.end());
  lock_.unlock();
  if (hint_logs.size() == 0 && queue_later_.size() == 0) {
    return;
  }

  // Based on the current log, determine what blobs to prefetch
  PrefetchSchema schema;
  for (auto &[hint, hint_log] : hint_logs) {
    auto algorithm = PrefetcherFactory::Get(hint);
    algorithm->Process(hint_log, schema);
  }

  // Take into consideration old prefetching decisions
  for (auto &[blob_id, decision] : queue_later_) {
    schema.emplace(decision);
  }
  queue_later_.erase(queue_later_.begin(), queue_later_.end());

  // Get the current time
  struct timespec ts;
  timespec_get(&ts, TIME_UTC);

  // Calculate new blob scores
  for (auto &[blob_id, decision] : schema) {
    if (decision.est_xfer_time_ == -1) {
      decision.est_xfer_time_ = EstimateBlobMovementTime(blob_id);
    }
    CalculateBlobScore(ts, decision);
    if (decision.new_score_ < 0) {
      if (decision.queue_later_) {
        queue_later_.emplace(blob_id, decision);
      }
    }
  }

  // Process decay blobs first (make room)
  for (auto &[blob_id, decision] : schema) {
    if (!decision.decay_) { continue; }
    LOG(INFO) << "Decaying Blob: " << blob_id.as_int
              << " to score: " << decision.new_score_ << std::endl;
    OrganizeBlob(&hermes_->context_, &hermes_->rpc_,
                 decision.bkt_id_, decision.blob_name_,
                 epsilon_, decision.new_score_);
  }

  // TODO(llogan): a hack to help BORG shuffle data
  struct timespec ts2;
  while (PrefetchStat::DiffTimespec(&ts2, &ts) < 1) {
    timespec_get(&ts2, TIME_UTC);
    ABT_thread_yield();
  }

  // Calculate new blob scores
  for (auto &[blob_id, decision] : schema) {
    if (decision.decay_) { continue; }
    if (decision.new_score_ < 0) { continue; }
    LOG(INFO) << "Prefetching bkt_id: " << decision.bkt_id_.as_int
              << " blob_id: " << decision.blob_name_
              << " score: " << decision.new_score_ << std::endl;
    OrganizeBlob(&hermes_->context_, &hermes_->rpc_,
                 decision.bkt_id_, decision.blob_name_,
                 epsilon_, 1.0);
  }
}

}  // namespace hermes
