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

#include "buffer_organizer.h"
#include "metadata_manager.h"
#include "borg_io_clients/borg_io_client_factory.h"
#include "hermes.h"
#include "bucket.h"

namespace hermes {

/**====================================
 * BORG I/O thread manager
 * ===================================*/

/** Spawn a thread for re-organizing blobs */
void BorgIoThreadManager::SpawnBlobMonitor() {
  auto flush_scheduler = [](void *args) {
    HILOG(kDebug, "Blob re-organization thread has started")
    (void) args;
    auto borg = &HERMES->borg_;
    while (HERMES_THREAD_MANAGER->Alive()) {
      borg->LocalAnalyzeBlobs();
      tl::thread::self().sleep(*HERMES->rpc_.server_engine_,
                               HERMES->server_config_.borg_.blob_reorg_period_);
    }
    HERMES_BORG_IO_THREAD_MANAGER->Join();
    HILOG(kDebug, "Blob re-organization thread has stopped")
  };
  HERMES_THREAD_MANAGER->Spawn(flush_scheduler);
}

/** Spawn the enqueuing thread */
void BorgIoThreadManager::SpawnFlushMonitor() {
  auto flush_scheduler = [](void *args) {
    HILOG(kDebug, "Flushing scheduler thread has started")
    (void) args;
    auto borg = &HERMES->borg_;
    while (HERMES_THREAD_MANAGER->Alive()) {
      borg->LocalEnqueueFlushes();
      tl::thread::self().sleep(*HERMES->rpc_.server_engine_,
                               HERMES->server_config_.borg_.flush_period_);
    }
    HERMES_BORG_IO_THREAD_MANAGER->Join();
    HILOG(kDebug, "Flush scheduler thread has stopped")
  };
  HERMES_THREAD_MANAGER->Spawn(flush_scheduler);
}

/** Spawn the flushing I/O threads */
void BorgIoThreadManager::SpawnFlushWorkers(int num_threads) {
  // Define flush worker thread function
  // The function will continue working until all pending flushes have
  // been processed
  auto flush = [](void *params) {
    BufferOrganizer *borg = &HERMES->borg_;
    int *id = reinterpret_cast<int*>(params);
    BorgIoThreadQueue &bq = (*borg->queues_)[*id];
    BorgIoThreadQueueInfo& bq_info = bq.GetSecond();
    _BorgIoThreadQueue& queue = bq.GetFirst();
    HILOG(kDebug, "Flushing worker {} has started", bq_info.id_)
    while (HERMES_BORG_IO_THREAD_MANAGER->Alive() ||
          (!HERMES_BORG_IO_THREAD_MANAGER->Alive() && bq_info.load_)) {
      borg->LocalProcessFlushes(bq_info, queue);
      tl::thread::self().sleep(*HERMES->rpc_.server_engine_, 25);
    }
    HILOG(kDebug, "Flushing worker {} has stopped", bq_info.id_)
  };

  // Create the flushing threads
  for (int i = 0; i < num_threads; ++i) {
    BorgIoThreadQueue &bq = (*queues_)[i];
    BorgIoThreadQueueInfo& bq_info = bq.GetSecond();
    bq_info.id_ = i;
    bq_info.load_ = 0;
    HERMES_THREAD_MANAGER->Spawn(flush, &bq_info.id_);
  }
}

/** Wait for flushing to complete */
void BorgIoThreadManager::WaitForFlush() {
  HILOG(kDebug, "Waiting for all flushing to complete {}",
        HERMES->rpc_.node_id_)
  while (IsFlushing()) {
    tl::thread::self().sleep(*HERMES->rpc_.server_engine_, 5);
  }
  HILOG(kDebug, "Finished flushing {}",
        HERMES->rpc_.node_id_)
}

/**====================================
 * SHM Init
 * ===================================*/

/**
 * Initialize the BORG
 * REQUIRES mdm to be initialized already.
 * */
void BufferOrganizer::shm_init(hipc::ShmArchive<BufferOrganizerShm> &header,
                               hipc::Allocator *alloc) {
  shm_deserialize(header);

  // Initialize device information
  for (TargetInfo &target : (*mdm_->targets_)) {
    DeviceInfo &dev_info =
        (*mdm_->devices_)[target.id_.GetDeviceId()];
    if (dev_info.mount_dir_->size() == 0) {
      dev_info.io_api_ = IoInterface::kRam;
    } else {
      dev_info.io_api_ = IoInterface::kPosix;
    }
    auto io_client = borg::BorgIoClientFactory::Get(dev_info.io_api_);
    io_client->Init(dev_info);
  }

  // Print out device info
  mdm_->PrintDeviceInfo();

  // Spawn the thread for flushing blobs
  int num_threads = HERMES->server_config_.borg_.num_threads_;
  HSHM_MAKE_AR((*header).queues_, alloc, num_threads)
  HERMES_BORG_IO_THREAD_MANAGER->queues_ = queues_;
  HERMES_BORG_IO_THREAD_MANAGER->SpawnBlobMonitor();
  HERMES_BORG_IO_THREAD_MANAGER->SpawnFlushMonitor();
  HERMES_BORG_IO_THREAD_MANAGER->SpawnFlushWorkers(num_threads);
}

/**====================================
 * SHM Deserialization
 * ===================================*/

/** Deserialize the BORG from shared memory */
void BufferOrganizer::shm_deserialize(
    hipc::ShmArchive<BufferOrganizerShm> &header)  {
  mdm_ = &HERMES->mdm_;
  rpc_ = &HERMES->rpc_;
  queues_ = (*header).queues_.get();
  HERMES_BORG_IO_THREAD_MANAGER->queues_ = queues_;
}

/**====================================
 * Destructors
 * ===================================*/

/** Finalize the BORG */
void BufferOrganizer::shm_destroy_main() {
  queues_->shm_destroy();
}

/**====================================
 * BORG Methods
 * ===================================*/

/** Stores a blob into a set of buffers */
RPC void BufferOrganizer::LocalPlaceBlobInBuffers(
    const Blob &blob, std::vector<BufferInfo> &buffers) {
  AUTO_TRACE(1)
  size_t blob_off = 0;
  for (BufferInfo &buffer_info : buffers) {
    if (buffer_info.tid_.GetNodeId() != mdm_->rpc_->node_id_) {
      continue;
    }
    TIMER_START("DeviceInfo")
    DeviceInfo &dev_info =
        (*mdm_->devices_)[buffer_info.tid_.GetDeviceId()];
    if (buffer_info.t_off_ + buffer_info.blob_size_ >
        dev_info.capacity_) {
      HELOG(kFatal, "Out of bounds: attempting to write to offset: {} / {} "
            "on device {}: {}",
            buffer_info.t_off_ + buffer_info.blob_size_,
            dev_info.capacity_,
            buffer_info.tid_.GetDeviceId(),
            dev_info.mount_point_->str())
    }
    auto io_client = borg::BorgIoClientFactory::Get(dev_info.io_api_);
    TIMER_END()

    TIMER_START("IO")
    bool ret = io_client->Write(dev_info,
                                blob.data() + blob_off,
                                buffer_info.t_off_,
                                buffer_info.blob_size_);
    TIMER_END()
    blob_off += buffer_info.blob_size_;
    if (!ret) {
      mdm_->PrintDeviceInfo();
      HELOG(kFatal, "Could not perform I/O in BORG."
            " Writing to target ID:"
            " (node_id: {}, tgt_id: {}, dev_id: {},"
            " t_off: {}, blob_size: {})",
            buffer_info.tid_.GetNodeId(),
            buffer_info.tid_.GetIndex(),
            buffer_info.tid_.GetDeviceId(),
            buffer_info.t_off_,
            buffer_info.blob_size_)
    }
  }
}

/** Globally store a blob into a set of buffers */
void BufferOrganizer::GlobalPlaceBlobInBuffers(
    const Blob &blob, std::vector<BufferInfo> &buffers) {
  AUTO_TRACE(1)
  // Get the nodes to transfer buffers to
  size_t total_size;
  auto unique_nodes = BufferPool::GroupByNodeId(buffers, total_size);

  // Send the buffers to each node
  for (auto &[node_id, size] : unique_nodes) {
    if (NODE_ID_IS_LOCAL(node_id)) {
      LocalPlaceBlobInBuffers(blob, buffers);
    } else {
      rpc_->IoCall<void>(
          node_id, "RpcPlaceBlobInBuffers",
          IoType::kWrite, blob.data(), blob.size(),
          blob.size(), buffers);
    }
  }
}

/** Stores a blob into a set of buffers */
RPC void BufferOrganizer::LocalReadBlobFromBuffers(
    Blob &blob, std::vector<BufferInfo> &buffers) {
  AUTO_TRACE(1)
  size_t blob_off = 0;
  for (BufferInfo &buffer_info : buffers) {
    if (buffer_info.tid_.GetNodeId() != mdm_->rpc_->node_id_) {
      continue;
    }
    DeviceInfo &dev_info =
        (*mdm_->devices_)[buffer_info.tid_.GetDeviceId()];
    if (buffer_info.t_off_ + buffer_info.blob_size_ >
        dev_info.capacity_) {
      HELOG(kFatal, "Out of bounds: attempting to read from offset: {} / {}"
            " on device {}: {}",
            buffer_info.t_off_ + buffer_info.blob_size_,
            dev_info.capacity_,
            buffer_info.tid_.GetDeviceId(),
            dev_info.mount_point_->str())
    }
    auto io_client = borg::BorgIoClientFactory::Get(dev_info.io_api_);
    bool ret = io_client->Read(dev_info,
                               blob.data() + blob_off,
                               buffer_info.t_off_,
                               buffer_info.blob_size_);
    blob_off += buffer_info.blob_size_;
    if (!ret) {
      HELOG(kFatal, "Could not perform I/O in BORG."
                    " reading from target ID:"
                    " (node_id: {}, tgt_id: {}, dev_id: {},"
                    " t_off: {}, blob_size: {})",
            buffer_info.tid_.GetNodeId(),
            buffer_info.tid_.GetIndex(),
            buffer_info.tid_.GetDeviceId(),
            buffer_info.t_off_,
            buffer_info.blob_size_)
    }
  }
}

/** The Global form of ReadBLobFromBuffers */
Blob BufferOrganizer::GlobalReadBlobFromBuffers(
    std::vector<BufferInfo> &buffers) {
  AUTO_TRACE(1)
  // Get the nodes to transfer buffers to
  size_t total_size = 0;
  auto unique_nodes = BufferPool::GroupByNodeId(buffers, total_size);

  // Send the buffers to each node
  std::vector<Blob> blobs;
  blobs.reserve(unique_nodes.size());
  for (auto &[node_id, size] : unique_nodes) {
    blobs.emplace_back(size);
    if (NODE_ID_IS_LOCAL(node_id)) {
      LocalReadBlobFromBuffers(blobs.back(), buffers);
    } else {
      rpc_->IoCall<void>(
          node_id, "RpcReadBlobFromBuffers",
          IoType::kRead, blobs.back().data(), size,
          size, buffers);
    }
  }

  // If the blob was only on one node
  if (unique_nodes.size() == 1) {
    return std::move(blobs.back());
  }

  // Merge the blobs at the end
  hapi::Blob blob(total_size);
  for (size_t i = 0; i < unique_nodes.size(); ++i) {
    auto &[node_id, size] = unique_nodes[i];
    auto &tmp_blob = blobs[i];
    size_t tmp_blob_off = 0;
    for (BufferInfo &info : buffers) {
      if (info.tid_.GetNodeId() != node_id) {
        continue;
      }
      memcpy(blob.data() + info.blob_off_,
             tmp_blob.data() + tmp_blob_off,
             info.blob_size_);
      tmp_blob_off += info.blob_size_;
    }
  }
  return blob;
}

/** Re-organize blobs based on a score */
void BufferOrganizer::GlobalOrganizeBlob(const std::string &bucket_name,
                                         const std::string &blob_name,
                                         float score) {
  AUTO_TRACE(1)
  auto bkt = HERMES->GetBucket(bucket_name);
  BlobId blob_id;
  bkt.GetBlobId(blob_name, blob_id);
  float blob_score = bkt.GetBlobScore(blob_id);
  GlobalOrganizeBlob(bkt, blob_name, blob_id, blob_score, score);
}

/** Re-organize blobs based on a score */
void BufferOrganizer::GlobalOrganizeBlob(hapi::Bucket &bkt,
                                         const std::string &blob_name,
                                         BlobId &blob_id,
                                         float blob_score,
                                         float score) {
  AUTO_TRACE(1);
  Context ctx;

  HILOG(kDebug, "Changing blob score from: {} to {}", blob_score, score)

  // Skip organizing if below threshold
  if (abs(blob_score - score) < .05) {
    return;
  }

  // Lock the blob to ensure it doesn't get modified
  bkt.LockBlob(blob_id, MdLockType::kExternalWrite);

  // Get the blob
  hapi::Blob blob;
  bkt.Get(blob_id, blob, ctx);

  // Re-emplace the blob with new score
  BlobId tmp_id;
  ctx.blob_score_ = score;
  bkt.Put(blob_name, blob, tmp_id, ctx);

  // Unlock the blob
  bkt.UnlockBlob(blob_id, MdLockType::kExternalWrite);
}

/**====================================
 * BORG Flushing methods
 * ===================================*/

/** Find blobs which should be re-organized */
void BufferOrganizer::LocalAnalyzeBlobs() {
  auto mdm = &HERMES->mdm_;
  ScopedRwReadLock blob_map_lock(mdm->header_->lock_[kBlobMapLock],
                                 kBORG_LocalEnqueueFlushes);
  float recency_min = HERMES->server_config_.borg_.recency_min_;
  float recency_max = HERMES->server_config_.borg_.recency_max_;
  float freq_max = HERMES->server_config_.borg_.freq_max_;
  float freq_min = HERMES->server_config_.borg_.freq_min_;

  // Only re-organize if there's a capacity trigger
  bool is_below_thresh = false;
  auto targets = mdm->LocalGetTargetInfo();
  for (TargetInfo &target : targets) {
    DeviceInfo &dev_info =
      (*mdm_->devices_)[target.id_.GetDeviceId()];
    float rem_cap = (float) target.rem_cap_ / (float)target.max_cap_;
    if (rem_cap < dev_info.borg_min_thresh_) {
      is_below_thresh = true;
    }
  }
  if (!is_below_thresh) {
    return;
  }

  u64 time = BlobInfo::GetTimeFromStartNs();
  for (hipc::pair<BlobId, BlobInfo>& blob_p : *mdm->blob_map_) {
    BlobInfo &blob_info = blob_p.GetSecond();
    // Get the recency score [0, 1]
    float last_access_elapse = (float)(time - blob_info.last_access_);
    float recency_score;
    if (last_access_elapse <= recency_min) {
      recency_score = 1;
    } else if (last_access_elapse >= recency_max) {
      recency_score = 0;
    } else {
      recency_score = (last_access_elapse - recency_min) /
        (recency_max - recency_min);
      recency_score = 1 - recency_score;
    }

    // Get the frequency score [0, 1]
    float freq_score;
    float freq = (float)blob_info.access_freq_;
    if (freq <= freq_min) {
      freq_score = 0;
    } else if (freq >= freq_max) {
      freq_score = 1;
    } else {
      freq_score = (freq - freq_min) / (freq_max - freq_min);
    }

    // Update the current blob score
    auto bkt = HERMES->GetBucket(blob_info.tag_id_);
    GlobalOrganizeBlob(bkt,
                       blob_info.name_->str(),
                       blob_info.blob_id_,
                       blob_info.score_,
                       std::max(freq_score, recency_score));
  }
}

/** Flush all blobs registered in this daemon */
void BufferOrganizer::LocalEnqueueFlushes() {
  auto mdm = &HERMES->mdm_;
  // Acquire the read lock on the blob map
  ScopedRwReadLock blob_map_lock(mdm->header_->lock_[kBlobMapLock],
                                 kBORG_LocalEnqueueFlushes);
  // Begin checking for blobs which need flushing
  size_t count = 0;
  for (hipc::pair<BlobId, BlobInfo>& blob_p : *mdm->blob_map_) {
    BlobId &blob_id = blob_p.GetFirst();
    BlobInfo &blob_info = blob_p.GetSecond();
    // Verify that flush is needing to happen
    if (blob_info.mod_count_ == blob_info.last_flush_) {
      continue;
    }
    // Check if bucket has flush trait
    TagId &bkt_id = blob_info.tag_id_;
    size_t blob_size = blob_info.blob_size_;
    std::vector<Trait*> traits = HERMES->GetTraits(bkt_id,
                                                   HERMES_TRAIT_FLUSH);
    if (traits.size() == 0) {
      continue;
    }
    // Schedule the blob on an I/O worker thread
    HERMES_BORG_IO_THREAD_MANAGER->Enqueue(bkt_id, blob_id, blob_size,
                                           std::move(traits));
    count += 1;
  }
  if (count) {
    HILOG(kDebug, "Flushing {} blobs", count);
  }
}

/** Actually process flush operations */
void BufferOrganizer::LocalProcessFlushes(
    BorgIoThreadQueueInfo &bq_info,
    _BorgIoThreadQueue& queue) {
  // Begin flushing
  ScopedRwWriteLock flush_lock(mdm_->header_->lock_[kFlushLock],
                               kBORG_LocalProcessFlushes);
  // Process tasks
  auto entry = hipc::make_uptr<BorgIoTask>();
  while (!queue.pop(*entry).IsNull()) {
    BorgIoTask &task = *entry;
    HILOG(kDebug, "Attempting to flush blob {}", task.blob_id_);
    Blob blob;

    // Verify the blob exists and then read lock it
    auto iter = mdm_->blob_map_->find(task.blob_id_);
    if (iter.is_end()) {
      bq_info.load_.fetch_sub(task.blob_size_);
      HILOG(kDebug, "Finished BORG task for blob {} and load {}",
            task.blob_id_, bq_info.load_.load())
      continue;
    }
    hipc::pair<BlobId, BlobInfo>& blob_info_p = *iter;
    BlobInfo &blob_info = blob_info_p.GetSecond();
    std::string blob_name = blob_info.name_->str();

    // Verify that flush is needing to happen
    if (blob_info.mod_count_ == blob_info.last_flush_) {
      bq_info.load_.fetch_sub(task.blob_size_);
      HILOG(kDebug, "Finished BORG task for blob {} and load {}",
            task.blob_id_, bq_info.load_.load())
      continue;
    }
    size_t last_flush = blob_info.mod_count_;
    blob_info.last_flush_ = last_flush;

    // Get the current blob from Hermes
    api::Bucket bkt = HERMES->GetBucket(task.bkt_id_);
    bkt.Get(task.blob_id_, blob, bkt.GetContext());
    HILOG(kDebug, "Flushing blob {} ({}) of size {}",
          blob_name,
          task.blob_id_,
          blob.size())
    FlushTraitParams trait_params;
    for (Trait *trait : task.traits_) {
      trait_params.blob_ = &blob;
      trait_params.blob_name_ = blob_name;
      trait_params.bkt_ = &bkt;
      trait->Run(HERMES_TRAIT_FLUSH, &trait_params);
    }

    // Dequeue
    bq_info.load_.fetch_sub(task.blob_size_);
    HILOG(kDebug, "Finished BORG task for blob {} and load {}",
          task.blob_id_, bq_info.load_.load())
  }
}

/** Barrier for all flushing to complete */
void BufferOrganizer::LocalWaitForFullFlush() {
  HILOG(kInfo, "Full synchronous flush on node {}", rpc_->node_id_)
  LocalEnqueueFlushes();
  HERMES_BORG_IO_THREAD_MANAGER->WaitForFlush();
  HILOG(kInfo, "Finished synchronous flush on node {}", rpc_->node_id_)
}

/** Barrier for all I/O in Hermes to flush */
void BufferOrganizer::GlobalWaitForFullFlush() {
  for (int i = 0; i < (int)rpc_->hosts_.size(); ++i) {
    int node_id = i + 1;
    HILOG(kInfo, "Wait for flush on node {}", node_id)
    rpc_->Call<bool>(node_id, "RpcWaitForFullFlush");
  }
}

}  // namespace hermes
