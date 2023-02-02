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

#include "sequential.h"
#include "singleton.h"
#include "api/bucket.h"
#include <mapper/abstract_mapper.h>

using hermes::adapter::BlobPlacement;

namespace hermes {

void SequentialPrefetcher::Process(std::list<IoLogEntry> &log,
                                   PrefetchSchema &schema) {
  auto prefetcher = Singleton<Prefetcher>::GetInstance();

  // Increase each unique_bucket access count
  for (auto &entry : log) {
    UniqueBucket id(entry.vbkt_id_, entry.bkt_id_);
    auto &state = state_[id];
    if (state.count_ == 0) {
      state.first_access_ = entry.timestamp_;
    }
    ++state.count_;
  }

  // Append time estimates and read-aheads to the schema
  for (auto &entry : log) {
    UniqueBucket id(entry.vbkt_id_, entry.bkt_id_);
    auto &state = state_[id];
    float cur_runtime = PrefetchStat::DiffTimespec(&entry.timestamp_,
                                                   &state.first_access_);
    float avg_time = cur_runtime / state.count_;
    if (avg_time == 0) {
      avg_time = prefetcher->max_wait_sec_ / 2.0;
    }
    LOG(INFO) << "Avg time: " << avg_time << std::endl;

    // Read-ahead the next few blobs
    BlobId cur_id = entry.blob_id_;
    for (int i = 0; i < entry.pctx_.read_ahead_; ++i) {
      PrefetchDecision decision;
      decision.bkt_id_ = entry.bkt_id_;
      if (IsNullVBucketId(entry.vbkt_id_)) {
        GetNextFromBucket(entry, decision, cur_id);
      } else {
        GetNextFromVbucket(entry, decision, cur_id);
      }
      if (IsNullBlobId(decision.blob_id_)) {
        break;
      }
      decision.AddStat((i+1)*avg_time, entry.timestamp_);
      schema.emplace(decision);
      cur_id = decision.blob_id_;
    }

    // Demote the previous blobs
    cur_id = entry.blob_id_;
    for (int i = 0; i < entry.pctx_.read_ahead_; ++i) {
      PrefetchDecision decision;
      if (IsNullVBucketId(entry.vbkt_id_)) {
        GetPriorFromBucket(entry, decision, cur_id);
      } else {
        GetPriorFromVbucket(entry, decision, cur_id);
      }
      if (IsNullBlobId(decision.blob_id_)) {
        break;
      }
      decision.AddStat(entry.pctx_.decay_);
      schema.emplace(decision);
      cur_id = decision.blob_id_;
    }
  }
}

void SequentialPrefetcher::GetNextFromBucket(IoLogEntry &entry,
                                             PrefetchDecision &decision,
                                             BlobId &cur_id) {
  // Get current blob name
  auto prefetcher = Singleton<Prefetcher>::GetInstance();
  auto hermes = prefetcher->hermes_;
  std::string blob_name = GetBlobNameFromId(&hermes->context_,
                                              &hermes->rpc_,
                                              cur_id);

  // Get next blob name
  BlobPlacement p;
  p.DecodeBlobName(blob_name);
  p.page_ += 1;
  decision.blob_name_ = p.CreateBlobName();

  // Get the next blob ID from the bucket
  decision.blob_id_ = GetBlobId(&hermes->context_, &hermes->rpc_,
                                decision.blob_name_, entry.bkt_id_,
                                false);
}

void SequentialPrefetcher::GetNextFromVbucket(IoLogEntry &entry,
                                              PrefetchDecision &decision,
                                              BlobId &cur_id) {
  // TODO(llogan): Not implemented for now.
}

void SequentialPrefetcher::GetPriorFromBucket(IoLogEntry &entry,
                                             PrefetchDecision &decision,
                                             BlobId &cur_id) {
  // Get current blob name
  auto prefetcher = Singleton<Prefetcher>::GetInstance();
  auto hermes = prefetcher->hermes_;
  std::string blob_name = GetBlobNameFromId(&hermes->context_,
                                            &hermes->rpc_,
                                            cur_id);

  // Get prior blob name
  BlobPlacement p;
  p.DecodeBlobName(blob_name);
  if (p.page_ == 0) {
    cur_id.as_int = 0;
    return;
  }
  p.page_ -= 1;
  decision.blob_name_ = p.CreateBlobName();

  // Get the prior blob ID from the bucket
  decision.blob_id_ = GetBlobId(&hermes->context_, &hermes->rpc_,
                                decision.blob_name_, entry.bkt_id_,
                                false);
}

void SequentialPrefetcher::GetPriorFromVbucket(IoLogEntry &entry,
                                              PrefetchDecision &decision,
                                              BlobId &cur_id) {
  // TODO(llogan): Not implemented for now.
}

}  // namespace hermes
