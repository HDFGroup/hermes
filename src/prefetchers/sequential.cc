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
  // Increase each unique_bucket access count
  for (auto &entry : log) {
    UniqueBucket id(entry.vbkt_id_, entry.bkt_id_);
    ++state_[id].count_;
  }

  // Append time estimates and read-aheads to the schema
  for (auto &entry : log) {
    UniqueBucket id(entry.vbkt_id_, entry.bkt_id_);
    auto &state = state_[id];
    float cur_runtime = PrefetchStat::DiffTimespec(&entry.timestamp_,
                                                   &state.first_access_);
    float avg_time = cur_runtime / state.count_;

    // Read-ahead the next few blobs
    BlobID cur_id = entry.blob_id_;
    for (int i = 0; i < entry.pctx_.read_ahead_; ++i) {
      PrefetchDecision decision;
      decision.bkt_id_ = entry.bkt_id_;
      if (IsNullVBucketId(entry.vbkt_id_)) {
        GetNextBucket(entry, decision, cur_id);
      } else {
        GetNextVbucket(entry, decision, cur_id);
      }
      if (IsNullBlobId(decision.blob_id_)) {
        break;
      }
      decision.AddStat(avg_time, entry.timestamp_);
      schema.emplace(decision);
      cur_id = decision.blob_id_;
    }
  }
}

void SequentialPrefetcher::GetNextBucket(IoLogEntry &entry,
                                         PrefetchDecision &decision,
                                         BlobID &cur_id) {
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
  std::string next_blob_name = p.CreateBlobName();

  // Get the next blob ID from the bucket
  decision.blob_id_ = GetBlobId(&hermes->context_, &hermes->rpc_,
                             next_blob_name, entry.bkt_id_,
                             false);
}

void SequentialPrefetcher::GetNextVbucket(IoLogEntry &entry,
                                          PrefetchDecision &decision,
                                          BlobID &cur_id) {
  // TODO(llogan): Not implemented for now.
}

}  // namespace hermes