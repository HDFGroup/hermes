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

#ifndef HERMES_SRC_DPE_ROUND_ROBIN_H_
#define HERMES_SRC_DPE_ROUND_ROBIN_H_

#include <map>

#include "dpe.h"

namespace hermes {

/** Represents the state of a Round-Robin data placement strategy */
class RoundRobin : public Dpe {
 private:
  std::atomic<size_t> counter_;

 public:
  RoundRobin() : counter_(0) {}

  Status Placement(const std::vector<size_t> &blob_sizes,
                   std::vector<TargetInfo> &targets,
                   Context &ctx,
                   std::vector<PlacementSchema> &output) {
    for (size_t blob_size : blob_sizes) {
      // Initialize blob's size, score, and schema
      size_t rem_blob_size = blob_size;
      output.emplace_back();
      PlacementSchema &blob_schema = output.back();

      // Choose RR target and iterate
      size_t target_id = counter_.fetch_add(1) % targets.size();
      for (size_t tgt_idx = 0; tgt_idx < targets.size(); ++tgt_idx) {
        TargetInfo &target = targets[(target_id + tgt_idx) % targets.size()];
        size_t rem_cap = target.GetRemCap();

        // NOTE(llogan): We skip targets that can't fit the ENTIRE blob
        if (rem_cap < blob_size) {
          continue;
        }
        if (ctx.blob_score_ == -1) {
          ctx.blob_score_ = target.score_;
        }

        // Place the blob on this target
        blob_schema.plcmnts_.emplace_back(rem_blob_size,
                                          target.id_);
        rem_blob_size = 0;
        break;
      }

      if (rem_blob_size > 0) {
        return DPE_MIN_IO_TIME_NO_SOLUTION;
      }
    }

    return Status();
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_DPE_ROUND_ROBIN_H_
