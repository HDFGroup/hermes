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

#ifndef HERMES_SRC_DPE_MINIMIZE_IO_TIME_H_
#define HERMES_SRC_DPE_MINIMIZE_IO_TIME_H_

#include "dpe.h"

namespace hermes {
/**
 A class to represent data placement engine that minimizes I/O time.
*/
class MinimizeIoTime : public Dpe {
 public:
  MinimizeIoTime() = default;
  ~MinimizeIoTime() = default;
  Status Placement(const std::vector<size_t> &blob_sizes,
                   std::vector<TargetInfo> &targets,
                   Context &ctx,
                   std::vector<PlacementSchema> &output) {
    for (size_t blob_size : blob_sizes) {
      // Initialize blob's size, score, and schema
      size_t rem_blob_size = blob_size;
      float score = ctx.blob_score_;
      if (ctx.blob_score_ == -1) {
        score = 1;
      }
      output.emplace_back();
      PlacementSchema &blob_schema = output.back();

      for (u32 tgt_idx = 0; tgt_idx < targets.size(); ++tgt_idx) {
        TargetInfo &target = targets[tgt_idx];
        // NOTE(llogan): We skip targets that are too high of priority or
        // targets that can't fit the ENTIRE blob
        size_t rem_cap = target.GetRemCap();
        // TODO(llogan): add back
        // if (target.score_ > score || rem_cap < blob_size) {
        if (rem_cap < blob_size) {
          // TODO(llogan): add other considerations of this Dpe
          continue;
        }
        if (ctx.blob_score_ == -1) {
          ctx.blob_score_ = target.score_;
        }

        // NOTE(llogan): we assume the TargetInfo list is sorted
        if (rem_cap >= rem_blob_size) {
          blob_schema.plcmnts_.emplace_back(rem_blob_size,
                                            target.id_);
          rem_cap -= rem_blob_size;
          rem_blob_size = 0;
        } else {
          // NOTE(llogan): this code technically never gets called,
          // but it might in the future
          blob_schema.plcmnts_.emplace_back(rem_cap,
                                            target.id_);
          rem_blob_size -= rem_cap;
          rem_cap = 0;
        }

        if (rem_blob_size == 0) {
          break;
        }
      }

      if (rem_blob_size > 0) {
        return DPE_MIN_IO_TIME_NO_SOLUTION;
      }
    }

    return Status();
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_DPE_MINIMIZE_IO_TIME_H_
