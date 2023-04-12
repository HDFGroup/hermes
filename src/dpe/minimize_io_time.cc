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

#include "minimize_io_time.h"
#include <assert.h>
#include <math.h>
#include <numeric>

namespace hermes {

Status MinimizeIoTime::Placement(const std::vector<size_t> &blob_sizes,
                                 std::vector<TargetInfo> &targets,
                                 api::Context &ctx,
                                 std::vector<PlacementSchema> &output) {
  for (size_t blob_size : blob_sizes) {
    // Initialize blob's size, score, and schema
    size_t rem_blob_size = blob_size;
    float score = ctx.blob_score_;
    if (score == -1) {
      score = 1;
      ctx.blob_score_ = 1;
    }
    output.emplace_back();
    PlacementSchema &blob_schema = output.back();

    for (TargetInfo &target : targets) {
      // NOTE(llogan): We skip targets that are too high of priority or
      // targets that can't fit the ENTIRE blob
      if (target.score_ > score ||
          target.rem_cap_ < blob_size) {
        // TODO(llogan): add other considerations of this DPE
        continue;
      }

      // NOTE(llogan): we assume the TargetInfo list is sorted
      if (target.rem_cap_ >= rem_blob_size) {
        blob_schema.plcmnts_.emplace_back(rem_blob_size,
                                          target.id_);
        target.rem_cap_ -= rem_blob_size;
        rem_blob_size = 0;
      } else {
        blob_schema.plcmnts_.emplace_back(target.rem_cap_,
                                          target.id_);
        rem_blob_size -= target.rem_cap_;
        target.rem_cap_ = 0;
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

}  // namespace hermes
