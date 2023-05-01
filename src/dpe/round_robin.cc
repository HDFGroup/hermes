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

#include "round_robin.h"
#include <numeric>

namespace hermes {

Status RoundRobin::Placement(const std::vector<size_t> &blob_sizes,
                             std::vector<TargetInfo> &targets,
                             api::Context &ctx,
                             std::vector<PlacementSchema> &output) {
  for (size_t blob_size : blob_sizes) {
    // Initialize blob's size, score, and schema
    size_t rem_blob_size = blob_size;
    output.emplace_back();
    PlacementSchema &blob_schema = output.back();

    // Choose RR target and iterate
    size_t target_id = counter_.fetch_add(1) % targets.size();
    for (size_t i = 0; i < targets.size(); ++i) {
      TargetInfo &target = targets[(target_id + i) % targets.size()];

      // NOTE(llogan): We skip targets that can't fit the ENTIRE blob
      if (target.rem_cap_ < blob_size) {
        continue;
      }

      // Place the blob on this target
      blob_schema.plcmnts_.emplace_back(rem_blob_size,
                                        target.id_);
      target.rem_cap_ -= rem_blob_size;
      rem_blob_size = 0;
      break;
    }

    if (rem_blob_size > 0) {
      return DPE_MIN_IO_TIME_NO_SOLUTION;
    }
  }

  return Status();
}

}  // namespace hermes
