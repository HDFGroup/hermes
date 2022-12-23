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

// Initialize RoundRobin devices
std::vector<DeviceID> RoundRobin::devices_;

Status RoundRobin::Placement(const std::vector<size_t> &blob_sizes,
                             const std::vector<u64> &node_state,
                             const std::vector<TargetId> &targets,
                             const api::Context &ctx,
                             std::vector<PlacementSchema> &output) {
  Status result;
  std::vector<u64> ns_local(node_state.begin(), node_state.end());
  bool split = ctx.rr_split;

  for (auto &blob_size : blob_sizes) {
    for (auto &target : targets) {

    }
  }

  return result;
}


}  // namespace hermes
