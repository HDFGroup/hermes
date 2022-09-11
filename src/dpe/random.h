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

#ifndef HERMES_SRC_DPE_RANDOM_H_
#define HERMES_SRC_DPE_RANDOM_H_

#include "data_placement_engine.h"

namespace hermes {

class Random : public DPE {
 public:
  Random() : DPE(PlacementPolicy::kRandom) {}
  Status Placement(const std::vector<size_t> &blob_sizes,
                   const std::vector<u64> &node_state,
                   const std::vector<TargetID> &targets,
                   const api::Context &ctx,
                   std::vector<PlacementSchema> &output);
 private:
  void GetOrderedCapacities(const std::vector<u64> &node_state,
                            const std::vector<TargetID> &targets,
                            std::multimap<u64, TargetID> &ordered_cap);
  Status AddSchema(std::multimap<u64, TargetID> &ordered_cap,
                   size_t blob_size, PlacementSchema &schema);
};

}  // namespace hermes

#endif  // HERMES_SRC_DPE_RANDOM_H_
