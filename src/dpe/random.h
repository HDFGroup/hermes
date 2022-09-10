//
// Created by lukemartinlogan on 9/9/22.
//

#ifndef HERMES_SRC_DPE_RANDOM_H_
#define HERMES_SRC_DPE_RANDOM_H_

#include "data_placement_engine.h"

namespace hermes {

class Random : public DPE {
 public:
  Status Placement(const std::vector<size_t> &blob_sizes,
                   const std::vector<u64> &node_state,
                   const std::vector<f32> &bandwidths,
                   const std::vector<TargetID> &targets,
                   std::vector<PlacementSchema> &output,
                   const api::Context &ctx);
 private:
  void GetOrderedCapacities(const std::vector<u64> &node_state,
                            const std::vector<TargetID> &targets,
                            std::multimap<u64, TargetID> &ordered_cap);
  Status AddSchema(std::multimap<u64, TargetID> &ordered_cap,
                   size_t blob_size, PlacementSchema &schema);
};

}  // namespace hermes

#endif  // HERMES_SRC_DPE_RANDOM_H_
