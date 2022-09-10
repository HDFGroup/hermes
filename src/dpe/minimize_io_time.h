//
// Created by lukemartinlogan on 9/9/22.
//

#ifndef HERMES_SRC_DPE_MINIMIZE_IO_TIME_H_
#define HERMES_SRC_DPE_MINIMIZE_IO_TIME_H_

#include "data_placement_engine.h"

namespace hermes {

class MinimizeIoTime : public DPE {
 public:
  Status Placement(const std::vector<size_t> &blob_sizes,
                   const std::vector<u64> &node_state,
                   const std::vector<f32> &bandwidths,
                   const std::vector<TargetID> &targets,
                   std::vector<PlacementSchema> &output,
                   const api::Context &ctx);
};

}

#endif  // HERMES_SRC_DPE_MINIMIZE_IO_TIME_H_
