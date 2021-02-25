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

#ifndef HERMES_DATA_PLACEMENT_ENGINE_H_
#define HERMES_DATA_PLACEMENT_ENGINE_H_

#include <map>

#include "hermes_types.h"
#include "hermes_status.h"
#include "hermes.h"

namespace hermes {

using api::Status;

class DataPlacementEngine {
  static inline size_t count_device_ {};

 public:
  size_t getCountDevice() const {return count_device_;}
  void setCountDevice(size_t new_count_device) {
    count_device_ = new_count_device;
  }
};

Status RoundRobinPlacement(std::vector<size_t> &blob_sizes,
                        std::vector<u64> &node_state,
                           std::vector<PlacementSchema> &output,
                           const std::vector<TargetID> &targets);

Status RandomPlacement(std::vector<size_t> &blob_sizes,
                       std::multimap<u64, TargetID> &ordered_cap,
                       std::vector<PlacementSchema> &output);

Status MinimizeIoTimePlacement(const std::vector<size_t> &blob_sizes,
                               const std::vector<u64> &node_state,
                               const std::vector<f32> &bandwidths,
                               const std::vector<TargetID> &targets,
                               std::vector<PlacementSchema> &output);

Status CalculatePlacement(SharedMemoryContext *context, RpcContext *rpc,
                          std::vector<size_t> &blob_size,
                          std::vector<PlacementSchema> &output,
                          const api::Context &api_context);

PlacementSchema AggregateBlobSchema(PlacementSchema &schema);

// internal
std::vector<int> GetValidSplitChoices(size_t blob_size);
Status AddRandomSchema(std::multimap<u64, size_t> &ordered_cap,
                       size_t blob_size, std::vector<PlacementSchema> &output,
                       std::vector<u64> &node_state);

}  // namespace hermes
#endif  // HERMES_DATA_PLACEMENT_ENGINE_H_
