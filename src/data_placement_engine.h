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

/** Represents the state of a Round-Robin data placement strategy */
class RoundRobinState {
  static inline int current_device_index_ {};  /**< The current device index */

  std::mutex device_index_mutex_;              /**< Protect index updates */

 public:
  static std::vector<DeviceID> devices_;       /**< A list of device targets */

  RoundRobinState();
  ~RoundRobinState();

  /** Retrieves the number of devices */
  size_t GetNumDevices() const;

  /** Retrieves the current device index */
  int GetCurrentDeviceIndex() const;

  /** Retrieves the device ID at a given index */
  DeviceID GetDeviceByIndex(int i) const;

  /** Re-/Sets the current device index */
  void SetCurrentDeviceIndex(int new_device_index);
};

Status RoundRobinPlacement(const std::vector<size_t> &blob_sizes,
                           std::vector<u64> &node_state,
                           std::vector<PlacementSchema> &output,
                           const std::vector<TargetID> &targets,
                           bool split);

Status RandomPlacement(const std::vector<size_t> &blob_sizes,
                       std::multimap<u64, TargetID> &ordered_cap,
                       std::vector<PlacementSchema> &output);

Status MinimizeIoTimePlacement(const std::vector<size_t> &blob_sizes,
                               const std::vector<u64> &node_state,
                               const std::vector<f32> &bandwidths,
                               const std::vector<TargetID> &targets,
                               std::vector<PlacementSchema> &output,
                               const api::Context &ctx = api::Context());

Status CalculatePlacement(SharedMemoryContext *context, RpcContext *rpc,
                          const std::vector<size_t> &blob_size,
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
