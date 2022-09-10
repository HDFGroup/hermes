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
#include "data_placement_engine.h"

namespace hermes {

using api::Status;

/** Represents the state of a Round-Robin data placement strategy */
class RoundRobin : public DPE {
 private:
  static inline int current_device_index_{}; /**< The current device index */
  static std::vector<DeviceID> devices_; /**< A list of device targets */
  std::mutex device_index_mutex_; /**< Protect index updates */

 public:
  RoundRobin();
  ~RoundRobin();

  Status Placement(const std::vector<size_t> &blob_sizes,
                   const std::vector<u64> &node_state,
                   const std::vector<f32> &bandwidths,
                   const std::vector<TargetID> &targets,
                   std::vector<PlacementSchema> &output,
                   const api::Context &ctx);

  /** Retrieves the number of devices */
  size_t GetNumDevices() const;

  /** Retrieves the current device index */
  int GetCurrentDeviceIndex() const;

  /** Retrieves the device ID at a given index */
  DeviceID GetDeviceByIndex(int i) const;

  /** Re-/Sets the current device index */
  void SetCurrentDeviceIndex(int new_device_index);

  /** Initialize the static variable for devices*/
  static void InitDevices(hermes::Config *config,
                          std::shared_ptr<api::Hermes> result);
  static void InitDevices(int count, int start_val = 0);

 private:
  Status AddSchema(size_t index, std::vector<u64> &node_state,
                   const std::vector<size_t> &blob_sizes,
                   const std::vector<TargetID> &targets,
                   PlacementSchema &output);
};

}  // namespace hermes

#endif  // HERMES_SRC_DPE_ROUND_ROBIN_H_
