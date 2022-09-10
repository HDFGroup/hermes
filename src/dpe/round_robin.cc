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

#include "metadata_management.h"
#include "metadata_management_internal.h"
#include "metadata_storage.h"

namespace hermes {

using hermes::api::Status;

// Initialize RoundRobin devices
std::vector<DeviceID> RoundRobin::devices_;

RoundRobin::RoundRobin() : device_index_mutex_() {
  device_index_mutex_.lock();
}

RoundRobin::~RoundRobin() {
  device_index_mutex_.unlock();
}

void RoundRobin::InitDevices(hermes::Config *config,
                        std::shared_ptr<api::Hermes> result) {
  devices_.reserve(config->num_devices);
  for (DeviceID id = 0; id < config->num_devices; ++id) {
    if (GetNumBuffersAvailable(&result->context_, id)) {
      devices_.push_back(id);
    }
  }
}

void RoundRobin::InitDevices(int count, int start_val) {
  devices_ = std::vector<DeviceID>(count);
  std::iota(devices_.begin(),
            devices_.end(), start_val);
}

size_t RoundRobin::GetNumDevices() const {
  return devices_.size();
}

DeviceID RoundRobin::GetDeviceByIndex(int i) const {
  return devices_[i];
}

int RoundRobin::GetCurrentDeviceIndex() const {
  return current_device_index_;
}

void RoundRobin::SetCurrentDeviceIndex(int new_device_index) {
  current_device_index_ = new_device_index;
}

Status RoundRobin::AddSchema(size_t index, std::vector<u64> &node_state,
                           const std::vector<size_t> &blob_sizes,
                           const std::vector<TargetID> &targets,
                           PlacementSchema &output) {
  Status result;
  TargetID dst = {};
  size_t num_targets = targets.size();
  int current_device_index {GetCurrentDeviceIndex()};
  size_t num_devices = GetNumDevices();

  // NOTE(chogan): Starting with current_device, loop through all devices until
  // we either 1) find a matching Target or 2) end up back at the starting
  // device.
  for (size_t i = 0; i < num_devices; ++i) {
    int next_index = (current_device_index + i) % num_devices;
    DeviceID dev_id = GetDeviceByIndex(next_index);

    for (size_t j = 0; j < num_targets; ++j) {
      if (node_state[j] >= blob_sizes[index]) {
        if (targets[j].bits.device_id == dev_id) {
          dst = targets[j];
          output.push_back(std::make_pair(blob_sizes[index], dst));
          node_state[j] -= blob_sizes[index];
          SetCurrentDeviceIndex((next_index + 1) % num_devices);
          break;
        }
      }
    }

    if (!IsNullTargetId(dst)) {
      break;
    }
  }

  if (IsNullTargetId(dst)) {
    result = DPE_RR_FIND_TGT_FAILED;
    LOG(ERROR) << result.Msg();
  }

  return result;
}

Status RoundRobin::Placement(const std::vector<size_t> &blob_sizes,
                             const std::vector<u64> &node_state,
                             const std::vector<f32> &bandwidths,
                             const std::vector<TargetID> &targets,
                             std::vector<PlacementSchema> &output,
                             const api::Context &ctx) {
  Status result;
  std::vector<u64> ns_local(node_state.begin(), node_state.end());
  bool split = ctx.rr_split;

  for (size_t i {0}; i < blob_sizes.size(); ++i) {
    PlacementSchema schema;

    if (split) {
      // Construct the vector for the splitted blob
      std::vector<size_t> new_blob_size;
      GetSplitSizes(blob_sizes[i], new_blob_size);

      for (size_t k {0}; k < new_blob_size.size(); ++k) {
        result = AddSchema(k, ns_local, new_blob_size, targets,
                                     schema);
        if (!result.Succeeded()) {
          break;
        }
      }
    } else {
      result = AddSchema(i, ns_local, blob_sizes, targets, schema);
      if (!result.Succeeded()) {
        return result;
      }
    }
    output.push_back(schema);
  }

  return result;
}



}