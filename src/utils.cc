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

#include "utils.h"

#include <iostream>
#include <vector>
#include <random>
#include <utility>

namespace hermes {

size_t RoundUpToMultiple(size_t val, size_t multiple) {
  if (multiple == 0) {
    return val;
  }

  size_t result = val;
  size_t remainder = val % multiple;

  if (remainder != 0) {
    result += multiple - remainder;
  }

  return result;
}

size_t RoundDownToMultiple(size_t val, size_t multiple) {
  if (multiple == 0) {
    return val;
  }

  size_t result = val;
  size_t remainder = val % multiple;
  result -= remainder;

  return result;
}

void InitDefaultConfig(Config *config) {
  config->num_devices = 4;
  config->num_targets = 4;
  assert(config->num_devices < kMaxDevices);

  for (int dev = 0; dev < config->num_devices; ++dev) {
    config->capacities[dev] = MEGABYTES(50);
    config->block_sizes[dev] = KILOBYTES(4);
    config->num_slabs[dev] = 4;

    config->slab_unit_sizes[dev][0] = 1;
    config->slab_unit_sizes[dev][1] = 4;
    config->slab_unit_sizes[dev][2] = 16;
    config->slab_unit_sizes[dev][3] = 32;

    config->desired_slab_percentages[dev][0] = 0.25f;
    config->desired_slab_percentages[dev][1] = 0.25f;
    config->desired_slab_percentages[dev][2] = 0.25f;
    config->desired_slab_percentages[dev][3] = 0.25f;
  }

  config->bandwidths[0] = 6000.0f;
  config->bandwidths[1] = 300.f;
  config->bandwidths[2] = 150.0f;
  config->bandwidths[3] = 70.0f;

  config->latencies[0] = 15.0f;
  config->latencies[1] = 250000;
  config->latencies[2] = 500000.0f;
  config->latencies[3] = 1000000.0f;

  // TODO(chogan): Express these in bytes instead of percentages to avoid
  // rounding errors?
  config->arena_percentages[kArenaType_BufferPool] = 0.85f;
  config->arena_percentages[kArenaType_MetaData] = 0.04f;
  config->arena_percentages[kArenaType_TransferWindow] = 0.08f;
  config->arena_percentages[kArenaType_Transient] = 0.03f;

  config->mount_points[0] = "";
  config->mount_points[1] = "./";
  config->mount_points[2] = "./";
  config->mount_points[3] = "./";
  config->swap_mount = "./";

  config->num_buffer_organizer_retries = 3;

  config->rpc_server_base_name = "localhost";
  config->rpc_server_suffix = "";
  config->rpc_protocol = "ofi+sockets";
  config->rpc_domain = "";
  config->rpc_port = 8080;
  config->buffer_organizer_port = 8081;
  config->rpc_num_threads = 1;

  config->max_buckets_per_node = 16;
  config->max_vbuckets_per_node = 8;
  config->system_view_state_update_interval_ms = 100;

  const char buffer_pool_shmem_name[] = "/hermes_buffer_pool_";
  size_t shmem_name_size = strlen(buffer_pool_shmem_name);
  for (size_t i = 0; i < shmem_name_size; ++i) {
    config->buffer_pool_shmem_name[i] = buffer_pool_shmem_name[i];
  }
  config->buffer_pool_shmem_name[shmem_name_size] = '\0';
}

namespace testing {

/** Use Megabytes */
std::vector<i64> device_size {128, 1024, 4096, 16384};

/** Use Megabytes/Sec */
std::vector<double> device_bandwidth {8192, 3072, 550, 120};

std::vector<double> tgt_homo_dist {0.25, 0.25, 0.25, 0.25};
std::vector<double> tgt_heto_dist {0.1, 0.2, 0.3, 0.4};

TargetViewState InitDeviceState(u64 total_target, bool homo_dist) {
  TargetViewState result = {};
  result.num_devices = total_target;
  std::vector<double> tgt_fraction;

  if (homo_dist)
    tgt_fraction = tgt_homo_dist;
  else
    tgt_fraction = tgt_heto_dist;

  std::vector<u64> tgt_num_per_type;
  u64 used_tgt {0};
  for (size_t i {0}; i < tgt_fraction.size()-1; ++i) {
    u64 used_tgt_tmp {static_cast<u64>(tgt_fraction[i] * total_target)};
    tgt_num_per_type.push_back(used_tgt_tmp);
    used_tgt += used_tgt_tmp;
  }
  tgt_num_per_type.push_back(total_target - used_tgt);

  using hermes::TargetID;
  std::vector<TargetID> targets = GetDefaultTargets(total_target);

  u64 target_position {0};
  for (size_t i {0}; i < tgt_num_per_type.size(); ++i) {
    for (size_t j {0}; j < tgt_num_per_type[i]; ++j) {
      result.bandwidth.push_back(device_bandwidth[i]);

      result.bytes_available.push_back(MEGABYTES(device_size[i]));
      result.bytes_capacity.push_back(MEGABYTES(device_size[i]));
      result.ordered_cap.insert(std::pair<hermes::u64, TargetID>(
                                MEGABYTES(device_size[i]),
                                targets[target_position]));
    }
  }

  return result;
}

u64 UpdateDeviceState(PlacementSchema &schema,
                      TargetViewState &node_state) {
  u64 result {0};
  node_state.ordered_cap.clear();

  for (auto [size, target] : schema) {
    result += size;
    node_state.bytes_available[target.bits.device_id] -= size;
    node_state.ordered_cap.insert(
      std::pair<u64, TargetID>(
        node_state.bytes_available[target.bits.device_id], target));
  }

  return result;
}

void PrintNodeState(TargetViewState &node_state) {
  for (int i {0}; i < node_state.num_devices; ++i) {
    std::cout << "  capacity of device[" << i << "]: "
              << node_state.bytes_available[i]
              << '\n' << std::flush;
    std::cout << "  available ratio of device["<< i << "]: "
              << static_cast<double>(node_state.bytes_available[i])/
                 node_state.bytes_capacity[i]
              << "\n\n" << std::flush;
  }
}

std::vector<TargetID> GetDefaultTargets(size_t n) {
  std::vector<TargetID> result(n);
  for (size_t i = 0; i < n; ++i) {
    TargetID id = {};
    id.bits.node_id = 1;
    id.bits.device_id = (DeviceID)i;
    id.bits.index = i;
    result[i] = id;
  }

  return result;
}

std::pair<size_t, size_t> GetBlobBound(BlobSizeRange blob_size_range) {
  if (blob_size_range == BlobSizeRange::kSmall)
    return {0, KILOBYTES(64)};
  else if (blob_size_range == BlobSizeRange::kMedium)
    return {KILOBYTES(64), MEGABYTES(1)};
  else if (blob_size_range == BlobSizeRange::kLarge)
    return {MEGABYTES(1), MEGABYTES(4)};
  else if (blob_size_range == BlobSizeRange::kXLarge)
    return {MEGABYTES(4), MEGABYTES(64)};
  else if (blob_size_range == BlobSizeRange::kHuge)
    return {GIGABYTES(1), GIGABYTES(1)};

  std::cout << "No specified blob range is found.\n"
            << "Use small blob range (0, 64KB].";
  return {0, KILOBYTES(64)};
}

std::vector<size_t> GenFixedTotalBlobSize(size_t total_size,
                                          BlobSizeRange range) {
  std::vector<size_t> result;
  size_t used_size {};
  size_t size {};
  std::random_device dev;
  std::mt19937 rng(dev());

  std::pair bound = GetBlobBound(range);
  size_t lo_bound  = bound.first;
  size_t hi_bound = bound.second;

  while (used_size < total_size) {
    std::vector<hermes::api::Blob> input_blobs;
    if (total_size - used_size > hi_bound) {
      std::uniform_int_distribution<std::mt19937::result_type>
        distribution(lo_bound, hi_bound);
      size = distribution(rng);
      used_size += size;
    } else {
      size = total_size - used_size;
      used_size = total_size;
    }
    result.push_back(size);
  }
  return result;
}

std::vector<size_t> GenFixedNumberOfBlobs(int num,
                                          size_t each_blob_size) {
  std::vector<size_t> result;

  for (auto i {0}; i < num; ++i) {
    result.push_back(each_blob_size);
  }
  return result;
}

}  // namespace testing
}  // namespace hermes
