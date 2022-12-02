//
// Created by lukemartinlogan on 12/2/22.
//

#include <float.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <yaml-cpp/yaml.h>
#include <ostream>
#include <glog/logging.h>
#include "utils.h"
#include "config.h"
#include <iomanip>

#include "config.h"
#include "config_server_default.h"

namespace hermes {

/** log an error message when the number of devices is 0 in \a config */
void ServerConfig::RequireNumDevices() {
  if (num_devices == 0) {
    LOG(FATAL) << "The configuration variable 'num_devices' must be defined "
               << "first" << std::endl;
  }
}

/** log an error message when the number of slabs is 0 in \a config  */
void ServerConfig::RequireNumSlabs() {
  if (num_slabs == 0) {
    LOG(FATAL) << "The configuration variable 'num_slabs' must be defined first"
               << std::endl;
  }
}

/** log an error message when capacities are specified multiple times */
void ServerConfig::RequireCapacitiesUnset(bool &already_specified) {
  if (already_specified) {
    LOG(FATAL) << "Capacities are specified multiple times in the configuration"
               << " file. Only use one of 'capacities_bytes', 'capacities_kb',"
               << "'capacities_mb', or 'capacities_gb'\n";
  } else {
    already_specified = true;
  }
}

/** log an error message when block sizes are specified multiple times */
void ServerConfig::RequireBlockSizesUnset(bool &already_specified) {
  if (already_specified) {
    LOG(FATAL) << "Block sizes are specified multiple times in the "
               << "configuration file. Only use one of 'block_sizes_bytes',"
               << "'block_sizes_kb', 'block_sizes_mb', or 'block_sizes_gb'\n";
  } else {
    already_specified = true;
  }
}

/** parse capacities from configuration file in YAML */
void ServerConfig::ParseCapacities(YAML::Node capacities, int unit_conversion,
                                   bool &already_specified) {
  int i = 0;
  RequireNumDevices(config);
  RequireCapacitiesUnset(already_specified);
  for (auto val_node : capacities) {
    capacities[i++] = val_node.as<size_t>() * unit_conversion;
  }
}

/** parse block sizes from configuration file in YAML */
void ServerConfig::ParseBlockSizes(YAML::Node block_sizes, int unit_conversion,
                                   bool &already_specified) {
  int i = 0;
  RequireNumDevices(config);
  RequireBlockSizesUnset(already_specified);
  for (auto val_node : block_sizes) {
    size_t block_size = val_node.as<size_t>() * unit_conversion;
    if (block_size > INT_MAX) {
      LOG(FATAL) << "Max supported block size is " << INT_MAX << " bytes. "
                 << "Config file requested " << block_size << " bytes\n";
    }
    block_sizes[i++] = block_size;
  }
}

/** parse host names from configuration file in YAML */
void ServerConfig::ParseHostNames(YAML::Node yaml_conf) {
  if (yaml_conf["rpc_server_host_file"]) {
    rpc_server_host_file =
        yaml_conf["rpc_server_host_file"].as<std::string>();
  }
  if (yaml_conf["rpc_server_base_name"]) {
    rpc_server_base_name =
        yaml_conf["rpc_server_base_name"].as<std::string>();
  }
  if (yaml_conf["rpc_server_suffix"]) {
    rpc_server_suffix =
        yaml_conf["rpc_server_suffix"].as<std::string>();
  }
  if (yaml_conf["rpc_host_number_range"]) {
    ParseRangeList(yaml_conf["rpc_host_number_range"], "rpc_host_number_range",
                   host_numbers);
  }

  if (rpc_server_host_file.empty()) {
    host_names.clear();
    if (host_numbers.size() == 0) {
      host_numbers.emplace_back("");
    }
    for (auto &host_number : host_numbers) {
      host_names.emplace_back(rpc_server_base_name +
                                      host_number + rpc_server_suffix);
    }
  }
}

/** check constraints in \a config configuration */
void ServerConfig::CheckConstraints() {
  // rpc_domain must be present if rpc_protocol is "verbs"
  if (rpc_protocol.find("verbs") != std::string::npos &&
      rpc_domain.empty()) {
    PrintExpectedAndFail("a non-empty value for rpc_domain");
  }

  double tolerance = 0.0000001;

  // arena_percentages must add up to 1.0
  /*double arena_percentage_sum = 0;
  for (int i = 0; i < kArenaType_Count; ++i) {
    arena_percentage_sum += arena_percentages[i];
  }
  if (fabs(1.0 - arena_percentage_sum) > tolerance) {
    std::ostringstream msg;
    msg << "the values in arena_percentages to add up to 1.0 but got ";
    msg << arena_percentage_sum << "\n";
    PrintExpectedAndFail(msg.str());
  }*/

  // Each slab's desired_slab_percentages should add up to 1.0
  for (int device = 0; device < num_devices; ++device) {
    double total_slab_percentage = 0;
    for (int slab = 0; slab < num_slabs[device]; ++slab) {
      total_slab_percentage += desired_slab_percentages[device][slab];
    }
    if (fabs(1.0 - total_slab_percentage) > tolerance) {
      std::ostringstream msg;
      msg << "the values in desired_slab_percentages[";
      msg << device;
      msg << "] to add up to 1.0 but got ";
      msg << total_slab_percentage << "\n";
      PrintExpectedAndFail(msg.str());
    }
  }
}

/** parse the YAML node */
void ServerConfig::ParseYAML(YAML::Node &yaml_conf) {
  bool capcities_specified = false, block_sizes_specified = false;
  std::vector<std::string> host_numbers;
  std::vector<std::string> host_basename;
  std::string host_suffix;

  if (yaml_conf["num_devices"]) {
    num_devices = yaml_conf["num_devices"].as<int>();
    num_targets = num_devices;
  }
  if (yaml_conf["num_targets"]) {
    num_targets = yaml_conf["num_targets"].as<int>();
  }

  if (yaml_conf["capacities_bytes"]) {
    ParseCapacities(yaml_conf["capacities_bytes"], 1,
                    capcities_specified);
  }
  if (yaml_conf["capacities_kb"]) {
    ParseCapacities(yaml_conf["capacities_kb"], KILOBYTES(1),
                    capcities_specified);
  }
  if (yaml_conf["capacities_mb"]) {
    ParseCapacities(yaml_conf["capacities_mb"], MEGABYTES(1),
                    capcities_specified);
  }
  if (yaml_conf["capacities_gb"]) {
    ParseCapacities(yaml_conf["capacities_gb"], GIGABYTES(1),
                    capcities_specified);
  }

  if (yaml_conf["block_sizes_bytes"]) {
    ParseBlockSizes(yaml_conf["block_sizes_bytes"], 1,
                    block_sizes_specified);
  }
  if (yaml_conf["block_sizes_kb"]) {
    ParseBlockSizes(yaml_conf["block_sizes_kb"], KILOBYTES(1),
                    block_sizes_specified);
  }
  if (yaml_conf["block_sizes_mb"]) {
    ParseBlockSizes(yaml_conf["block_sizes_mb"], MEGABYTES(1),
                    block_sizes_specified);
  }
  if (yaml_conf["block_sizes_gb"]) {
    ParseBlockSizes(yaml_conf["block_sizes_gb"], GIGABYTES(1),
                    block_sizes_specified);
  }

  if (yaml_conf["num_slabs"]) {
    RequireNumDevices();
    ParseArray<int>(yaml_conf["num_slabs"], "num_slabs", num_slabs,
                    num_devices);
  }
  if (yaml_conf["slab_unit_sizes"]) {
    RequireNumDevices();
    RequireNumSlabs();
    ParseMatrix<int>(yaml_conf["slab_unit_sizes"], "slab_unit_sizes",
                     reinterpret_cast<int *>(slab_unit_sizes),
                     kMaxDevices, kMaxBufferPoolSlabs, num_slabs);
  }
  if (yaml_conf["desired_slab_percentages"]) {
    RequireNumDevices();
    RequireNumSlabs();
    ParseMatrix<f32>(yaml_conf["desired_slab_percentages"],
                     "desired_slab_percentages",
                     reinterpret_cast<f32 *>(desired_slab_percentages),
                     kMaxDevices, kMaxBufferPoolSlabs, num_slabs);
  }
  if (yaml_conf["bandwidth_mbps"]) {
    RequireNumDevices();
    ParseArray<f32>(yaml_conf["bandwidth_mbps"], "bandwidth_mbps",
                    bandwidths, num_devices);
  }
  if (yaml_conf["latencies"]) {
    RequireNumDevices();
    ParseArray<f32>(yaml_conf["latencies"], "latencies", latencies,
                    num_devices);
  }
  /*if (yaml_conf["buffer_pool_arena_percentage"]) {
    arena_percentages[hermes::kArenaType_BufferPool] =
        yaml_conf["buffer_pool_arena_percentage"].as<f32>();
  }
  if (yaml_conf["metadata_arena_percentage"]) {
    arena_percentages[hermes::kArenaType_MetaData] =
        yaml_conf["metadata_arena_percentage"].as<f32>();
  }
  if (yaml_conf["transient_arena_percentage"]) {
    arena_percentages[hermes::kArenaType_Transient] =
        yaml_conf["transient_arena_percentage"].as<f32>();
  }*/
  if (yaml_conf["mount_points"]) {
    RequireNumDevices();
    ParseArray<std::string>(yaml_conf["mount_points"], "mount_points",
                            mount_points, num_devices);
  }
  if (yaml_conf["swap_mount"]) {
    swap_mount = yaml_conf["swap_mount"].as<std::string>();
  }
  if (yaml_conf["num_buffer_organizer_retries"]) {
    num_buffer_organizer_retries =
        yaml_conf["num_buffer_organizer_retries"].as<int>();
  }
  if (yaml_conf["max_buckets_per_node"]) {
    max_buckets_per_node = yaml_conf["max_buckets_per_node"].as<int>();
  }
  if (yaml_conf["max_vbuckets_per_node"]) {
    max_vbuckets_per_node =
        yaml_conf["max_vbuckets_per_node"].as<int>();
  }
  if (yaml_conf["system_view_state_update_interval_ms"]) {
    system_view_state_update_interval_ms =
        yaml_conf["system_view_state_update_interval_ms"].as<int>();
  }
  if (yaml_conf["buffer_pool_shmem_name"]) {
    std::string name = yaml_conf["buffer_pool_shmem_name"].as<std::string>();
    std::snprintf(buffer_pool_shmem_name, kMaxBufferPoolShmemNameLength,
                  "%s", name.c_str());
  }
  if (yaml_conf["rpc_protocol"]) {
    rpc_protocol = yaml_conf["rpc_protocol"].as<std::string>();
  }
  if (yaml_conf["rpc_domain"]) {
    rpc_domain = yaml_conf["rpc_domain"].as<std::string>();
  }
  if (yaml_conf["rpc_port"]) {
    rpc_port = yaml_conf["rpc_port"].as<int>();
  }
  if (yaml_conf["buffer_organizer_port"]) {
    buffer_organizer_port =
        yaml_conf["buffer_organizer_port"].as<int>();
  }
  if (yaml_conf["rpc_num_threads"]) {
    rpc_num_threads = yaml_conf["rpc_num_threads"].as<int>();
  }
  if (yaml_conf["default_placement_policy"]) {
    std::string policy =
        yaml_conf["default_placement_policy"].as<std::string>();

    if (policy == "MinimizeIoTime") {
      default_placement_policy = api::PlacementPolicy::kMinimizeIoTime;
    } else if (policy == "Random") {
      default_placement_policy = api::PlacementPolicy::kRandom;
    } else if (policy == "RoundRobin") {
      default_placement_policy = api::PlacementPolicy::kRoundRobin;
    } else {
      LOG(FATAL) << "Unknown default_placement_policy: '" << policy << "'"
                 << std::endl;
    }
  }
  if (yaml_conf["is_shared_device"]) {
    RequireNumDevices();
    ParseArray<int>(yaml_conf["is_shared_device"], "is_shared_device",
                    is_shared_device, num_devices);
  }
  if (yaml_conf["buffer_organizer_num_threads"]) {
    bo_num_threads =
        yaml_conf["buffer_organizer_num_threads"].as<int>();
  }
  if (yaml_conf["default_rr_split"]) {
    default_rr_split = yaml_conf["default_rr_split"].as<int>();
  }
  if (yaml_conf["bo_num_threads"]) {
    bo_num_threads = yaml_conf["bo_num_threads"].as<int>();
  }
  if (yaml_conf["bo_capacity_thresholds"]) {
    RequireNumDevices();
    f32 thresholds[kMaxDevices][2] = {0};
    ParseMatrix<f32>(yaml_conf["bo_capacity_thresholds"],
                     "bo_capacity_thresholds",
                     reinterpret_cast<f32 *>(thresholds), kMaxDevices, 2);
    for (int i = 0; i < num_devices; ++i) {
      bo_capacity_thresholds[i].min = thresholds[i][0];
      bo_capacity_thresholds[i].max = thresholds[i][1];
    }
  }
  if (yaml_conf["path_exclusions"]) {
    ParseVector<std::string>(yaml_conf["path_exclusions"],
                             path_exclusions);
  }
  if (yaml_conf["path_inclusions"]) {
    ParseVector<std::string>(yaml_conf["path_inclusions"],
                             path_inclusions);
  }
  ParseHostNames(yaml_conf);
  CheckConstraints();
}

/** load configuration from a string */
void ServerConfig::LoadText(const std::string &config_string) {
  YAML::Node yaml_conf = YAML::Load(config_string);
  ParseYAML(yaml_conf);
}

/** load configuration from file */
void ServerConfig::LoadFromFile(const char *path) {
  if (path == nullptr) {
    return;
  }
  LOG(INFO) << "ParseConfig-LoadFile" << std::endl;
  YAML::Node yaml_conf = YAML::LoadFile(path);
  LOG(INFO) << "ParseConfig-LoadComplete" << std::endl;
  ParseYAML(yaml_conf);
}

/** The default server config */
ServerConfig::ServerConfig() {

}

}  // namespace hermes
