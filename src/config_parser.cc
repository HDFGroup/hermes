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

#include <float.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <yaml-cpp/yaml.h>
#include <ostream>
#include <glog/logging.h>
#include "utils.h"
#include "config_parser.h"
#include <iomanip>

// Steps to add a new configuration variable:
// 1. Add an "if" statement similar to ParseConfigYAML function for the variable
// 2. Add an Assert to config_parser_test.cc to test the functionality.
// 3. Set a default value in InitDefaultConfig
// 4. Add the variable with documentation to test/data/hermes.yaml

namespace hermes {

// TODO(llogan): Use to check for invalid params
/*
static const char *kConfigVariableStrings[] = {
  "unknown",
  "num_devices",
  "num_targets",
  "capacities_bytes",
  "capacities_kb",
  "capacities_mb",
  "capacities_gb",
  "block_sizes_bytes",
  "block_sizes_kb",
  "block_sizes_mb",
  "block_sizes_gb",
  "num_slabs",
  "slab_unit_sizes",
  "desired_slab_percentages",
  "bandwidths_mbps",
  "latencies_us",
  "buffer_pool_arena_percentage",
  "metadata_arena_percentage",
  "transient_arena_percentage",
  "mount_points",
  "swap_mount",
  "num_buffer_organizer_retries",
  "max_buckets_per_node",
  "max_vbuckets_per_node",
  "system_view_state_update_interval_ms",
  "rpc_server_host_file",
  "rpc_server_base_name",
  "rpc_server_suffix",
  "buffer_pool_shmem_name",
  "rpc_protocol",
  "rpc_domain",
  "rpc_port",
  "buffer_organizer_port",
  "rpc_host_number_range",
  "rpc_num_threads",
  "default_placement_policy",
  "is_shared_device",
  "buffer_organizer_num_threads",
  "default_rr_split",
  "bo_capacity_thresholds",
};*/

void PrintExpectedAndFail(const std::string &expected, u32 line_number = 0) {
  std::ostringstream msg;
  msg << "Configuration parser expected '" << expected << "'";
  if (line_number > 0) {
    msg << " on line " << line_number;
  }
  msg << "\n";

  LOG(FATAL) << msg.str();
}

void RequireNumDevices(Config *config) {
  if (config->num_devices == 0) {
    LOG(FATAL) << "The configuration variable 'num_devices' must be defined "
               << "first" << std::endl;
  }
}

void RequireNumSlabs(Config *config) {
  if (config->num_slabs == 0) {
    LOG(FATAL) << "The configuration variable 'num_slabs' must be defined first"
               << std::endl;
  }
}

void RequireCapacitiesUnset(bool &already_specified) {
  if (already_specified) {
    LOG(FATAL) << "Capacities are specified multiple times in the configuration"
               << " file. Only use one of 'capacities_bytes', 'capacities_kb',"
               << "'capacities_mb', or 'capacities_gb'\n";
  } else {
    already_specified = true;
  }
}

void RequireBlockSizesUnset(bool &already_specified) {
  if (already_specified) {
    LOG(FATAL) << "Block sizes are specified multiple times in the "
               << "configuration file. Only use one of 'block_sizes_bytes',"
               << "'block_sizes_kb', 'block_sizes_mb', or 'block_sizes_gb'\n";
  } else {
    already_specified = true;
  }
}

void ParseCapacities(Config *config, YAML::Node capacities,
                     int unit_conversion, bool &already_specified) {
  int i = 0;
  RequireNumDevices(config);
  RequireCapacitiesUnset(already_specified);
  for (auto val_node : capacities) {
    config->capacities[i++] = val_node.as<size_t>() * unit_conversion;
  }
}

void ParseBlockSizes(Config *config, YAML::Node block_sizes,
                     int unit_conversion, bool &already_specified) {
  int i = 0;
  RequireNumDevices(config);
  RequireBlockSizesUnset(already_specified);
  for (auto val_node : block_sizes) {
    size_t block_size = val_node.as<size_t>() * unit_conversion;
    if (block_size > INT_MAX) {
      LOG(FATAL) << "Max supported block size is " << INT_MAX << " bytes. "
                 << "Config file requested " << block_size << " bytes\n";
    }
    config->block_sizes[i++] = block_size;
  }
}

template<typename T>
void ParseArray(YAML::Node list_node, const std::string var,
                T *list, int max_list_len) {
  int i = 0;
  if (max_list_len < (int)list_node.size()) {
    LOG(FATAL) << var << " (array) had "
               << list_node.size() << " arguments "
               << "but up to " << max_list_len << " expected\n";
  }
  for (auto val_node : list_node) {
    list[i++] = val_node.as<T>();
  }
}

template<typename T>
void ParseVector(YAML::Node list_node, std::vector<T> &list) {
  for (auto val_node : list_node) {
    list.emplace_back(val_node.as<T>());
  }
}

template<typename T>
void ParseMatrix(YAML::Node matrix_node, const std::string var, T *matrix,
                 int max_row_len, int max_col_len, int *col_len) {
  int i = 0;
  if (max_row_len < (int)matrix_node.size()) {
    LOG(FATAL) << var << " (matrix) had "
               << matrix_node.size() << " arguments "
               << "but up to " << max_row_len << " expected\n";
  }
  for (auto row : matrix_node) {
    ParseArray<T>(row, var, &matrix[i*max_col_len], col_len[i]);
    ++i;
  }
}

template<typename T>
void ParseMatrix(YAML::Node matrix_node, std::string var, T *matrix,
                 int max_row_len, int max_col_len) {
  int i = 0;
  if (max_row_len < (int)matrix_node.size()) {
    LOG(FATAL) << var << " (matrix) had "
               << matrix_node.size() << " arguments "
               << "but up to " << max_row_len << " expected\n";
  }
  for (auto row : matrix_node) {
    ParseArray<T>(row, var, &matrix[i*max_col_len], max_col_len);
    ++i;
  }
}

void ParseRangeList(YAML::Node list_node, std::string var,
                    std::vector<std::string> &list) {
  int min, max, width = 0;
  for (auto val_node : list_node) {
    std::string val = val_node.as<std::string>();
    if (val.find('-') == std::string::npos) {
      min = val_node.as<int>();
      max = min;
    } else {
      std::stringstream ss(val);
      std::string word;
      std::vector<std::string> words;
      while (std::getline(ss, word, '-')) {
        words.push_back(word);
      }
      if (words.size() != 2) {
        LOG(FATAL) << var <<
            " has invalid range definition " << val << std::endl;
        return;
      }
      min = std::stoi(words[0]);
      max = std::stoi(words[1]);
      width = words[0].size();
    }

    if (width > 0) {
      for (int i = min; i <= max; ++i) {
        std::stringstream ss;
        ss << std::setw(width) << std::setfill('0') << i;
        list.emplace_back(ss.str());
      }
    } else {
      for (int i = min; i <= max; ++i) {
        list.emplace_back(std::to_string(i));
      }
    }
  }
}

void ParseHostNames(YAML::Node yaml_conf, hermes::Config *config) {
  if (yaml_conf["rpc_server_host_file"]) {
    config->rpc_server_host_file =
        yaml_conf["rpc_server_host_file"].as<std::string>();
  }
  if (yaml_conf["rpc_server_base_name"]) {
    config->rpc_server_base_name =
        yaml_conf["rpc_server_base_name"].as<std::string>();
  }
  if (yaml_conf["rpc_server_suffix"]) {
    config->rpc_server_suffix =
        yaml_conf["rpc_server_suffix"].as<std::string>();
  }
  if (yaml_conf["rpc_host_number_range"]) {
    ParseRangeList(yaml_conf["rpc_host_number_range"],
                   "rpc_host_number_range",
                   config->host_numbers);
  }

  if (config->rpc_server_host_file.empty()) {
    config->host_names.clear();
    if (config->host_numbers.size() == 0) {
      config->host_numbers.emplace_back("");
    }
    for (auto &host_number : config->host_numbers) {
      config->host_names.emplace_back(
          config->rpc_server_base_name +
          host_number +
          config->rpc_server_suffix);
    }
  }
}

void CheckConstraints(Config *config) {
  // rpc_domain must be present if rpc_protocol is "verbs"
  if (config->rpc_protocol.find("verbs") != std::string::npos &&
      config->rpc_domain.empty()) {
    PrintExpectedAndFail("a non-empty value for rpc_domain");
  }

  double tolerance = 0.0000001;

  // arena_percentages must add up to 1.0
  double arena_percentage_sum = 0;
  for (int i = 0; i < kArenaType_Count; ++i) {
    arena_percentage_sum += config->arena_percentages[i];
  }
  if (fabs(1.0 - arena_percentage_sum) > tolerance) {
    std::ostringstream msg;
    msg << "the values in arena_percentages to add up to 1.0 but got ";
    msg << arena_percentage_sum << "\n";
    PrintExpectedAndFail(msg.str());
  }

  // Each slab's desired_slab_percentages should add up to 1.0
  for (int device = 0; device < config->num_devices; ++device) {
    double total_slab_percentage = 0;
    for (int slab = 0; slab < config->num_slabs[device]; ++slab) {
      total_slab_percentage += config->desired_slab_percentages[device][slab];
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

void ParseConfigYAML(YAML::Node &yaml_conf, Config *config) {
  bool capcities_specified = false, block_sizes_specified = false;
  std::vector<std::string> host_numbers;
  std::vector<std::string> host_basename;
  std::string host_suffix;

  if (yaml_conf["num_devices"]) {
    config->num_devices = yaml_conf["num_devices"].as<int>();
    config->num_targets = config->num_devices;
  }
  if (yaml_conf["num_targets"]) {
    config->num_targets = yaml_conf["num_targets"].as<int>();
  }

  if (yaml_conf["capacities_bytes"]) {
    ParseCapacities(config, yaml_conf["capacities_bytes"], 1,
                    capcities_specified);
  }
  if (yaml_conf["capacities_kb"]) {
    ParseCapacities(config, yaml_conf["capacities_kb"], KILOBYTES(1),
                    capcities_specified);
  }
  if (yaml_conf["capacities_mb"]) {
    ParseCapacities(config, yaml_conf["capacities_mb"], MEGABYTES(1),
                    capcities_specified);
  }
  if (yaml_conf["capacities_gb"]) {
    ParseCapacities(config, yaml_conf["capacities_gb"], GIGABYTES(1),
                    capcities_specified);
  }

  if (yaml_conf["block_sizes_bytes"]) {
    ParseBlockSizes(config, yaml_conf["block_sizes_bytes"],
                    1, block_sizes_specified);
  }
  if (yaml_conf["block_sizes_kb"]) {
    ParseBlockSizes(config, yaml_conf["block_sizes_kb"], KILOBYTES(1),
                    block_sizes_specified);
  }
  if (yaml_conf["block_sizes_mb"]) {
    ParseBlockSizes(config, yaml_conf["block_sizes_mb"], MEGABYTES(1),
                    block_sizes_specified);
  }
  if (yaml_conf["block_sizes_gb"]) {
    ParseBlockSizes(config, yaml_conf["block_sizes_gb"], GIGABYTES(1),
                    block_sizes_specified);
  }

  if (yaml_conf["num_slabs"]) {
    RequireNumDevices(config);
    ParseArray<int>(yaml_conf["num_slabs"], "num_slabs",
                    config->num_slabs,
                    config->num_devices);
  }
  if (yaml_conf["slab_unit_sizes"]) {
    RequireNumDevices(config);
    RequireNumSlabs(config);
    ParseMatrix<int>(yaml_conf["slab_unit_sizes"],
                     "slab_unit_sizes",
                     reinterpret_cast<int*>(config->slab_unit_sizes),
                     kMaxDevices, kMaxBufferPoolSlabs, config->num_slabs);
  }
  if (yaml_conf["desired_slab_percentages"]) {
    RequireNumDevices(config);
    RequireNumSlabs(config);
    ParseMatrix<f32>(yaml_conf["desired_slab_percentages"],
                     "desired_slab_percentages",
                     reinterpret_cast<f32*>(config->desired_slab_percentages),
                     kMaxDevices, kMaxBufferPoolSlabs, config->num_slabs);
  }
  if (yaml_conf["bandwidth_mbps"]) {
    RequireNumDevices(config);
    ParseArray<f32>(yaml_conf["bandwidth_mbps"],
                    "bandwidth_mbps",
                    config->bandwidths, config->num_devices);
  }
  if (yaml_conf["latencies"]) {
    RequireNumDevices(config);
    ParseArray<f32>(yaml_conf["latencies"],
                    "latencies",
                    config->latencies, config->num_devices);
  }
  if (yaml_conf["buffer_pool_arena_percentage"]) {
    config->arena_percentages[hermes::kArenaType_BufferPool] =
        yaml_conf["buffer_pool_arena_percentage"].as<f32>();
  }
  if (yaml_conf["metadata_arena_percentage"]) {
    config->arena_percentages[hermes::kArenaType_MetaData] =
        yaml_conf["metadata_arena_percentage"].as<f32>();
  }
  if (yaml_conf["transient_arena_percentage"]) {
    config->arena_percentages[hermes::kArenaType_Transient] =
        yaml_conf["transient_arena_percentage"].as<f32>();
  }
  if (yaml_conf["mount_points"]) {
    RequireNumDevices(config);
    ParseArray<std::string>(yaml_conf["mount_points"],
                            "mount_points",
                            config->mount_points, config->num_devices);
  }
  if (yaml_conf["swap_mount"]) {
    config->swap_mount = yaml_conf["swap_mount"].as<std::string>();
  }
  if (yaml_conf["num_buffer_organizer_retries"]) {
    config->num_buffer_organizer_retries =
        yaml_conf["num_buffer_organizer_retries"].as<int>();
  }
  if (yaml_conf["max_buckets_per_node"]) {
    config->max_buckets_per_node =
        yaml_conf["max_buckets_per_node"].as<int>();
  }
  if (yaml_conf["max_vbuckets_per_node"]) {
    config->max_vbuckets_per_node =
        yaml_conf["max_vbuckets_per_node"].as<int>();
  }
  if (yaml_conf["system_view_state_update_interval_ms"]) {
    config->system_view_state_update_interval_ms =
        yaml_conf["system_view_state_update_interval_ms"].as<int>();
  }
  if (yaml_conf["buffer_pool_shmem_name"]) {
    std::string name = yaml_conf["buffer_pool_shmem_name"].as<std::string>();
    std::snprintf(config->buffer_pool_shmem_name,
                  kMaxBufferPoolShmemNameLength,
                  "%s", name.c_str());
  }
  if (yaml_conf["rpc_protocol"]) {
    config->rpc_protocol = yaml_conf["rpc_protocol"].as<std::string>();
  }
  if (yaml_conf["rpc_domain"]) {
    config->rpc_domain = yaml_conf["rpc_domain"].as<std::string>();
  }
  if (yaml_conf["rpc_port"]) {
    config->rpc_port = yaml_conf["rpc_port"].as<int>();
  }
  if (yaml_conf["buffer_organizer_port"]) {
    config->buffer_organizer_port =
        yaml_conf["buffer_organizer_port"].as<int>();
  }
  if (yaml_conf["rpc_num_threads"]) {
    config->rpc_num_threads =
        yaml_conf["rpc_num_threads"].as<int>();
  }
  if (yaml_conf["default_placement_policy"]) {
    std::string policy =
        yaml_conf["default_placement_policy"].as<std::string>();

    if (policy == "MinimizeIoTime") {
      config->default_placement_policy =
          api::PlacementPolicy::kMinimizeIoTime;
    } else if (policy == "Random") {
      config->default_placement_policy = api::PlacementPolicy::kRandom;
    } else if (policy == "RoundRobin") {
      config->default_placement_policy = api::PlacementPolicy::kRoundRobin;
    } else {
      LOG(FATAL) << "Unknown default_placement_policy: '" << policy << "'"
                 << std::endl;
    }
  }
  if (yaml_conf["is_shared_device"]) {
    RequireNumDevices(config);
    ParseArray<int>(yaml_conf["is_shared_device"],
                    "is_shared_device",
                    config->is_shared_device, config->num_devices);
  }
  if (yaml_conf["buffer_organizer_num_threads"]) {
    config->bo_num_threads =
        yaml_conf["buffer_organizer_num_threads"].as<int>();
  }
  if (yaml_conf["default_rr_split"]) {
    config->default_rr_split = yaml_conf["default_rr_split"].as<int>();
  }
  if (yaml_conf["bo_num_threads"]) {
    config->bo_num_threads = yaml_conf["bo_num_threads"].as<int>();
  }
  if (yaml_conf["bo_capacity_thresholds"]) {
    RequireNumDevices(config);
    f32 thresholds[kMaxDevices][2] = {0};
    ParseMatrix<f32>(yaml_conf["bo_capacity_thresholds"],
                     "bo_capacity_thresholds",
                     reinterpret_cast<f32*>(thresholds),
                     kMaxDevices, 2);
    for (int i = 0; i < config->num_devices; ++i) {
      config->bo_capacity_thresholds[i].min = thresholds[i][0];
      config->bo_capacity_thresholds[i].max = thresholds[i][1];
    }
  }
  if (yaml_conf["path_exclusions"]) {
    ParseVector<std::string>(
        yaml_conf["path_exclusions"], config->path_exclusions);
  }
  if (yaml_conf["path_inclusions"]) {
    ParseVector<std::string>(
        yaml_conf["path_inclusions"], config->path_inclusions);
  }
  ParseHostNames(yaml_conf, config);
  CheckConstraints(config);
}

void ParseConfig(Arena *arena, const char *path, Config *config) {
  ScopedTemporaryMemory scratch(arena);
  InitDefaultConfig(config);
  LOG(INFO) << "ParseConfig-LoadFile" << std::endl;
  YAML::Node yaml_conf = YAML::LoadFile(path);
  LOG(INFO) << "ParseConfig-LoadComplete" << std::endl;
  ParseConfigYAML(yaml_conf, config);
}

void ParseConfigString(
    Arena *arena, const std::string &config_string, Config *config) {
  ScopedTemporaryMemory scratch(arena);
  InitDefaultConfig(config);
  YAML::Node yaml_conf = YAML::Load(config_string);
  ParseConfigYAML(yaml_conf, config);
}

void InitConfig(hermes::Config *config, const char *config_file) {
  const size_t kConfigMemorySize = KILOBYTES(16);
  hermes::u8 config_memory[kConfigMemorySize];
  if (config_file) {
    hermes::Arena config_arena = {};
    hermes::InitArena(&config_arena, kConfigMemorySize, config_memory);
    hermes::ParseConfig(&config_arena, config_file, config);
  } else {
    InitDefaultConfig(config);
  }
}

}  // namespace hermes
