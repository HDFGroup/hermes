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

#include "config_parser.h"


// Steps to add a new configuration variable:
// 6. Add an Assert to config_parser_test.cc to test the functionality.
// 7. Set a default value in InitDefaultConfig
// 8. Add the variable with documentation to test/data/hermes.conf

namespace hermes {

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

void ParseConfigYAML(Arena *arena, YAML::Node &yaml_conf, Config *config) {
  if(yaml_conf["num_devices"]) {
    config->num_devices = yaml_conf["num_devices"].as<int>();
    config->num_targets = yaml_conf["num_devices"].as<int>();
  }
  if(yaml_conf["num_targets"]) {
    config->num_targets = yaml_conf["num_targets"].as<int>();
  }

  if(yaml_conf["capacities_bytes"]) {
    ParseCapacities(config, yaml_conf["capacities_bytes"], 1);
  }
  if(yaml_conf["capacities_kb"]) {
    ParseCapacities(config, yaml_conf["capacities_kb"], KILOBYTES(1));
  }
  if(yaml_conf["capacities_mb"]) {
    ParseCapacities(config, yaml_conf["capacities_mb"], MEGABYTES(1));
  }
  if(yaml_conf["capacities_gb"]) {
    ParseCapacities(config, yaml_conf["capacities_gb"], GIGABYTES(1));
  }

  if(yaml_conf["block_sizes_bytes"]) {
    ParseBlockSizes(config, yaml_conf["block_sizes_bytes"], 1);
  }
  if(yaml_conf["block_sizes_kb"]) {
    ParseBlockSizes(config, yaml_conf["block_sizes_kb"], KILOBYTES(1));
  }
  if(yaml_conf["block_sizes_mb"]) {
    ParseBlockSizes(config, yaml_conf["block_sizes_mb"], MEGABYTES(1));
  }
  if(yaml_conf["block_sizes_gb"]) {
    ParseBlockSizes(config, yaml_conf["block_sizes_gb"], GIGABYTES(1));
  }

  if(yaml_conf["num_slabs"]) {
    RequireNumDevices(config);
    ParseList<int>(yaml_conf["num_slabs"], config->num_slabs, config->num_devices);
  }
  if(yaml_conf["slab_unit_sizes"]) {
    RequireNumDevices(config);
    RequireNumSlabs(config);
    ParseSlabUnitSizes(config, yaml_conf["slab_unit_sizes"], config->num_devices, config->num_slabs);
  }
  if(yaml_conf["desired_slab_percentages"]) {
    RequireNumDevices(config);
    RequireNumSlabs(config);
    ParseDesiredSlabPercentages(config, yaml_conf["desired_slab_percentages"], config->num_devices, config->num_slabs);
  }
  if(yaml_conf["bandwidths_mbps"]) {
    RequireNumDevices(config);
    ParseList<f32>(yaml_conf["bandwidths_mbps"], config->bandwidths, config->num_devices);
  }
  if(yaml_conf["latencies"]) {
    RequireNumDevices(config);
    ParseList<f32>(yaml_conf["latencies"], config->latencies, config->num_devices);
  }
  if(yaml_conf["buffer_pool_arena_percentage"]) {
    config->arena_percentages[hermes::kArenaType_BufferPool] = yaml_conf["buffer_pool_arena_percentage"].as<f32>();
  }
  if(yaml_conf["metadata_arena_percentage"]) {
    config->arena_percentages[hermes::kArenaType_MetaData] = yaml_conf["metadata_arena_percentage"].as<f32>();
  }
  if(yaml_conf["transient_arena_percentage"]) {
    config->arena_percentages[hermes::kArenaType_Transient] = yaml_conf["transient_arena_percentage"].as<f32>();
  }
  if(yaml_conf["mount_points"]) {
    RequireNumDevices(config);
    ParseList<std::string>(yaml_conf["mount_points"], config->mount_points, config->num_devices);
  }
  if(yaml_conf["swap_mount"]) {
    config->swap_mount = yaml_conf["swap_mount"].as<std::string>();
  }
  if(yaml_conf["num_buffer_organizer_retries"]) {
    config->num_buffer_organizer_retries = yaml_conf["num_buffer_organizer_retries"].as<int>();
  }
  if(yaml_conf["max_buckets_per_node"]) {
    config->max_buckets_per_node = yaml_conf["max_buckets_per_node"].as<int>();
  }
  if(yaml_conf["max_vbuckets_per_node"]) {
    config->max_vbuckets_per_node = yaml_conf["max_vbuckets_per_node"].as<int>();
  }
  if(yaml_conf["system_view_state_update_interval_ms"]) {
    config->system_view_state_update_interval_ms = yaml_conf["system_view_state_update_interval_ms"].as<int>();
  }
  if(yaml_conf["rpc_server_host_file"]) {
    config->rpc_server_host_file = yaml_conf["rpc_server_host_file"].as<std::string>();
  }
  if(yaml_conf["rpc_server_base_name"]) {
    config->rpc_server_base_name = yaml_conf["rpc_server_base_name"].as<std::string>();
  }
  if(yaml_conf["rpc_server_suffix"]) {
    config->rpc_server_suffix = yaml_conf["rpc_server_suffix"].as<std::string>();
  }
  if(yaml_conf["buffer_pool_shmem_name"]) {
    std::string name = yaml_conf["buffer_pool_shmem_name"].as<std::string>();
    std::strcpy(config->buffer_pool_shmem_name, name.c_str());
  }
  if(yaml_conf["rpc_protocol"]) {
    config->rpc_protocol = yaml_conf["rpc_protocol"].as<std::string>();
  }
  if(yaml_conf["rpc_domain"]) {
    config->rpc_domain = yaml_conf["rpc_domain"].as<std::string>();
  }
  if(yaml_conf["rpc_port"]) {
    config->rpc_port = yaml_conf["rpc_port"].as<int>();
  }
  if(yaml_conf["buffer_organizer_port"]) {
    config->buffer_organizer_port = yaml_conf["buffer_organizer_port"].as<int>();
  }
  if(yaml_conf["rpc_host_number_range"]) {
    ParseRangeList(yaml_conf["rpc_host_number_range"], config->host_numbers);
  }
  if(yaml_conf["rpc_num_threads"]) {
    config->rpc_num_threads = yaml_conf["rpc_num_threads"].as<int>();
  }
  if(yaml_conf["default_placement_policy"]) {
    std::string policy = yaml_conf["default_placement_policy"].as<std::string>();

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
  if(yaml_conf["is_shared_device"]) {
    RequireNumDevices(config);
    ParseList<int>(yaml_conf["is_shared_device"], config->is_shared_device, config->num_devices);
  }
  if(yaml_conf["buffer_organizer_num_threads"]) {
    config->bo_num_threads = yaml_conf["buffer_organizer_num_threads"].as<int>();
  }
  if(yaml_conf["default_rr_split"]) {
    config->default_rr_split = yaml_conf["default_rr_split"].as<int>();
  }

  /*switch (var) {
    default: {
      HERMES_INVALID_CODE_PATH;
      break;
    }
  }*/
}

void ParseConfig(Arena *arena, const char *path, Config *config) {
  ScopedTemporaryMemory scratch(arena);
  InitDefaultConfig(config);
  YAML::Node yaml_conf = YAML::LoadFile(path);
  ParseConfigYAML(arena, yaml_conf, config);
}

void ParseConfigString(Arena *arena, const std::string &config_string, Config *config) {
  ScopedTemporaryMemory scratch(arena);
  InitDefaultConfig(config);
  YAML::Node yaml_conf = YAML::Load(config_string);
  ParseConfigYAML(arena, yaml_conf, config);
}

}  // namespace hermes
