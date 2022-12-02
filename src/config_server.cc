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
    for (auto host_number : host_numbers) {
      host_names.emplace_back(rpc_server_base_name +
                                      host_number + rpc_server_suffix);
    }
  }
}

/** parse the YAML node */
void ServerConfig::ParseYAML(YAML::Node &yaml_conf) {
  bool capcities_specified = false, block_sizes_specified = false;
  std::vector<std::string> host_numbers;
  std::vector<std::string> host_basename;
  std::string host_suffix;

  if (yaml_conf["devices"]) {
    for (auto device : yaml_conf["devices"]) {
      DeviceInfo dev;
      auto dev_info = device.second;
      dev.mount_point_ = dev_info["mount_point"].as<std::string>();
      dev.capacity_ =
          ParseSize(dev_info["capacity"].as<std::string>());
      dev.bandwidth_ =
          ParseSize(dev_info["bandwidth"].as<std::string>());
      dev.latency_ =
          ParseLatency(dev_info["latency"].as<std::string>());
    }
  }
  if (yaml_conf["system_view_state_update_interval_ms"]) {
    system_view_state_update_interval_ms =
        yaml_conf["system_view_state_update_interval_ms"].as<int>();
  }
  if (yaml_conf["shmem_name"]) {
    std::string name = yaml_conf["shmem_name"].as<std::string>();
    std::snprintf(shmem_name_, kMaxBufferPoolShmemNameLength,
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
