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

#include <string.h>
#include <yaml-cpp/yaml.h>
#include <ostream>
#include "hermes_shm/util/logging.h"
#include "utils.h"
#include "config.h"
#include "hermes_shm/util/config_parse.h"

#include "config_server.h"
#include "config_server_default.h"

namespace hermes::config {

/** parse device information from YAML config */
void ServerConfig::ParseDeviceInfo(YAML::Node yaml_conf) {
  devices_ = hipc::make_uptr<hipc::vector<DeviceInfo>>();
  for (auto device : yaml_conf) {
    devices_->emplace_back();
    DeviceInfo &dev = devices_->back();
    auto dev_info = device.second;
    (*dev.dev_name_) = device.first.as<std::string>();
    (*dev.mount_dir_) = hshm::ConfigParse::ExpandPath(
        dev_info["mount_point"].as<std::string>());
    dev.borg_min_thresh_ =
        dev_info["borg_capacity_thresh"][0].as<float>();
    dev.borg_max_thresh_ =
        dev_info["borg_capacity_thresh"][1].as<float>();
    dev.is_shared_ =
        dev_info["is_shared_device"].as<bool>();
    dev.block_size_ =
        hshm::ConfigParse::ParseSize(dev_info["block_size"].as<std::string>());
    dev.capacity_ =
        hshm::ConfigParse::ParseSize(dev_info["capacity"].as<std::string>());
    dev.bandwidth_ =
        hshm::ConfigParse::ParseSize(dev_info["bandwidth"].as<std::string>());
    dev.latency_ =
        hshm::ConfigParse::ParseLatency(dev_info["latency"].as<std::string>());
    std::vector<std::string> size_vec;
    ParseVector<std::string, std::vector<std::string>>(
        dev_info["slab_sizes"], size_vec);
    dev.slab_sizes_->reserve(size_vec.size());
    for (const std::string &size_str : size_vec) {
      dev.slab_sizes_->emplace_back(hshm::ConfigParse::ParseSize(size_str));
    }
  }
}

/** parse RPC information from YAML config */
void ServerConfig::ParseRpcInfo(YAML::Node yaml_conf) {
  std::string suffix;

  if (yaml_conf["host_file"]) {
    rpc_.host_file_ =
        hshm::ConfigParse::ExpandPath(yaml_conf["host_file"].as<std::string>());
    rpc_.host_names_.clear();
  }
  if (yaml_conf["host_names"] && rpc_.host_file_.size() == 0) {
    // NOTE(llogan): host file is prioritized
    rpc_.host_names_.clear();
    for (YAML::Node host_name_gen : yaml_conf["host_names"]) {
      std::string host_names = host_name_gen.as<std::string>();
      hshm::ConfigParse::ParseHostNameString(host_names, rpc_.host_names_);
    }
  }
  if (yaml_conf["domain"]) {
    rpc_.domain_ = yaml_conf["domain"].as<std::string>();
  }
  if (yaml_conf["protocol"]) {
    rpc_.protocol_ = yaml_conf["protocol"].as<std::string>();
  }
  if (yaml_conf["port"]) {
    rpc_.port_ = yaml_conf["port"].as<int>();
  }
  if (yaml_conf["num_threads"]) {
    rpc_.num_threads_ = yaml_conf["num_threads"].as<int>();
  }
}

/** parse dpe information from YAML config */
void ServerConfig::ParseDpeInfo(YAML::Node yaml_conf) {
  if (yaml_conf["default_placement_policy"]) {
    std::string policy =
        yaml_conf["default_placement_policy"].as<std::string>();
    dpe_.default_policy_ = api::PlacementPolicyConv::to_enum(policy);
  }
}

/** parse buffer organizer information from YAML config */
void ServerConfig::ParseBorgInfo(YAML::Node yaml_conf) {
  if (yaml_conf["num_threads"]) {
    borg_.num_threads_ = yaml_conf["num_threads"].as<int>();
  }
  if (yaml_conf["flush_period"]) {
    borg_.flush_period_ = yaml_conf["flush_period"].as<size_t>();
  }
  if (yaml_conf["blob_reorg_period"]) {
    borg_.blob_reorg_period_ = yaml_conf["blob_reorg_period"].as<size_t>();
  }
  if (yaml_conf["recency_min"]) {
    borg_.recency_min_ = yaml_conf["recency_min"].as<float>();
  }
  if (yaml_conf["recency_max"]) {
    borg_.recency_max_ = yaml_conf["recency_max"].as<float>();
  }
  if (yaml_conf["freq_max"]) {
    borg_.freq_max_ = yaml_conf["freq_max"].as<float>();
  }
  if (yaml_conf["freq_min"]) {
    borg_.freq_min_ = yaml_conf["freq_min"].as<float>();
  }
}

/** parse I/O tracing information from YAML config */
void ServerConfig::ParseTracingInfo(YAML::Node yaml_conf) {
  if (yaml_conf["enabled"]) {
    tracing_.enabled_ = yaml_conf["enabled"].as<bool>();
  }
  if (yaml_conf["output"]) {
    tracing_.output_ = hshm::ConfigParse::ExpandPath(
        yaml_conf["output"].as<std::string>());
  }
}

/** parse prefetch information from YAML config */
void ServerConfig::ParsePrefetchInfo(YAML::Node yaml_conf) {
  if (yaml_conf["enabled"]) {
    prefetcher_.enabled_ = yaml_conf["enabled"].as<bool>();
  }
  if (yaml_conf["io_trace_path"]) {
    prefetcher_.trace_path_ = hshm::ConfigParse::ExpandPath(
        yaml_conf["io_trace_path"].as<std::string>());
  }
  if (yaml_conf["epoch_ms"]) {
    prefetcher_.epoch_ms_ = yaml_conf["epoch_ms"].as<size_t>();
  }
  if (yaml_conf["is_mpi"]) {
    prefetcher_.is_mpi_ = yaml_conf["is_mpi"].as<bool>();
  }
}

/** parse prefetch information from YAML config */
void ServerConfig::ParseTraitInfo(YAML::Node yaml_conf) {
  std::vector<std::string> trait_names;
  ParseVector<std::string, std::vector<std::string>>(
      yaml_conf, trait_names);
  trait_paths_.reserve(trait_names.size());
  for (auto &name : trait_names) {
    name = hshm::ConfigParse::ExpandPath(name);
    trait_paths_.emplace_back(
        hshm::Formatter::format("lib{}.so", name));
  }
}

/** parse the YAML node */
void ServerConfig::ParseYAML(YAML::Node &yaml_conf) {
  if (yaml_conf["devices"]) {
    ParseDeviceInfo(yaml_conf["devices"]);
  }
  if (yaml_conf["rpc"]) {
    ParseRpcInfo(yaml_conf["rpc"]);
  }
  if (yaml_conf["dpe"]) {
    ParseDpeInfo(yaml_conf["dpe"]);
  }
  if (yaml_conf["buffer_organizer"]) {
    ParseBorgInfo(yaml_conf["buffer_organizer"]);
  }
  if (yaml_conf["tracing"]) {
    ParsePrefetchInfo(yaml_conf["tracing"]);
  }
  if (yaml_conf["prefetch"]) {
    ParsePrefetchInfo(yaml_conf["prefetch"]);
  }
  if (yaml_conf["system_view_state_update_interval_ms"]) {
    system_view_state_update_interval_ms =
        yaml_conf["system_view_state_update_interval_ms"].as<int>();
  }
  if (yaml_conf["traits"]) {
    ParseTraitInfo(yaml_conf["traits"]);
  }
  if (yaml_conf["shmem_name"]) {
    shmem_name_ = yaml_conf["shmem_name"].as<std::string>();
  }
  if (yaml_conf["max_memory"]) {
    max_memory_ = hshm::ConfigParse::ParseSize(
      yaml_conf["max_memory"].as<std::string>());
  }
}

/** Load the default configuration */
void ServerConfig::LoadDefault() {
  LoadText(kServerDefaultConfigStr, false);
}

}  // namespace hermes::config
