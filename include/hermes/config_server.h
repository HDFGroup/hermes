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

#ifndef HERMES_SRC_CONFIG_SERVER_H_
#define HERMES_SRC_CONFIG_SERVER_H_

#include "config.h"
#include "config_server_default.h"

namespace hermes::config {

struct DeviceInfo; /** Forward declaration of DeviceInfo */

/**
 * The type of interface the device exposes
 * */
enum class IoInterface {
  kRam,
  kPosix
};

/**
 * DeviceInfo shared-memory representation
 * */
struct DeviceInfo {
  /** The human-readable name of the device */
  std::string dev_name_;
  /** The unit of each slab, a multiple of the Device's block size */
  std::vector<size_t> slab_sizes_;
  /** The directory the device is mounted on */
  std::string mount_dir_;
  /** The file to create on the device */
  std::string mount_point_;
  /** The I/O interface for the device */
  IoInterface io_api_;
  /** The minimum transfer size of each device */
  size_t block_size_;
  /** Device capacity (bytes) */
  size_t capacity_;
  /** Bandwidth of a device (MBps) */
  f32 bandwidth_;
  /** Latency of the device (us)*/
  f32 latency_;
  /** Whether or not the device is shared among all nodes */
  bool is_shared_;
  /** BORG's minimum and maximum capacity threshold for device */
  f32 borg_min_thresh_, borg_max_thresh_;
};

/**
 * RPC information defined in server config
 * */
struct RpcInfo {
  /** The name of a file that contains host names, 1 per line */
  std::string host_file_;
  /** The parsed hostnames from the hermes conf */
  std::vector<std::string> host_names_;
  /** The RPC protocol to be used. */
  std::string protocol_;
  /** The RPC domain name for verbs transport. */
  std::string domain_;
  /** The RPC port number. */
  int port_;
  /** The number of handler threads per RPC server. */
  int num_threads_;
};

/**
 * DPE information defined in server config
 * */
struct DpeInfo {
  /** The default blob placement policy. */
  PlacementPolicy default_policy_;

  /** Whether blob splitting is enabled for Round-Robin blob placement. */
  bool default_rr_split_;
};

/**
 * Buffer organizer information defined in server config
 * */
struct BorgInfo {
  /** The number of buffer organizer threads. */
  int num_threads_;
  /** Interval (seconds) where blobs are checked for flushing */
  size_t flush_period_;
  /** Interval (seconds) where blobs are checked for re-organization */
  size_t blob_reorg_period_;
  /** Time when score is equal to 1 (seconds) */
  float recency_min_;
  /** Time when score is equal to 0 (seconds) */
  float recency_max_;
  /** Number of accesses for score to be equal to 1 (count) */
  float freq_max_;
  /** Number of accesses for score to be equal to 0 (count) */
  float freq_min_;
};

/**
 * Prefetcher information in server config
 * */
struct PrefetchInfo {
  bool enabled_;
  std::string trace_path_;
  std::string apriori_schema_path_;
  size_t epoch_ms_;
  bool is_mpi_;
};

/**
 * Tracing information in server config
 * */
struct TracingInfo {
  bool enabled_;
  std::string output_;
};

/** MDM information */
struct MdmInfo {
  /** Number of buckets in mdm bucket map before collisions */
  size_t num_bkts_;

  /** Number of blobs in mdm blob map before collisions */
  size_t num_blobs_;

  /** Number of traits in mdm trait map before collisions */
  size_t num_traits_;
};

/**
 * System configuration for Hermes
 */
class ServerConfig : public BaseConfig {
 public:
  /** The device information */
  std::vector<DeviceInfo> devices_;

  /** The RPC information */
  RpcInfo rpc_;

  /** The DPE information */
  DpeInfo dpe_;

  /** Buffer organizer (BORG) information */
  BorgInfo borg_;

  /** Tracing information */
  TracingInfo tracing_;

  /** Prefetcher information */
  PrefetchInfo prefetcher_;

  /** Metadata Manager information */
  MdmInfo mdm_;

  /** Trait repo information */
  std::vector<std::string> trait_paths_;

  /** The length of a view state epoch */
  u32 system_view_state_update_interval_ms;

 public:
  /** Default constructor */
  ServerConfig() = default;

  /** Load the default configuration */
  void LoadDefault() {
    LoadText(kHermesServerDefaultConfigStr, false);
  }

 private:
  /** parse the YAML node */
  void ParseYAML(YAML::Node &yaml_conf)  {
    if (yaml_conf["devices"]) {
      ParseDeviceInfo(yaml_conf["devices"]);
    }
    if (yaml_conf["dpe"]) {
      ParseDpeInfo(yaml_conf["dpe"]);
    }
    if (yaml_conf["buffer_organizer"]) {
      ParseBorgInfo(yaml_conf["buffer_organizer"]);
    }
    if (yaml_conf["tracing"]) {
      ParseTracingInfo(yaml_conf["tracing"]);
    }
    if (yaml_conf["prefetch"]) {
      ParsePrefetchInfo(yaml_conf["prefetch"]);
    }
    if (yaml_conf["mdm"]) {
      ParseMdmInfo(yaml_conf["mdm"]);
    }
    if (yaml_conf["system_view_state_update_interval_ms"]) {
      system_view_state_update_interval_ms =
          yaml_conf["system_view_state_update_interval_ms"].as<int>();
    }
    if (yaml_conf["traits"]) {
      ParseTraitInfo(yaml_conf["traits"]);
    }
  }


  /** parse device information from YAML config */
  void ParseDeviceInfo(YAML::Node yaml_conf) {
    devices_.clear();
    for (auto device : yaml_conf) {
      devices_.emplace_back();
      DeviceInfo &dev = devices_.back();
      auto dev_info = device.second;
      dev.dev_name_ = device.first.as<std::string>();
      dev.mount_dir_ = hshm::ConfigParse::ExpandPath(
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
      dev.slab_sizes_.reserve(size_vec.size());
      for (const std::string &size_str : size_vec) {
        dev.slab_sizes_.emplace_back(hshm::ConfigParse::ParseSize(size_str));
      }
    }
  }

  /** parse dpe information from YAML config */
  void ParseDpeInfo(YAML::Node yaml_conf) {
    if (yaml_conf["default_placement_policy"]) {
      std::string policy =
          yaml_conf["default_placement_policy"].as<std::string>();
      dpe_.default_policy_ = PlacementPolicyConv::to_enum(policy);
    }
  }

  /** parse buffer organizer information from YAML config */
  void ParseBorgInfo(YAML::Node yaml_conf) {
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
  void ParsePrefetchInfo(YAML::Node yaml_conf) {
    if (yaml_conf["enabled"]) {
      tracing_.enabled_ = yaml_conf["enabled"].as<bool>();
    }
    if (yaml_conf["output"]) {
      tracing_.output_ = hshm::ConfigParse::ExpandPath(
          yaml_conf["output"].as<std::string>());
    }
  }

  /** parse prefetch information from YAML config */
  void ParseTracingInfo(YAML::Node yaml_conf) {
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
    if (yaml_conf["apriori_schema_path"]) {
      prefetcher_.apriori_schema_path_ =
          yaml_conf["apriori_schema_path"].as<std::string>();
    }
  }

  /** parse prefetch information from YAML config */
  void ParseTraitInfo(YAML::Node yaml_conf) {
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

  /** parse prefetch information from YAML config */
  void ParseMdmInfo(YAML::Node yaml_conf) {
    mdm_.num_blobs_ = yaml_conf["est_blob_count"].as<size_t>();
    mdm_.num_bkts_ = yaml_conf["est_blob_count"].as<size_t>();
    mdm_.num_traits_ = yaml_conf["est_num_traits"].as<size_t>();
  }
};

}  // namespace hermes::config

namespace hermes {
using config::ServerConfig;
using config::DeviceInfo;
}  // namespace hermes

#endif  // HERMES_SRC_CONFIG_SERVER_H_
