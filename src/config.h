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

#ifndef HERMES_CONFIG_PARSER_H_
#define HERMES_CONFIG_PARSER_H_

#include <string.h>
#include <yaml-cpp/yaml.h>
#include <iomanip>
#include <ostream>
#include <vector>
#include <sstream>
#include "data_structures.h"

#include "hermes_types.h"
#include "constants.h"

namespace hermes {

class BaseConfig {
 public:
  /** load configuration from a string */
  void LoadText(const std::string &config_string, bool with_default = true) {
    if (with_default) { LoadDefault(); }
    if (config_string.size() == 0) {
      return;
    }
    YAML::Node yaml_conf = YAML::Load(config_string);
    ParseYAML(yaml_conf);
  }

  /** load configuration from file */
  void LoadFromFile(const std::string &path, bool with_default = true) {
    if (with_default) { LoadDefault(); }
    if (path.size() == 0) {
      return;
    }
    LOG(INFO) << "ParseConfig-LoadFile" << std::endl;
    YAML::Node yaml_conf = YAML::LoadFile(path);
    LOG(INFO) << "ParseConfig-LoadComplete" << std::endl;
    ParseYAML(yaml_conf);
  }

  /** load the default configuration */
  virtual void LoadDefault() = 0;

 private:
  virtual void ParseYAML(YAML::Node &yaml_conf) = 0;
};

/**
 * Configuration used to intialize client
 * */

class ClientConfig : public BaseConfig {
 public:
  bool stop_daemon_;

 public:
  ClientConfig() = default;
  void LoadDefault() override;

 private:
   void ParseYAML(YAML::Node &yaml_conf) override;
};

struct DeviceInfo {
  /** The minimum transfer size of each device */
  size_t block_size_;
  /** The unit of each slab, a multiple of the Device's block size */
  std::vector<size_t> slab_sizes_;
  /** The mount point of a device */
  std::string mount_point_;
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

struct DpeInfo {
  /** The default blob placement policy. */
  api::PlacementPolicy default_policy_;

  /** Whether blob splitting is enabled for Round-Robin blob placement. */
  bool default_rr_split_;
};

struct BorgInfo {
  /** The RPC port number for the buffer organizer. */
  int port_;
  /** The number of buffer organizer threads. */
  int num_threads_;
};

/**
 * System and user configuration that is used to initialize Hermes.
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

  /** The length of a view state epoch */
  u32 system_view_state_update_interval_ms;

  /** A base name for the BufferPool shared memory segement. Hermes appends the
   * value of the USER environment variable to this string.
   */
  std::string shmem_name_;

  /**
   * Paths prefixed with the following directories are not tracked in Hermes
   * Exclusion list used by darshan at
   * darshan/darshan-runtime/lib/darshan-core.c
   */
  std::vector<std::string> path_exclusions;

  /**
   * Paths prefixed with the following directories are tracked by Hermes even if
   * they share a root with a path listed in path_exclusions
   */
  std::vector<std::string> path_inclusions;

 public:
  ServerConfig() = default;
  void LoadDefault();

 private:
  void ParseYAML(YAML::Node &yaml_conf);
  void CheckConstraints();
  void ParseRpcInfo(YAML::Node yaml_conf);
  void ParseDeviceInfo(YAML::Node yaml_conf);
  void ParseDpeInfo(YAML::Node yaml_conf);
  void ParseBorgInfo(YAML::Node yaml_conf);
};

/** print \a expected value and fail when an error occurs */
static void PrintExpectedAndFail(const std::string &expected, u32 line_number = 0) {
  std::ostringstream msg;
  msg << "Configuration parser expected '" << expected << "'";
  if (line_number > 0) {
    msg << " on line " << line_number;
  }
  msg << "\n";

  LOG(FATAL) << msg.str();
}

/** parse \a list_node vector from configuration file in YAML */
template<typename T>
static void ParseVector(YAML::Node list_node, std::vector<T> &list) {
  for (auto val_node : list_node) {
    list.emplace_back(val_node.as<T>());
  }
}

/** parse range list from configuration file in YAML */
static void ParseRangeList(YAML::Node list_node, std::string var,
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

static std::string ParseNumberSuffix(const std::string &num_text) {
  int i;
  for (i = 0; i < num_text.size(); ++i) {
    char c = num_text[i];
    if ('0' <= c && c <= '9') continue;
    if (c == ' ' || c == '\t' || c == '\n' || c == '\r') continue;
    break;
  }
  return std::string(num_text.begin() + i, num_text.end());;
}

static size_t ParseNumber(const std::string &num_text) {
  size_t size;
  std::stringstream(num_text) >> size;
  return size;
}

/** Returns size (bytes) */
static size_t ParseSize(const std::string &size_text) {
  size_t size = ParseNumber(size_text);
  std::string suffix = ParseNumberSuffix(size_text);
  if (suffix.size() == 0) {
    return size;
  } else if (suffix[0] == 'k' || suffix[0] == 'k') {
    return KILOBYTES(size);
  } else if (suffix[0] == 'm' || suffix[0] == 'M') {
    return MEGABYTES(size);
  } else if (suffix[0] == 'g' || suffix[0] == 'G') {
    return GIGABYTES(size);
  } else {
    LOG(FATAL) << "Could not parse the size: " << size_text << std::endl;
  }
}

/** Returns bandwidth (bytes / second) */
static size_t ParseBandwidth(const std::string &size_text) {
  return ParseSize(size_text);
}

/** Returns latency (nanoseconds) */
static size_t ParseLatency(const std::string &latency_text) {
  size_t size = ParseNumber(latency_text);
  std::string suffix = ParseNumberSuffix(latency_text);
  if (suffix.size() == 0) {
    return size;
  } else if (suffix[0] == 'n' || suffix[0] == 'N') {
    return size;
  } else if (suffix[0] == 'u' || suffix[0] == 'U') {
    return KILOBYTES(size);
  } else if (suffix[0] == 'm' || suffix[0] == 'M') {
    return MEGABYTES(size);
  } else if (suffix[0] == 's' || suffix[0] == 'S') {
    return GIGABYTES(size);
  }
  LOG(FATAL) << "Could not parse the latency: " << latency_text << std::endl;
}

}  // namespace hermes
#endif  // HERMES_CONFIG_PARSER_H_
