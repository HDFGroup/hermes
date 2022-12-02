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

namespace hermes {

/**
 * Configuration used to intialize client
 * */

struct ClientConfig {
 public:
  bool stop_daemon_;

 public:
  ClientConfig();
  void LoadText(const std::string &text);
  void LoadFromFile(const char *path);
  void LoadDefault();
};

struct DeviceInfo {
  /** The minimum transfer size of each device */
  size_t block_size_;
  /** The unit of each slab, a multiple of the Device's block size */
  lipcl::vector<size_t> slab_sizes_;
  /** The mount point of a device */
  lipcl::string mount_point_;
  /** Device capacity (bytes) */
  size_t capacity_;
  /** Bandwidth of a device (MBps) */
  f32 bandwidth_;
  /** Latency of the device (us)*/
  f32 latency_;
  /** Whether or not the device is shared among all nodes */
  bool is_shared_;
  /** */
};

/**
 * System and user configuration that is used to initialize Hermes.
 */
class ServerConfig {
 public:
  /** The device information */
  lipcl::vector<DeviceInfo> devices_;

  /** The length of a view state epoch */
  u32 system_view_state_update_interval_ms;

  /** The name of a file that contains host names, 1 per line */
  lipcl::string rpc_server_host_file;
  /** The hostname of the RPC server, minus any numbers that Hermes may
   * auto-generate when the rpc_hostNumber_range is specified. */
  lipcl::string rpc_server_base_name;
  /** The list of numbers from all server names. E.g., '{1, 3}' if your servers
   * are named ares-comp-1 and ares-comp-3 */
  lipcl::vector<lipcl::string> host_numbers;
  /** The RPC server name suffix. This is appended to the base name plus host
      number. */
  lipcl::string rpc_server_suffix;
  /** The parsed hostnames from the hermes conf */
  lipcl::vector<lipcl::string> host_names;
  /** The RPC protocol to be used. */
  lipcl::string rpc_protocol;
  /** The RPC domain name for verbs transport. */
  lipcl::string rpc_domain;
  /** The RPC port number. */
  int rpc_port;
  /** The RPC port number for the buffer organizer. */
  int buffer_organizer_port;
  /** The number of handler threads per RPC server. */
  int rpc_num_threads;
  /** The number of buffer organizer threads. */
  int bo_num_threads;
  /** The default blob placement policy. */
  api::PlacementPolicy default_placement_policy;
  /** Whether blob splitting is enabled for Round-Robin blob placement. */
  bool default_rr_split;
  /** The min and max capacity threshold in MiB for each device at which the
   * BufferOrganizer will trigger. */
  Thresholds bo_capacity_thresholds[kMaxDevices];
  /** A base name for the BufferPool shared memory segement. Hermes appends the
   * value of the USER environment variable to this string.
   */
  char buffer_pool_shmem_name[kMaxBufferPoolShmemNameLength];

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
  ServerConfig();
  void LoadText(const std::string &text);
  void LoadFromFile(const char *path);
  void LoadDefault();

 private:
  void ParseYAML(YAML::Node &yaml_conf);
  void CheckConstraints();
  void ParseHostNames(YAML::Node yaml_conf);
};

/** print \a expected value and fail when an error occurs */
void PrintExpectedAndFail(const std::string &expected, u32 line_number = 0) {
  std::ostringstream msg;
  msg << "Configuration parser expected '" << expected << "'";
  if (line_number > 0) {
    msg << " on line " << line_number;
  }
  msg << "\n";

  LOG(FATAL) << msg.str();
}

/** parse \a var array from configuration file in YAML */
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

/** parse \a list_node vector from configuration file in YAML */
template<typename T>
void ParseVector(YAML::Node list_node, std::vector<T> &list) {
  for (auto val_node : list_node) {
    list.emplace_back(val_node.as<T>());
  }
}

/** parse \a matrix_node matrix using \a col_len column length */
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

/** parse \a matrix_node matrix from configuration file in YAML */
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

/** parse range list from configuration file in YAML */
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

std::string ParseNumberSuffix(const std::string &num_text) {
  int i;
  for (i = 0; i < num_text.size(); ++i) {
    char c = num_text[i];
    if ('0' <= c && c <= '9') continue;
    if (c == ' ' || c == '\t' || c == '\n' || c == '\r') continue;
    break;
  }
  return std::string(num_text.begin() + i, num_text.end());;
}

size_t ParseNumber(const std::string &num_text) {
  size_t size;
  std::stringstream(num_text) >> size;
  return size;
}

/** Returns size (bytes) */
size_t ParseSize(const std::string &size_text) {
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
size_t ParseBandwidth(const std::string &size_text) {
  return ParseSize(size_text);
}

/** Returns latency (nanoseconds) */
size_t ParseLatency(const std::string &latency_text) {
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
  else {
    LOG(FATAL) << "Could not parse the latency: " << latency_text << std::endl;
  }
}

}  // namespace hermes
#endif  // HERMES_CONFIG_PARSER_H_
