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
#include <labstor/data_structures/lockless/vector.h>

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

/**
 * System and user configuration that is used to initialize Hermes.
 */
class ServerConfig {
 public:
  /** The total capacity of each buffering Device */
  size_t capacities_[kMaxDevices];
  /** The block sizes for each device */
  lipcl::vector<int> block_sizes_;

  /** The block sizes of each Device */
  int block_sizes_[kMaxDevices];
  /** The unit of each slab, a multiple of the Device's block size */
  int slab_unit_sizes[kMaxDevices][kMaxBufferPoolSlabs];
  /** The percentage of space each slab should occupy per Device. The values
   * for each Device should add up to 1.0.
   */
  f32 desired_slab_percentages[kMaxDevices][kMaxBufferPoolSlabs];
  /** The bandwidth of each Device */
  f32 bandwidths[kMaxDevices];
  /** The latency of each Device */
  f32 latencies[kMaxDevices];
  /** The number of Devices */
  int num_devices;
  /** The number of Targets */
  int num_targets;

  /** The maximum number of buckets per node */
  u32 max_buckets_per_node;
  /** The maximum number of vbuckets per node */
  u32 max_vbuckets_per_node;
  /** The length of a view state epoch */
  u32 system_view_state_update_interval_ms;

  /** The mount point or desired directory for each Device. RAM Device should
   * be the empty string.
   */
  std::string mount_points[kMaxDevices];
  /** The mount point of the swap target. */
  std::string swap_mount;
  /** The number of times the BufferOrganizer will attempt to place a swap
   * blob into the hierarchy before giving up. */
  int num_buffer_organizer_retries;

  /** If non-zero, the device is shared among all nodes (e.g., burst buffs) */
  int is_shared_device[kMaxDevices];

  /** The name of a file that contains host names, 1 per line */
  std::string rpc_server_host_file;
  /** The hostname of the RPC server, minus any numbers that Hermes may
   * auto-generate when the rpc_hostNumber_range is specified. */
  std::string rpc_server_base_name;
  /** The list of numbers from all server names. E.g., '{1, 3}' if your servers
   * are named ares-comp-1 and ares-comp-3 */
  std::vector<std::string> host_numbers;
  /** The RPC server name suffix. This is appended to the base name plus host
      number. */
  std::string rpc_server_suffix;
  /** The parsed hostnames from the hermes conf */
  std::vector<std::string> host_names;
  /** The RPC protocol to be used. */
  std::string rpc_protocol;
  /** The RPC domain name for verbs transport. */
  std::string rpc_domain;
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

}  // namespace hermes
#endif  // HERMES_CONFIG_PARSER_H_
