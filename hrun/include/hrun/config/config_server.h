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

#ifndef HRUN_SRC_CONFIG_SERVER_H_
#define HRUN_SRC_CONFIG_SERVER_H_

#include "config.h"
#include "hrun/hrun_types.h"

namespace hrun::config {

/**
 * Work orchestrator information defined in server config
 * */
struct WorkOrchestratorInfo {
  /** Maximum number of dedicated workers */
  size_t max_dworkers_;
  /** Maximum number of overlapping workers */
  size_t max_oworkers_;
  /** Overlapped workers per core */
  size_t owork_per_core_;
};

/**
 * Queue manager information defined in server config
 * */
struct QueueManagerInfo {
  /** Maximum depth of IPC queues */
  u32 queue_depth_;
  /** Maximum depth of process queue */
  u32 proc_queue_depth_;
  /** Maximum number of lanes per IPC queue */
  u32 max_lanes_;
  /** Maximum number of allocatable IPC queues */
  u32 max_queues_;
  /** Shared memory allocator */
  std::string shm_allocator_;
  /** Shared memory region name */
  std::string shm_name_;
  /** Shared memory region size */
  size_t shm_size_;
  /** Shared memory data region name */
  std::string data_shm_name_;
  /** Shared memory runtime data region name */
  std::string rdata_shm_name_;
  /** Client data shared memory region size */
  size_t data_shm_size_;
  /** Runtime data shared memory region size */
  size_t rdata_shm_size_;
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
  /** Number of RPC threads */
  int num_threads_;
};

/**
 * System configuration for Hermes
 */
class ServerConfig : public BaseConfig {
 public:
  /** Work orchestrator info */
  WorkOrchestratorInfo wo_;
  /** Queue manager info */
  QueueManagerInfo queue_manager_;
  /** The RPC information */
  RpcInfo rpc_;
  /** Bootstrap task registry */
  std::vector<std::string> task_libs_;

 public:
  ServerConfig() = default;
  void LoadDefault();

 private:
  void ParseYAML(YAML::Node &yaml_conf);
  void ParseWorkOrchestrator(YAML::Node yaml_conf);
  void ParseQueueManager(YAML::Node yaml_conf);
  void ParseRpcInfo(YAML::Node yaml_conf);
};

}  // namespace hrun::config

namespace hrun {
using ServerConfig = config::ServerConfig;
}  // namespace hrun

#endif  // HRUN_SRC_CONFIG_SERVER_H_
