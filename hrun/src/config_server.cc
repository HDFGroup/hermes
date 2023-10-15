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
#include "hermes_shm/util/config_parse.h"
#include "hrun/config/config.h"
#include "hrun/config/config_server.h"
#include "hrun/config/config_server_default.h"

namespace hrun::config {

/** parse work orchestrator info from YAML config */
void ServerConfig::ParseWorkOrchestrator(YAML::Node yaml_conf) {
  if (yaml_conf["max_workers"]) {
    wo_.max_workers_ = yaml_conf["max_workers"].as<size_t>();
  }
}

/** parse work orchestrator info from YAML config */
void ServerConfig::ParseQueueManager(YAML::Node yaml_conf) {
  if (yaml_conf["queue_depth"]) {
    queue_manager_.queue_depth_ = yaml_conf["queue_depth"].as<size_t>();
  }
  if (yaml_conf["max_lanes"]) {
    queue_manager_.max_lanes_ = yaml_conf["max_lanes"].as<size_t>();
  }
  if (yaml_conf["max_queues"]) {
    queue_manager_.max_queues_ = yaml_conf["max_queues"].as<size_t>();
  }
  if (yaml_conf["shm_allocator"]) {
    queue_manager_.shm_allocator_ = yaml_conf["shm_allocator"].as<std::string>();
  }
  if (yaml_conf["shm_name"]) {
    queue_manager_.shm_name_ = yaml_conf["shm_name"].as<std::string>();
  }
  if (yaml_conf["shm_size"]) {
    queue_manager_.shm_size_ = hshm::ConfigParse::ParseSize(
        yaml_conf["shm_size"].as<std::string>());
  }
}

/** parse RPC information from YAML config */
void ServerConfig::ParseRpcInfo(YAML::Node yaml_conf) {
  std::string suffix;
  rpc_.host_names_.clear();
  if (yaml_conf["host_file"]) {
    rpc_.host_file_ =
        hshm::ConfigParse::ExpandPath(yaml_conf["host_file"].as<std::string>());
    if (rpc_.host_file_.size() > 0) {
      rpc_.host_names_ = hshm::ConfigParse::ParseHostfile(rpc_.host_file_);
    }
  }
  if (yaml_conf["host_names"] && rpc_.host_names_.size() == 0) {
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

/** parse the YAML node */
void ServerConfig::ParseYAML(YAML::Node &yaml_conf) {
  if (yaml_conf["work_orchestrator"]) {
    ParseWorkOrchestrator(yaml_conf["work_orchestrator"]);
  }
  if (yaml_conf["queue_manager"]) {
    ParseQueueManager(yaml_conf["queue_manager"]);
  }
  if (yaml_conf["rpc"]) {
    ParseRpcInfo(yaml_conf["rpc"]);
  }
  if (yaml_conf["task_registry"]) {
    ParseVector<std::string>(yaml_conf["task_registry"], task_libs_);
  }
}

/** Load the default configuration */
void ServerConfig::LoadDefault() {
  LoadText(kHrunServerDefaultConfigStr, false);
}

}  // namespace hrun::config
