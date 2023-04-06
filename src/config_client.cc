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

#include "config_client.h"
#include "config_client_default.h"
#include "hermes_shm/util/config_parse.h"
#include <filesystem>

namespace stdfs = std::filesystem;

namespace hermes::config {

/** parse the AdapterConfig YAML node */
void ClientConfig::ParseAdapterConfig(YAML::Node &yaml_conf,
                                      AdapterObjectConfig &conf) {
  std::string path = yaml_conf["path"].as<std::string>();
  path = hshm::ConfigParse::ExpandPath(path);
  path = stdfs::absolute(path).string();
  if (yaml_conf["mode"]) {
    conf.mode_ = AdapterModeConv::to_enum(yaml_conf["mode"].as<std::string>());
  }
  if (yaml_conf["page_size"]) {
    conf.page_size_ = hshm::ConfigParse::ParseSize(yaml_conf["page_size"].as<std::string>());
  }
  SetAdapterConfig(path, conf);
}

/** parse the YAML node */
void ClientConfig::ParseYAML(YAML::Node &yaml_conf) {
  if (yaml_conf["stop_daemon"]) {
    std::string stop_env = GetEnvSafe(kHermesStopDaemon);
    if (stop_env.size() == 0) {
      stop_daemon_ = yaml_conf["stop_daemon"].as<bool>();
    } else {
      std::istringstream(stop_env) >> stop_daemon_;
    }
  }
  if (yaml_conf["base_adapter_mode"]) {
    std::string mode_env = GetEnvSafe(kHermesAdapterMode);
    if (mode_env.size() == 0) {
      base_adapter_config_.mode_ = AdapterModeConv::to_enum(
          yaml_conf["base_adapter_mode"].as<std::string>());
    } else {
      base_adapter_config_.mode_ = AdapterModeConv::to_enum(mode_env);
    }
  }
  if (yaml_conf["file_page_size"]) {
    std::string page_size_env = GetEnvSafe(kHermesPageSize);
    if (page_size_env.size() == 0) {
      base_adapter_config_.page_size_ =
          hshm::ConfigParse::ParseSize(yaml_conf["file_page_size"].as<std::string>());
    } else {
      base_adapter_config_.page_size_ = hshm::ConfigParse::ParseSize(page_size_env);
    }
  }
  if (yaml_conf["path_inclusions"]) {
    std::vector<std::string> inclusions;
    ParseVector<std::string>(yaml_conf["path_inclusions"], inclusions);
    for (auto &entry : inclusions) {
      entry = hshm::ConfigParse::ExpandPath(entry);
      SetAdapterPathTracking(std::move(entry), true);
    }
  }
  if (yaml_conf["path_exclusions"]) {
    std::vector<std::string> exclusions;
    ParseVector<std::string>(yaml_conf["path_exclusions"], exclusions);
    for (auto &entry : exclusions) {
      entry = hshm::ConfigParse::ExpandPath(entry);
      SetAdapterPathTracking(std::move(entry), false);
    }
  }
  if (yaml_conf["flushing_mode"]) {
    flushing_mode_ =
        FlushingModeConv::GetEnum(yaml_conf["flushing_mode"].as<std::string>());
    auto flush_mode_env = getenv("HERMES_FLUSH_MODE");
    if (flush_mode_env) {
      flushing_mode_ = FlushingModeConv::GetEnum(flush_mode_env);
    }
  }
  if (yaml_conf["file_adapter_configs"]) {
    for (auto node : yaml_conf["file_adapter_configs"]) {
      AdapterObjectConfig conf(base_adapter_config_);
      ParseAdapterConfig(node, conf);
    }
  }
}

/** Load the default configuration */
void ClientConfig::LoadDefault() {
  LoadText(kClientDefaultConfigStr, false);
}

}  // namespace hermes::config
