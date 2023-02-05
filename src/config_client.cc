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
#include <filesystem>

namespace stdfs = std::filesystem;

namespace hermes::config {

/** parse the AdapterConfig YAML node */
void ClientConfig::ParseAdapterConfig(YAML::Node &yaml_conf,
                                      AdapterObjectConfig &conf) {
  std::string path = yaml_conf["path"].as<std::string>();
  path = stdfs::weakly_canonical(path).string();
  if (yaml_conf["mode"]) {
    conf.mode_ = AdapterModeConv::to_enum(yaml_conf["mode"].as<std::string>());
  }
  if (yaml_conf["page_size"]) {
    conf.page_size_ = ParseSize(
        yaml_conf["page_size"].as<std::string>());
  }
  SetAdapterConfig(path, conf);
}

/** parse the YAML node */
void ClientConfig::ParseYAML(YAML::Node &yaml_conf) {
  if (yaml_conf["stop_daemon"]) {
    stop_daemon_ = yaml_conf["stop_daemon"].as<bool>();
  }
  if (yaml_conf["base_adapter_mode"]) {
    base_adapter_config_.mode_ = AdapterModeConv::to_enum(
        yaml_conf["base_adapter_mode"].as<std::string>());
  }
  if (yaml_conf["file_page_size"]) {
    base_adapter_config_.page_size_ = ParseSize(
        yaml_conf["file_page_size"].as<std::string>());
  }
  if (yaml_conf["path_inclusions"]) {
    std::vector<std::string> inclusions;
    ParseVector<std::string>(yaml_conf["path_inclusions"], inclusions);
    for (auto &entry : inclusions) {
      SetAdapterPathTracking(std::move(entry), true);
    }
  }
  if (yaml_conf["path_exclusions"]) {
    std::vector<std::string> exclusions;
    ParseVector<std::string>(yaml_conf["path_inclusions"], exclusions);
    for (auto &entry : exclusions) {
      SetAdapterPathTracking(std::move(entry), false);
    }
  }
  if (yaml_conf["file_adapter_configs"]) {
    for (auto node : yaml_conf["file_adapter_modes"]) {
      AdapterObjectConfig conf(base_adapter_config_);
      ParseAdapterConfig(node.second, conf);
    }
  }
}

/** Load the default configuration */
void ClientConfig::LoadDefault() {
  LoadText(kClientDefaultConfigStr, false);
}

}  // namespace hermes::config
