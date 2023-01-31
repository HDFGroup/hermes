//
// Created by lukemartinlogan on 12/2/22.
//

#include "config_client.h"
#include "config_client_default.h"

namespace hermes::config {

/** parse the YAML node */
void ClientConfig::ParseYAML(YAML::Node &yaml_conf) {
  if (yaml_conf["stop_daemon"]) {
    stop_daemon_ = yaml_conf["stop_daemon"].as<bool>();
  }
  if (yaml_conf["file_page_size"]) {
    file_page_size_ = ParseSize(
        yaml_conf["file_page_size"].as<std::string>());
  }
}

/** Load the default configuration */
void ClientConfig::LoadDefault() {
  LoadText(kClientDefaultConfigStr, false);
}

}  // namespace hermes::config