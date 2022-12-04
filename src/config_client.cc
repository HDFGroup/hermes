//
// Created by lukemartinlogan on 12/2/22.
//

#include "config.h"
#include "config_client_default.h"

namespace hermes {

/** parse the YAML node */
void ClientConfig::ParseYAML(YAML::Node &yaml_conf) {
  if (yaml_conf["stop_daemon"]) {
    stop_daemon_ = yaml_conf["stop_daemon"].as<bool>();
  }
}

/** Load the default configuration */
void ClientConfig::LoadDefault() {
  LoadText(kClientDefaultConfigStr, false);
}

}  // namespace hermes