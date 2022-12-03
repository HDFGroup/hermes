//
// Created by lukemartinlogan on 12/2/22.
//

#include "config.h"
#include "config_client_default.h"

namespace hermes {

/** parse the YAML node */
void ClientConfig::ParseYAML(YAML::Node &yaml_conf) {
}

/** Load the default configuration */
void ClientConfig::LoadDefault() {
  LoadText(kClientDefaultConfigStr, false);
}

}  // namespace hermes