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
