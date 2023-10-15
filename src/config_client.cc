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

#include "hrun/config/config_client.h"
#include "hrun/config/config_client_default.h"
#include "hermes_shm/util/config_parse.h"
#include <filesystem>

namespace stdfs = std::filesystem;

namespace hrun::config {

/** parse the YAML node */
void ClientConfig::ParseYAML(YAML::Node &yaml_conf) {
  if (yaml_conf["thread_model"]) {
    thread_model_ = yaml_conf["thread_model"].as<std::string>();
  }
}

/** Load the default configuration */
void ClientConfig::LoadDefault() {
  LoadText(kLabstorClientDefaultConfigStr, false);
}

}  // namespace hrun::config
