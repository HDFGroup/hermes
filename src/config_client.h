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

#ifndef HERMES_SRC_CONFIG_CLIENT_H_
#define HERMES_SRC_CONFIG_CLIENT_H_

#include "config.h"

namespace hermes::config {

/**
 * Configuration used to intialize client
 * */
class ClientConfig : public BaseConfig {
 public:
  bool stop_daemon_;
  size_t file_page_size_;
  std::vector<std::string> path_inclusions_;
  std::vector<std::string> path_exclusions_;

 public:
  ClientConfig() = default;
  void LoadDefault() override;

 private:
  void ParseYAML(YAML::Node &yaml_conf) override;
};

}  // namespace hermes::config

namespace hermes {
using config::ClientConfig;
}  // namespace hermes

#endif  // HERMES_SRC_CONFIG_CLIENT_H_
