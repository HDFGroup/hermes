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

#ifndef HRUN_SRC_CONFIG_CLIENT_H_
#define HRUN_SRC_CONFIG_CLIENT_H_

#include <filesystem>
#include "config.h"

namespace stdfs = std::filesystem;

namespace hrun::config {

/**
 * Configuration used to intialize client
 * */
class ClientConfig : public BaseConfig {
 public:
  /** The thread model of the application */
  std::string thread_model_;

 private:
  void ParseYAML(YAML::Node &yaml_conf) override;
  void LoadDefault() override;
};

}  // namespace hrun::config

namespace hrun {
using ClientConfig = config::ClientConfig;
}  // namespace hrun

#endif  // HRUN_SRC_CONFIG_CLIENT_H_
