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

#ifndef HRUN_SRC_CONFIG_H_
#define HRUN_SRC_CONFIG_H_

#include <string.h>
#include <yaml-cpp/yaml.h>
#include <iomanip>
#include <ostream>
#include <vector>
#include <sstream>
#include <limits>
#include "hermes_shm/util/config_parse.h"

namespace hrun::config {

/**
 * Base class for configuration files
 * */
class BaseConfig {
 public:
  /** load configuration from a string */
  void LoadText(const std::string &config_string, bool with_default = true) {
    if (with_default) {
      LoadDefault();
    }
    if (config_string.size() == 0) {
      return;
    }
    YAML::Node yaml_conf = YAML::Load(config_string);
    ParseYAML(yaml_conf);
  }

  /** load configuration from file */
  void LoadFromFile(const std::string &path, bool with_default = true) {
    if (with_default) {
      LoadDefault();
    }
    if (path.size() == 0) {
      return;
    }
    auto real_path = hshm::ConfigParse::ExpandPath(path);
    HILOG(kDebug, "Start load config {}", real_path)
    try {
      YAML::Node yaml_conf = YAML::LoadFile(real_path);
      HILOG(kDebug, "Complete load config {}", real_path)
      ParseYAML(yaml_conf);
    } catch (std::exception &e) {
      HELOG(kFatal, e.what())
    }
  }

  /** load the default configuration */
  virtual void LoadDefault() = 0;

 public:
  /** parse \a list_node vector from configuration file in YAML */
  template<typename T, typename VEC_TYPE = std::vector<T>>
  static void ParseVector(YAML::Node list_node, VEC_TYPE &list) {
    for (auto val_node : list_node) {
      list.emplace_back(val_node.as<T>());
    }
  }

 private:
  virtual void ParseYAML(YAML::Node &yaml_conf) = 0;
};

}  // namespace hrun::config

#endif  // HRUN_SRC_CONFIG_H_
