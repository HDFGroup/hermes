//
// Created by lukemartinlogan on 12/19/22.
//

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

 public:
  ClientConfig() = default;
  void LoadDefault() override;

 private:
  void ParseYAML(YAML::Node &yaml_conf) override;
};

}  // hermes::config

namespace hermes {
using config::ClientConfig;
}  // namespace hermes

#endif  // HERMES_SRC_CONFIG_CLIENT_H_
