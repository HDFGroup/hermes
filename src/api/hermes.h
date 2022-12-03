//
// Created by lukemartinlogan on 12/2/22.
//

#ifndef HERMES_SRC_API_HERMES_H_
#define HERMES_SRC_API_HERMES_H_

#include "config.h"
#include "constants.h"

namespace hermes::api {

enum class HermesType {
  kDaemon,
  kClient,
  kColocated
};

class Hermes {
 public:
  HermesType mode_;
  ServerConfig server_config_;
  ClientConfig client_config_;

 public:
  Hermes() = default;
  Hermes(HermesType mode, std::string config_path) {
    Init(mode, std::move(config_path));
  }

  void Init(HermesType mode, std::string config_path) {
    mode_ = mode;
    switch (mode) {
      case HermesType::kDaemon: {
        InitDaemon(std::move(config_path));
        break;
      }
      case HermesType::kClient: {
        InitClient(std::move(config_path));
        break;
      }
      case HermesType::kColocated: {
        InitColocated(std::move(config_path));
        break;
      }
    }
  }

  void Finalize() {
  }

  void RunDaemon() {
  }

 private:
  void InitDaemon(std::string config_path) {
    // Load the Hermes Configuration
    if (config_path.size() == 0) {
      config_path = GetEnvSafe(kHermesServerConf);
    }
    server_config_.LoadFromFile(config_path);
  }

  void InitClient(std::string config_path) {
  }

  void InitColocated(std::string config_path) {
  }

  void FinalizeDaemon() {
  }

  void FinalizeClient() {
  }

  void FinalizeColocated() {
  }

 private:
  inline std::string GetEnvSafe(const char *env_name) {
    char *val = getenv(env_name);
    if (val == nullptr){
      return "";
    }
    return val;
  }
};

}

#endif  // HERMES_SRC_API_HERMES_H_
