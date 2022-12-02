//
// Created by lukemartinlogan on 12/2/22.
//

#ifndef HERMES_SRC_API_HERMES_H_
#define HERMES_SRC_API_HERMES_H_

#include "config.h"

namespace hermes::api {

const char* kHermesConf = "HERMES_CONF";
const char* kHermesClientConf = "HERMES_CLIENT_CONF";

enum class HermesType {
  kDaemon,
  kClient,
  kColocated
};

class Hermes {
 public:
  HermesType mode_;
  Config config_;

 public:
  Hermes() = default;

  void Init(HermesType mode, std::string config_path) {
    // Load the Hermes Configuration
    if (config_path.size() == 0) {
      config_path = getenv(kHermesConf);
    }
    InitConfig(&config_, config_path.c_str());

    mode_ = mode;
    switch (mode) {
      case HermesType::kDaemon: {
        InitDaemon();
        break;
      }
      case HermesType::kClient: {
        InitClient();
        break;
      }
      case HermesType::kColocated: {
        InitColocated();
        break;
      }
    }
  }

  void Finalize() {
  }

  void RunDaemon() {
  }

 private:
  void InitDaemon() {
  }

  void InitClient() {
  }

  void InitColocated() {
  }

  void FinalizeDaemon() {
  }

  void FinalizeClient() {
  }

  void FinalizeColocated() {
  }
};

}

#endif  // HERMES_SRC_API_HERMES_H_
