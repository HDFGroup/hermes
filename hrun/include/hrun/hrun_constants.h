//
// Created by lukemartinlogan on 6/22/23.
//

#ifndef HRUN_INCLUDE_HRUN_HRUN_CONSTANTS_H_
#define HRUN_INCLUDE_HRUN_HRUN_CONSTANTS_H_

namespace hrun {

#include <string>

class Constants {
 public:
  inline static const std::string kClientConfEnv = "HERMES_CLIENT_CONF";
  inline static const std::string kServerConfEnv = "HERMES_CONF";

  static std::string GetEnvSafe(const std::string &env_name) {
    char *data = getenv(env_name.c_str());
    if (data == nullptr) {
      return "";
    } else {
      return data;
    }
  }
};

}  // namespace hrun

#endif  // HRUN_INCLUDE_HRUN_HRUN_CONSTANTS_H_
