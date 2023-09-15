//
// Created by lukemartinlogan on 6/22/23.
//

#ifndef LABSTOR_INCLUDE_LABSTOR_LABSTOR_CONSTANTS_H_
#define LABSTOR_INCLUDE_LABSTOR_LABSTOR_CONSTANTS_H_

namespace labstor {

#include <string>

class Constants {
 public:
  inline static const std::string kClientConfEnv = "LABSTOR_CLIENT_CONF";
  inline static const std::string kServerConfEnv = "LABSTOR_SERVER_CONF";

  static std::string GetEnvSafe(const std::string &env_name) {
    char *data = getenv(env_name.c_str());
    if (data == nullptr) {
      return "";
    } else {
      return data;
    }
  }
};

}  // namespace labstor

#endif  // LABSTOR_INCLUDE_LABSTOR_LABSTOR_CONSTANTS_H_
