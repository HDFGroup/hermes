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
