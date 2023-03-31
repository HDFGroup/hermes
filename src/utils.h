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

#ifndef HERMES_UTILS_H_
#define HERMES_UTILS_H_

#include <assert.h>
#include <map>
#include "logging.h"

namespace hermes {

/** Get an environment variable with null safety. */
static inline std::string GetEnvSafe(const char *env_name) {
  char *val = getenv(env_name);
  if (val == nullptr) {
    return "";
  }
  return val;
}

void FailedLibraryCall(std::string func);

}  // namespace hermes

#endif  // HERMES_UTILS_H_
