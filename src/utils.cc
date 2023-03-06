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

#include "utils.h"

#include <iostream>
#include <vector>
#include <random>
#include <utility>

namespace hermes {

/**
   print an error message for \a func function that failed
 */   
void FailedLibraryCall(std::string func) {
  int saved_errno = errno;
  LOG(FATAL) << func << " failed with error: "  << strerror(saved_errno)
             << std::endl;
}

}  // namespace hermes
