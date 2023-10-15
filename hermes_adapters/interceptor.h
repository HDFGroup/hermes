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

#ifndef HERMES_ADAPTER_UTILS_H_
#define HERMES_ADAPTER_UTILS_H_

#include "hermes/hermes.h"
#include "hermes_adapters/filesystem/filesystem_mdm.h"

namespace stdfs = std::filesystem;

namespace hermes::adapter {

#define HERMES_DECL(F) F

/** The maximum length of a POSIX path */
static inline const int kMaxPathLen = 4096;

}  // namespace hermes::adapter

#endif  // HERMES_ADAPTER_UTILS_H_
