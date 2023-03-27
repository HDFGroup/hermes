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

#ifndef HERMES_SRC_CONSTANTS_H_
#define HERMES_SRC_CONSTANTS_H_

#include <cstdlib>

namespace hermes {

static const hipc::allocator_id_t main_alloc_id(0, 1);

static inline const char* kHermesServerConf = "HERMES_CONF";
static inline const char* kHermesClientConf = "HERMES_CLIENT_CONF";
static inline const char* kHermesAdapterMode = "HERMES_ADAPTER_MODE";
static inline const char* kHermesPageSize = "HERMES_PAGE_SIZE";
static inline const char* kHermesStopDaemon = "HERMES_STOP_DAEMON";
static inline const size_t kMaxPathLength = 4096;

static inline const int
    kMaxServerNamePrefix = 32; /**< max. server name prefix */
static inline const int
    kMaxServerNamePostfix = 8; /**< max. server name suffix */
static inline const char
    kBoPrefix[] = "BO::";     /**< buffer organizer prefix */

/** buffer organizer prefix length */
static inline const int kBoPrefixLength = sizeof(kBoPrefix) - 1;

}  // namespace hermes

#endif  // HERMES_SRC_CONSTANTS_H_
