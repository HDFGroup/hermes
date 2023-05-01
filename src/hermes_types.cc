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

#include "hermes_types.h"
#include "hermes.h"

namespace hermes {
/** Identifier of the Hermes allocator */
const hipc::allocator_id_t main_alloc_id(0, 1);

/** Hermes server environment variable */
const char* kHermesServerConf = "HERMES_CONF";

/** Hermes client environment variable */
const char* kHermesClientConf = "HERMES_CLIENT_CONF";

/** Hermes adapter mode environment variable */
const char* kHermesAdapterMode = "HERMES_ADAPTER_MODE";

/** Filesystem page size environment variable */
const char* kHermesPageSize = "HERMES_PAGE_SIZE";

/** Stop daemon environment variable */
const char* kHermesStopDaemon = "HERMES_STOP_DAEMON";

/** Maximum path length environment variable */
const size_t kMaxPathLength = 4096;
}  // namespace hermes

namespace hermes::api {

/** Default constructor */
Context::Context()
    : policy(HERMES->server_config_.dpe_.default_policy_),
      rr_split(HERMES->server_config_.dpe_.default_rr_split_),
      rr_retry(false),
      disable_swap(false),
      blob_score_(-1) {}

}  // namespace hermes::api
