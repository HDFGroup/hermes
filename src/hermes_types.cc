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

namespace hermes::api {

////////////////////////////
/// Context Operations
////////////////////////////

/** Default constructor */
Context::Context()
    : policy(HERMES->server_config_.dpe_.default_policy_),
      rr_split(HERMES->server_config_.dpe_.default_rr_split_),
      rr_retry(false),
      disable_swap(false),
      vbkt_id_({0, 0}) {}

}  // namespace hermes::api
