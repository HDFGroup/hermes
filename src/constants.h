//
// Created by lukemartinlogan on 12/2/22.
//

#ifndef HERMES_SRC_CONSTANTS_H_
#define HERMES_SRC_CONSTANTS_H_

#include <cstdlib>

namespace hermes {

static const lipc::allocator_id_t main_alloc_id(0, 0);

static const char* kHermesServerConf = "HERMES_CONF";
static const char* kHermesClientConf = "HERMES_CLIENT_CONF";
static const size_t kMaxPathLength = 4096;

static const int kMaxServerNamePrefix = 32; /**< max. server name prefix */
static const int kMaxServerNamePostfix = 8; /**< max. server name suffix */
static const char kBoPrefix[] = "BO::";     /**< buffer organizer prefix */

/** buffer organizer prefix length */
static const int kBoPrefixLength = sizeof(kBoPrefix) - 1;

}

#endif  // HERMES_SRC_CONSTANTS_H_
