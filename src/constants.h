//
// Created by lukemartinlogan on 12/2/22.
//

#ifndef HERMES_SRC_CONSTANTS_H_
#define HERMES_SRC_CONSTANTS_H_

#include <cstdlib>

namespace hermes {

extern const char* kHermesServerConf;
extern const char* kHermesClientConf;
extern const size_t kMaxPathLength;

const int kMaxServerNamePrefix = 32; /**< max. server name prefix */
const int kMaxServerNamePostfix = 8; /**< max. server name suffix */
const char kBoPrefix[] = "BO::";     /**< buffer organizer prefix */

/** buffer organizer prefix length */
const int kBoPrefixLength = sizeof(kBoPrefix) - 1;

}

#endif  // HERMES_SRC_CONSTANTS_H_
