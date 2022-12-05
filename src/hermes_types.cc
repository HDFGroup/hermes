//
// Created by lukemartinlogan on 12/5/22.
//

#include "hermes_types.h"
#include "hermes.h"

namespace hermes::api {

Context::Context()
    : policy(HERMES->server_config_.dpe_.default_policy_),
      rr_split(HERMES->server_config_.dpe_.default_rr_split_),
      rr_retry(false),
      disable_swap(false),
      vbkt_id_({0, 0}) {}

}  // namespace hermes::api