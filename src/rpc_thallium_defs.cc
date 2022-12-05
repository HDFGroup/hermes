//
// Created by lukemartinlogan on 12/3/22.
//

#include "rpc_thallium.h"
#include "metadata_manager.h"

namespace hermes {

using thallium::request;

void ThalliumRpc::DefineRpcs() {
  RPC_AUTOGEN_START
  auto get_or_create_bucket =
      [this](const request &req, std::string name) {
        this->mdm_->LocalGetOrCreateBucket(name);
        req.respond(true);
      };
  server_engine_->define("GetOrCreateBucket", get_or_create_bucket);
  RPC_AUTOGEN_END
}

}  // namespace hermes