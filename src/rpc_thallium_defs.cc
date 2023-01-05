//
// Created by lukemartinlogan on 12/3/22.
//

#include "rpc_thallium.h"
#include "metadata_manager.h"
#include "rpc_thallium_serialization.h"
#include "data_structures.h"

namespace hermes {

using thallium::request;

void ThalliumRpc::DefineRpcs() {
  RPC_CLASS_INSTANCE_DEFS_START
  MetadataManager *mdm = this->mdm_;
  RPC_CLASS_INSTANCE_DEFS_END

  RPC_AUTOGEN_START
  RPC_AUTOGEN_END
}

}  // namespace hermes