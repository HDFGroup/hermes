//
// Created by lukemartinlogan on 12/4/22.
//

#ifndef HERMES_SRC_METADATA_MANAGER_H_
#define HERMES_SRC_METADATA_MANAGER_H_

#include "rpc_decorator.h"
#include "hermes_types.h"
#include "hermes_status.h"
#include "rpc.h"

namespace hermes {

class MetadataManager {
 private:
  RPC_TYPE *rpc_;

 public:
  MetadataManager() = default;
  void Init();

  RPC bool LocalGetOrCreateBucket(std::string bucket_name);

 public:
  RPC_AUTOGEN_START
  bool GetOrCreateBucket(std::string bucket_name);
  RPC_AUTOGEN_END
};

}  // namespace hermes

#endif  // HERMES_SRC_METADATA_MANAGER_H_
