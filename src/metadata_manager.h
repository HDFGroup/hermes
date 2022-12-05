//
// Created by lukemartinlogan on 12/4/22.
//

#ifndef HERMES_SRC_METADATA_MANAGER_H_
#define HERMES_SRC_METADATA_MANAGER_H_

#include "rpc_decorator.h"
#include "hermes_types.h"
#include "hermes_status.h"

namespace hermes {

class MetadataManager {
 public:
  MetadataManager() = default;

  RPC void LocalGetOrCreateBucket(std::string bucket_name) {
    std::cout << bucket_name << std::endl;
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_METADATA_MANAGER_H_
