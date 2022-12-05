//
// Created by lukemartinlogan on 12/4/22.
//

#include "hermes.h"
#include "metadata_manager.h"

namespace hermes {

void MetadataManager::Init() {
  rpc_ = &HERMES->rpc_;
}

bool MetadataManager::LocalGetOrCreateBucket(std::string name) {
  std::cout << "In bucket!" << std::endl;
  return true;
}

bool MetadataManager::GetOrCreateBucket(std::string name) {
  return rpc_->Call<bool>(
      rpc_->node_id_, "GetOrCreateBucket", name);
}

}  // namespace hermes