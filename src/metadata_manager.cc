//
// Created by lukemartinlogan on 12/4/22.
//

#include "hermes.h"
#include "metadata_manager.h"

namespace hermes {

void MetadataManager::Init() {
  rpc_ = &HERMES->rpc_;
}

BucketID MetadataManager::LocalGetOrCreateBucket(std::string name) {
  std::cout << "In bucket!" << std::endl;
}

}  // namespace hermes