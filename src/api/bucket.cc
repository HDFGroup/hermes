//
// Created by lukemartinlogan on 12/4/22.
//

#include "bucket.h"

namespace hermes::api {

Bucket::Bucket(std::string name,
               Context &ctx) : mdm_(&HERMES->mdm_) {
  // mdm_->GetOrCreateBucket(name);
}

}  // namespace hermes::api