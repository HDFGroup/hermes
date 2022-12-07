//
// Created by lukemartinlogan on 12/7/22.
//

#ifndef HERMES_SRC_METADATA_TYPES_H_
#define HERMES_SRC_METADATA_TYPES_H_

#include "hermes_types.h"

namespace hermes {

using api::Blob;

struct BufferInfo {
  size_t off_;
  size_t size_;
  TargetID target_;
};

struct BlobInfo {
  std::string name_;
  std::vector<BufferInfo> buffers_;
  RwLock rwlock_;
};

struct BucketInfo {
  std::string name_;
  std::vector<BlobID> blobs_;
};

struct VBucketInfo {
  std::vector<char> name_;
  std::unordered_set<BlobID> blobs_;
};

}  // namespace hermes

#endif  // HERMES_SRC_METADATA_TYPES_H_
