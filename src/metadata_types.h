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
  BucketID bkt_id_;  /**< The bucket containing the blob */
  lipc::ShmArchive<lipc::uptr<lipc::string>> name_;
  lipc::ShmArchive<lipc::uptr<lipc::vector<BufferInfo>>> buffers_;
  RwLock rwlock_;
};

struct BucketInfo {
  lipc::ShmArchive<lipc::uptr<lipc::string>> name_;
};

struct VBucketInfo {
  lipc::ShmArchive<lipc::uptr<lipc::vector<char>>> name_;
  lipc::ShmArchive<lipc::uptr<lipc::unordered_map<BlobID, BlobID>>> blobs_;
};

}  // namespace hermes

#endif  // HERMES_SRC_METADATA_TYPES_H_
