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

struct BlobInfoShmHeader {
  BucketID bkt_id_;  /**< The bucket containing the blob */
  lipc::ShmArchive<lipc::mptr<lipc::string>> name_ar_;
  lipc::ShmArchive<lipc::mptr<lipc::vector<BufferInfo>>> buffers_ar_;
  RwLock rwlock_;

  BlobInfoShmHeader() = default;
};

struct BlobInfo {
  BucketID bkt_id_;  /**< The bucket containing the blob */
  lipc::mptr<lipc::string> name_;
  lipc::mptr<lipc::vector<BufferInfo>> buffers_;

  BlobInfo() = default;

  BlobInfo(BlobInfoShmHeader &ar) {
    shm_deserialize(ar);
  }

  void shm_serialize(BlobInfoShmHeader &ar) {
    name_ >> ar.name_ar_;
    buffers_ >> ar.buffers_ar_;
  }

  void shm_deserialize(BlobInfoShmHeader &ar) {
    name_ << ar.name_ar_;
    buffers_ << ar.buffers_ar_;
  }
};

struct BucketInfoShmHeader {
  lipc::ShmArchive<lipc::mptr<lipc::string>> name_ar_;
};

struct BucketInfo {
  lipc::mptr<lipc::string> name_;
};

struct VBucketInfoShmHeader {
  lipc::ShmArchive<lipc::mptr<lipc::vector<char>>> name_;
  lipc::ShmArchive<lipc::mptr<lipc::unordered_map<BlobID, BlobID>>> blobs_;
};

struct VBucketInfo {
  lipc::mptr<lipc::vector<char>> name_;
  lipc::mptr<lipc::unordered_map<BlobID, BlobID>> blobs_;
};

}  // namespace hermes

#endif  // HERMES_SRC_METADATA_TYPES_H_
