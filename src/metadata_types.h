//
// Created by lukemartinlogan on 12/7/22.
//

#ifndef HERMES_SRC_METADATA_TYPES_H_
#define HERMES_SRC_METADATA_TYPES_H_

#include "hermes_types.h"

namespace hermes {

using api::Blob;

/**  */
struct DeviceInfo {

};

/** Represents the current status of a target */
struct TargetInfo {
  TargetID tid_;    /**< unique Target ID */
  size_t max_cap_;  /**< maximum capacity of the target */
  size_t rem_cap_;  /**< remaining capacity of the target */
};

/** Represents an allocated fraction of a target */
struct BufferInfo {
  size_t off_;
  size_t size_;
  TargetID target_;
};

/** Represents BlobInfo in shared memory */
struct BlobInfoShmHeader {
  BucketId bkt_id_;  /**< The bucket containing the blob */
  lipc::ShmArchive<lipc::mptr<lipc::string>> name_ar_;
  lipc::ShmArchive<lipc::mptr<lipc::vector<BufferInfo>>> buffers_ar_;
  RwLock rwlock_;

  BlobInfoShmHeader() = default;
};

/** Blob metadata */
struct BlobInfo {
  /// The bucket containing the blob
  BucketId bkt_id_;
  /// The name of the blob
  lipc::mptr<lipc::string> name_;
  /// The BufferInfo vector
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

/** Represents BucketInfo in shared memory */
struct BucketInfoShmHeader {
  lipc::ShmArchive<lipc::mptr<lipc::string>> name_ar_;
};

/** Metadata for a Bucket */
struct BucketInfo {
  lipc::mptr<lipc::string> name_;
};

/** Represents a VBucket in shared memory */
struct VBucketInfoShmHeader {
  lipc::ShmArchive<lipc::mptr<lipc::vector<char>>> name_;
  lipc::ShmArchive<lipc::mptr<lipc::unordered_map<BlobId, BlobId>>> blobs_;
};

/** Metadata for a VBucket */
struct VBucketInfo {
  lipc::mptr<lipc::vector<char>> name_;
  lipc::mptr<lipc::unordered_map<BlobId, BlobId>> blobs_;
};

}  // namespace hermes

#endif  // HERMES_SRC_METADATA_TYPES_H_
