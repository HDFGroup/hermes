//
// Created by lukemartinlogan on 12/7/22.
//

#ifndef HERMES_SRC_METADATA_TYPES_H_
#define HERMES_SRC_METADATA_TYPES_H_

#include "hermes_types.h"
#include "config_server.h"

namespace hermes {

using api::Blob;

/** Device information required by other processes */
using config::DeviceInfo;
using config::IoInterface;

/** Represents the current status of a target */
struct TargetInfo {
  TargetId id_;         /**< unique Target ID */
  size_t max_cap_;      /**< maximum capacity of the target */
  size_t rem_cap_;      /**< remaining capacity of the target */
  double bandwidth_;    /**< the bandwidth of the device */
  double latency_;      /**< the latency of the device */

  TargetInfo() = default;

  TargetInfo(TargetId id, size_t max_cap, size_t rem_cap,
             double bandwidth, double latency)
      : id_(id), max_cap_(max_cap), rem_cap_(rem_cap),
        bandwidth_(bandwidth), latency_(latency) {}
};

/** Represents an allocated fraction of a target */
struct BufferInfo {
  TargetId tid_;        /**< The destination target */
  size_t t_off_;        /**< Offset in the target */
  size_t t_size_;       /**< Size in the target */
  size_t blob_off_;     /**< Offset in the blob */
  size_t blob_size_;    /**< The amount of the blob being placed */

  BufferInfo() = default;

  BufferInfo(TargetId tid, size_t t_off, size_t t_size,
             size_t blob_off, size_t blob_size)
      : tid_(tid), t_off_(t_off), t_size_(t_size),
        blob_off_(blob_off), blob_size_(blob_size){}

  /** Copy constructor */
  BufferInfo(const BufferInfo &other) {
    Copy(other);
  }

  /** Move constructor */
  BufferInfo(BufferInfo &&other) {
    Copy(other);
  }

  /** Copy assignment */
  BufferInfo& operator=(const BufferInfo &other) {
    Copy(other);
  }

  /** Move assignment */
  BufferInfo& operator=(BufferInfo &&other) {
    Copy(other);
  }

  /** Performs move/copy */
  void Copy(const BufferInfo &other) {
    tid_ = other.tid_;
    t_off_ = other.t_off_;
    t_size_ = other.t_size_;
    blob_off_ = other.blob_off_;
    blob_size_ = other.blob_size_;
  }
};

/** Represents BlobInfo in shared memory */
struct BlobInfoShmHeader {
  BucketId bkt_id_;  /**< The bucket containing the blob */
  lipc::ShmArchive<lipc::string> name_ar_;
  lipc::ShmArchive<lipc::vector<BufferInfo>> buffers_ar_;
  RwLock rwlock_;

  BlobInfoShmHeader() = default;

  BlobInfoShmHeader(const BlobInfoShmHeader &other) noexcept
      : bkt_id_(other.bkt_id_), name_ar_(other.name_ar_),
        buffers_ar_(other.buffers_ar_),
        rwlock_() {}

  BlobInfoShmHeader(BlobInfoShmHeader &&other) noexcept
  : bkt_id_(std::move(other.bkt_id_)), name_ar_(std::move(other.name_ar_)),
    buffers_ar_(std::move(other.buffers_ar_)),
    rwlock_() {}

  BlobInfoShmHeader& operator=(BlobInfoShmHeader &&other) {
    if (this != &other) {
      bkt_id_ = std::move(other.bkt_id_);
      name_ar_ = std::move(other.name_ar_);
      buffers_ar_ = std::move(other.buffers_ar_);
      rwlock_ = std::move(other.rwlock_);
    }
    return *this;
  }
};

/** Blob metadata */
struct BlobInfo {
 public:
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
    ar.bkt_id_ = bkt_id_;
    name_ >> ar.name_ar_;
    buffers_ >> ar.buffers_ar_;
  }

  void shm_deserialize(BlobInfoShmHeader &ar) {
    bkt_id_ = ar.bkt_id_;
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
