//
// Created by lukemartinlogan on 12/7/22.
//

#ifndef HERMES_SRC_METADATA_TYPES_H_
#define HERMES_SRC_METADATA_TYPES_H_

#include "hermes_types.h"
#include "config_server.h"

namespace hermes {

using api::Blob;
struct BucketInfo;     /** Forward declaration of BucketInfo */
struct BlobInfo;       /** Forward declaration of BlobInfo */
struct VBucketInfo;    /** Forward declaration of VBucketInfo */

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
    return *this;
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
template<>
struct ShmHeader<BlobInfo> : public lipc::ShmBaseHeader {
  BucketId bkt_id_;  /**< The bucket containing the blob */
  lipc::ShmArchive<lipc::string> name_ar_;
  lipc::ShmArchive<lipc::vector<BufferInfo>> buffers_ar_;
  RwLock rwlock_;

  ShmHeader() = default;

  ShmHeader(const ShmHeader &other) noexcept
      : bkt_id_(other.bkt_id_), name_ar_(other.name_ar_),
        buffers_ar_(other.buffers_ar_),
        rwlock_() {}

  ShmHeader(ShmHeader &&other) noexcept
  : bkt_id_(std::move(other.bkt_id_)), name_ar_(std::move(other.name_ar_)),
    buffers_ar_(std::move(other.buffers_ar_)),
    rwlock_() {}

  ShmHeader& operator=(ShmHeader &&other) {
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
struct BlobInfo : public SHM_CONTAINER(BlobInfo){
 public:
  SHM_CONTAINER_TEMPLATE(BlobInfo, BlobInfo);

 public:
  /// The bucket containing the blob
  BucketId bkt_id_;
  /// The name of the blob
  lipc::mptr<lipc::string> name_;
  /// The BufferInfo vector
  lipc::mptr<lipc::vector<BufferInfo>> buffers_;

  BlobInfo() = default;

  void shm_init_main(lipc::ShmArchive<BlobInfo> *ar,
                     lipc::Allocator *alloc) {
    shm_init_header(ar, alloc);
  }

  void shm_destroy(bool destroy_header = true) {
    SHM_DESTROY_DATA_START
    SHM_DESTROY_DATA_END
    SHM_DESTROY_END
  }

  void shm_serialize(lipc::ShmArchive<BlobInfo> &ar) const {
    shm_serialize_header(ar.header_ptr_);
    header_->bkt_id_ = bkt_id_;
    name_ >> header_->name_ar_;
    buffers_ >> header_->buffers_ar_;
  }

  void shm_deserialize(const lipc::ShmArchive<BlobInfo> &ar) {
    shm_deserialize_header(ar.header_ptr_);
    bkt_id_ = header_->bkt_id_;
    name_ << header_->name_ar_;
    buffers_ << header_->buffers_ar_;
  }

  void WeakMove(BlobInfo &other) {
    throw NOT_IMPLEMENTED;
  }

  void StrongCopy(const BlobInfo &other) {
    throw NOT_IMPLEMENTED;
  }
};


/** Represents BucketInfo in shared memory */
template<>
struct ShmHeader<BucketInfo> : public lipc::ShmBaseHeader {
  lipc::ShmArchive<lipc::string> name_ar_;
};

/** Metadata for a Bucket */
struct BucketInfo : public SHM_CONTAINER(BucketInfo) {
 public:
  SHM_CONTAINER_TEMPLATE(BucketInfo, BucketInfo);

 public:
  lipc::mptr<lipc::string> name_;

 public:
  BucketInfo() = default;

  void shm_init_main(lipc::ShmArchive<BucketInfo> *ar,
                     lipc::Allocator *alloc) {
    shm_init_header(ar, alloc);
  }

  void shm_destroy(bool destroy_header = true) {
    SHM_DESTROY_DATA_START
    SHM_DESTROY_DATA_END
    SHM_DESTROY_END
  }

  void shm_serialize(lipc::ShmArchive<BucketInfo> &ar) const {
    shm_serialize_header(ar.header_ptr_);
  }

  void shm_deserialize(const lipc::ShmArchive<BucketInfo> &ar) {
    shm_deserialize_header(ar.header_ptr_);
  }

  void WeakMove(BucketInfo &other) {
    throw NOT_IMPLEMENTED;
  }

  void StrongCopy(const BucketInfo &other) {
    throw NOT_IMPLEMENTED;
  }
};

/** Represents a VBucket in shared memory */
template<>
struct ShmHeader<VBucketInfo> : public lipc::ShmBaseHeader {
  lipc::ShmArchive<lipc::vector<char>> name_;
  lipc::ShmArchive<lipc::unordered_map<BlobId, BlobId>> blobs_;
};

/** Metadata for a VBucket */
struct VBucketInfo : public SHM_CONTAINER(VBucketInfo) {
 public:
  SHM_CONTAINER_TEMPLATE(VBucketInfo, VBucketInfo);

 public:
  lipc::mptr<lipc::vector<char>> name_;
  lipc::mptr<lipc::unordered_map<BlobId, BlobId>> blobs_;

 public:
  void shm_init_main(lipc::ShmArchive<VBucketInfo> *ar,
                     lipc::Allocator *alloc) {
    shm_init_header(ar, alloc);
  }

  void shm_destroy(bool destroy_header = true) {
    SHM_DESTROY_DATA_START
    SHM_DESTROY_DATA_END
    SHM_DESTROY_END
  }

  void shm_serialize(lipc::ShmArchive<VBucketInfo> &ar) const {
    shm_serialize_header(ar.header_ptr_);
  }

  void shm_deserialize(const lipc::ShmArchive<VBucketInfo> &ar) {
    shm_deserialize_header(ar.header_ptr_);
  }

  void WeakMove(VBucketInfo &other) {
    throw NOT_IMPLEMENTED;
  }

  void StrongCopy(const VBucketInfo &other) {
    throw NOT_IMPLEMENTED;
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_METADATA_TYPES_H_
