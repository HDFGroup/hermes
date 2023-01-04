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
    return *this;
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

  /** Default constructor */
  ShmHeader() = default;

  /** Copy constructor */
  ShmHeader(const ShmHeader &other) noexcept
      : bkt_id_(other.bkt_id_), name_ar_(other.name_ar_),
        buffers_ar_(other.buffers_ar_),
        rwlock_() {}

  /** Move constructor */
  ShmHeader(ShmHeader &&other) noexcept
  : bkt_id_(std::move(other.bkt_id_)), name_ar_(std::move(other.name_ar_)),
    buffers_ar_(std::move(other.buffers_ar_)),
    rwlock_() {}

  /** Copy assignment */
  ShmHeader& operator=(const ShmHeader &other) {
    if (this != &other) {
      bkt_id_ = other.bkt_id_;
      name_ar_ = other.name_ar_;
      buffers_ar_ = other.buffers_ar_;
    }
    return *this;
  }

  /** Move assignment */
  ShmHeader& operator=(ShmHeader &&other) {
    if (this != &other) {
      bkt_id_ = std::move(other.bkt_id_);
      name_ar_ = std::move(other.name_ar_);
      buffers_ar_ = std::move(other.buffers_ar_);
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

  /** Default constructor. Does nothing. */
  BlobInfo() = default;

  /** Initialize the data structure */
  void shm_init_main(lipc::ShmArchive<BlobInfo> *ar,
                     lipc::Allocator *alloc) {
    shm_init_header(ar, alloc);
    name_.shm_init(alloc);
    buffers_.shm_init(alloc);
    shm_serialize(ar_);
  }

  /** Destroy all allocated data */
  void shm_destroy(bool destroy_header = true) {
    SHM_DESTROY_DATA_START
    name_.shm_destroy();
    buffers_.shm_destroy();
    SHM_DESTROY_DATA_END
    SHM_DESTROY_END
  }

  /** Serialize into \a ShmArchive */
  void shm_serialize(lipc::ShmArchive<BlobInfo> &ar) const {
    shm_serialize_header(ar.header_ptr_);
    header_->bkt_id_ = bkt_id_;
    name_ >> header_->name_ar_;
    buffers_ >> header_->buffers_ar_;
  }

  /** Deserialize from \a ShmArchive */
  void shm_deserialize(const lipc::ShmArchive<BlobInfo> &ar) {
    if(!shm_deserialize_header(ar.header_ptr_)) { return; }
    bkt_id_ = header_->bkt_id_;
    name_ << header_->name_ar_;
    buffers_ << header_->buffers_ar_;
  }

  /** Move pointers into another BlobInfo */
  void WeakMove(BlobInfo &other) {
    SHM_WEAK_MOVE_START(SHM_WEAK_MOVE_DEFAULT(BlobInfo))
    (*header_) = (*other.header_);
    bkt_id_ = other.bkt_id_;
    (*name_) = lipc::Move(*other.name_);
    (*buffers_) = lipc::Move(*other.buffers_);
    SHM_WEAK_MOVE_END()
  }

  /** Deep copy data into another BlobInfo */
  void StrongCopy(const BlobInfo &other) {
    SHM_STRONG_COPY_START(SHM_STRONG_COPY_DEFAULT(BlobInfo))
    (*header_) = (*other.header_);
    bkt_id_ = other.bkt_id_;
    (*name_) = lipc::Copy(*other.name_);
    (*buffers_) = lipc::Copy(*other.buffers_);
    SHM_STRONG_COPY_END()
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
  /** The name of the bucket */
  lipc::mptr<lipc::string> name_;

 public:
  /** Default constructor */
  BucketInfo() = default;

  /** Initialize the data structure */
  void shm_init_main(lipc::ShmArchive<BucketInfo> *ar,
                     lipc::Allocator *alloc) {
    shm_init_header(ar, alloc);
    name_.shm_init(alloc);
  }

  /** Destroy all allocated data */
  void shm_destroy(bool destroy_header = true) {
    SHM_DESTROY_DATA_START
    name_.shm_destroy();
    SHM_DESTROY_DATA_END
    SHM_DESTROY_END
  }

  /** Serialize into \a ShmArchive */
  void shm_serialize(lipc::ShmArchive<BucketInfo> &ar) const {
    shm_serialize_header(ar.header_ptr_);
    name_ >> header_->name_ar_;
  }

  /** Deserialize from \a ShmArchive */
  void shm_deserialize(const lipc::ShmArchive<BucketInfo> &ar) {
    if(!shm_deserialize_header(ar.header_ptr_)) { return; }
    name_ << header_->name_ar_;
  }

  void WeakMove(BucketInfo &other) {
    SHM_WEAK_MOVE_START(SHM_WEAK_MOVE_DEFAULT(BucketInfo))
    (*header_) = (*other.header_);
    (*name_) = lipc::Move(*other.name_);
    shm_serialize(ar_);
    SHM_WEAK_MOVE_END()
  }

  void StrongCopy(const BucketInfo &other) {
    SHM_STRONG_COPY_START(SHM_STRONG_COPY_DEFAULT(BucketInfo))
    (*header_) = (*other.header_);
    (*name_) = lipc::Copy(*other.name_);
    shm_serialize(ar_);
    SHM_STRONG_COPY_END()
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
  VBucketInfo() = default;

  void shm_init_main(lipc::ShmArchive<VBucketInfo> *ar,
                     lipc::Allocator *alloc) {
    shm_init_header(ar, alloc);
    name_.shm_init(alloc);
    blobs_.shm_init(alloc_);
  }

  void shm_destroy(bool destroy_header = true) {
    SHM_DESTROY_DATA_START
    name_.shm_destroy();
    blobs_.shm_destroy();
    SHM_DESTROY_DATA_END
    SHM_DESTROY_END
  }

  void shm_serialize(lipc::ShmArchive<VBucketInfo> &ar) const {
    shm_serialize_header(ar.header_ptr_);
    name_ >> header_->name_;
    blobs_ >> header_->blobs_;
  }

  void shm_deserialize(const lipc::ShmArchive<VBucketInfo> &ar) {
    if(!shm_deserialize_header(ar.header_ptr_)) { return; }
    name_ << header_->name_;
    blobs_ << header_->blobs_;
  }

  void WeakMove(VBucketInfo &other) {
    SHM_WEAK_MOVE_START(SHM_WEAK_MOVE_DEFAULT(VBucketInfo))
    (*header_) = (*other.header_);
    (*name_) = lipc::Move(*other.name_);
    (*blobs_) = lipc::Move(*other.blobs_);
    shm_serialize(ar_);
    SHM_WEAK_MOVE_END()
  }

  void StrongCopy(const VBucketInfo &other) {
    SHM_STRONG_COPY_START(SHM_STRONG_COPY_DEFAULT(VBucketInfo))
    (*header_) = (*other.header_);
    (*name_) = lipc::Copy(*other.name_);
    (*blobs_) = lipc::Copy(*other.blobs_);
    shm_serialize(ar_);
    SHM_STRONG_COPY_END()
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_METADATA_TYPES_H_
