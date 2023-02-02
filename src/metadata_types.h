/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Distributed under BSD 3-Clause license.                                   *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Illinois Institute of Technology.                        *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Hermes. The full Hermes copyright notice, including  *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the top directory. If you do not  *
 * have access to the file, you may request a copy from help@hdfgroup.org.   *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#ifndef HERMES_SRC_METADATA_TYPES_H_
#define HERMES_SRC_METADATA_TYPES_H_

#include "hermes_types.h"
#include "config_server.h"
#include "adapter/io_client/io_client.h"

namespace hermes {

using adapter::GlobalIoClientState;
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

  /** Default constructor */
  TargetInfo() = default;

  /** Primary constructor */
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

  /** Default constructor */
  BufferInfo() = default;

  /** Primary constructor */
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
  lipc::TypedPointer<lipc::string> name_ar_; /**< SHM pointer to string */
  lipc::TypedPointer<lipc::vector<BufferInfo>>
      buffers_ar_;     /**< SHM pointer to BufferInfo vector */
  size_t blob_size_;   /**< The overall size of the blob */
  RwLock rwlock_;      /**< Ensures BlobInfo access is synchronized */

  /** Default constructor */
  ShmHeader() = default;

  /** Copy constructor */
  ShmHeader(const ShmHeader &other) noexcept
      : bkt_id_(other.bkt_id_), name_ar_(other.name_ar_),
        buffers_ar_(other.buffers_ar_),
        blob_size_(other.blob_size_),
        rwlock_() {}

  /** Move constructor */
  ShmHeader(ShmHeader &&other) noexcept
  : bkt_id_(std::move(other.bkt_id_)), name_ar_(std::move(other.name_ar_)),
    buffers_ar_(std::move(other.buffers_ar_)),
    blob_size_(other.blob_size_),
    rwlock_() {}

  /** Copy assignment */
  ShmHeader& operator=(const ShmHeader &other) {
    if (this != &other) {
      bkt_id_ = other.bkt_id_;
      name_ar_ = other.name_ar_;
      buffers_ar_ = other.buffers_ar_;
      blob_size_ = other.blob_size_;
    }
    return *this;
  }

  /** Move assignment */
  ShmHeader& operator=(ShmHeader &&other) {
    if (this != &other) {
      bkt_id_ = std::move(other.bkt_id_);
      name_ar_ = std::move(other.name_ar_);
      buffers_ar_ = std::move(other.buffers_ar_);
      blob_size_ = other.blob_size_;
    }
    return *this;
  }
};

/** Blob metadata */
struct BlobInfo : public lipc::ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE(BlobInfo, BlobInfo, ShmHeader<BlobInfo>);

 public:
  /// The bucket containing the blob
  BucketId bkt_id_;
  /// The name of the blob
  lipc::mptr<lipc::string> name_;
  /// The BufferInfo vector
  lipc::mptr<lipc::vector<BufferInfo>> buffers_;
  /// Synchronize access to blob
  Mutex lock_;
  /// Ensure that operations belonging to a transaction are not locked forever
  TransactionId transaction_;

  /** Default constructor. Does nothing. */
  BlobInfo() = default;

  /** Initialize the data structure */
  void shm_init_main(ShmHeader<BlobInfo> *header,
                     lipc::Allocator *alloc) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    name_.shm_init(alloc_);
    buffers_.shm_init(alloc_);
    shm_serialize_main();
  }

  /** Destroy all allocated data */
  void shm_destroy_main() {
    name_.shm_destroy();
    buffers_.shm_destroy();
  }

  /** Serialize pointers */
  void shm_serialize_main() const {
    header_->bkt_id_ = bkt_id_;
    name_ >> header_->name_ar_;
    buffers_ >> header_->buffers_ar_;
  }

  /** Deserialize pointers */
  void shm_deserialize_main() {
    bkt_id_ = header_->bkt_id_;
    name_ << header_->name_ar_;
    buffers_ << header_->buffers_ar_;
  }

  /** Move pointers into another BlobInfo */
  void shm_weak_move_main(ShmHeader<BlobInfo> *header,
                          lipc::Allocator *alloc,
                          BlobInfo &other) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    (*header_) = (*other.header_);
    bkt_id_ = other.bkt_id_;
    (*name_) = std::move(*other.name_);
    (*buffers_) = std::move(*other.buffers_);
    shm_serialize_main();
  }

  /** Deep copy data into another BlobInfo */
  void shm_strong_copy_main(ShmHeader<BlobInfo> *header,
                            lipc::Allocator *alloc,
                            const BlobInfo &other) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    (*header_) = (*other.header_);
    bkt_id_ = other.bkt_id_;
    (*name_) = (*other.name_);
    (*buffers_) = (*other.buffers_);
    shm_serialize_main();
  }
};

/** Represents BucketInfo in shared memory */
template<>
struct ShmHeader<BucketInfo> : public lipc::ShmBaseHeader {
  lipc::TypedPointer<lipc::string> name_ar_;
  lipc::TypedPointer<lipc::vector<BlobId>> blobs_ar_;
  size_t internal_size_;
  GlobalIoClientState client_state_;
};

/** Metadata for a Bucket */
struct BucketInfo : public lipc::ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE(BucketInfo, BucketInfo, ShmHeader<BucketInfo>);

 public:
  lipc::mptr<lipc::string> name_; /**< The name of the bucket */

 public:
  /** Default constructor */
  BucketInfo() = default;

  /** Initialize the data structure */
  void shm_init_main(ShmHeader<BucketInfo> *header,
                     lipc::Allocator *alloc) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    name_ = lipc::make_mptr<lipc::string>(alloc);
  }

  /** Destroy all allocated data */
  void shm_destroy_main(bool destroy_header = true) {
    name_.shm_destroy();
  }

  /** Serialize pointers */
  void shm_serialize_main() const {
    name_ >> header_->name_ar_;
  }

  /** Deserialize pointers */
  void shm_deserialize_main() {
    name_ << header_->name_ar_;
  }

  /** Move other object into this one */
  void shm_weak_move_main(ShmHeader<BucketInfo> *header,
                          lipc::Allocator *alloc,
                          BucketInfo &other) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    (*header_) = (*other.header_);
    (*name_) = (*other.name_);
    shm_serialize_main();
  }

  /** Copy other object into this one */
  void shm_strong_copy_main(ShmHeader<BucketInfo> *header,
                            lipc::Allocator *alloc,
                            const BucketInfo &other) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    (*header_) = (*other.header_);
    (*name_) = (*other.name_);
    shm_serialize_main();
  }
};

/** Represents a VBucket in shared memory */
template<>
struct ShmHeader<VBucketInfo> : public lipc::ShmBaseHeader {
  lipc::TypedPointer<lipc::charbuf> name_;
  lipc::TypedPointer<lipc::unordered_map<BlobId, BlobId>> blobs_;
};

/** Metadata for a VBucket */
struct VBucketInfo : public lipc::ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE(VBucketInfo, VBucketInfo, ShmHeader<VBucketInfo>);

 public:
  lipc::mptr<lipc::charbuf> name_;
  lipc::mptr<lipc::unordered_map<BlobId, BlobId>> blobs_;

 public:
  /** Default constructor. */
  VBucketInfo() = default;

  /** Default SHM Constructor */
  void shm_init_main(ShmHeader<VBucketInfo> *header,
                     lipc::Allocator *alloc) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    name_ = lipc::make_mptr<lipc::charbuf>(alloc_);
    blobs_ = lipc::make_mptr<lipc::unordered_map<BlobId, BlobId>>(alloc_);
  }

  /** Free shared memory */
  void shm_destroy_main() {
    name_.shm_destroy();
    blobs_.shm_destroy();
  }

  /** Serialize into SHM */
  void shm_serialize_main() const {
    name_ >> header_->name_;
    blobs_ >> header_->blobs_;
  }

  /** Deserialize from SHM */
  void shm_deserialize_main() {
    name_ << header_->name_;
    blobs_ << header_->blobs_;
  }

  /** Move other object into this one */
  void shm_weak_move_main(ShmHeader<VBucketInfo> *header,
                          lipc::Allocator *alloc,
                          VBucketInfo &other) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    (*header_) = (*other.header_);
    (*name_) = std::move(*other.name_);
    (*blobs_) = std::move(*other.blobs_);
    shm_serialize_main();
  }

  /** Copy other object into this one */
  void shm_strong_copy_main(ShmHeader<VBucketInfo> *header,
                            lipc::Allocator *alloc,
                            const VBucketInfo &other) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    (*header_) = (*other.header_);
    (*name_) = (*other.name_);
    (*blobs_) = (*other.blobs_);
    shm_serialize_main();
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_METADATA_TYPES_H_
