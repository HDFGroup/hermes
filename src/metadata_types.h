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
#include "io_client/io_client.h"

namespace hermes {

#define TEXT_HASH(text) \
  std::hash<lipc::charbuf>{}(text);
#define ID_HASH(id) \
  id.bits_.node_id_
#define BUCKET_HASH TEXT_HASH(bkt_name)
#define BLOB_HASH TEXT_HASH(blob_name)

using adapter::GlobalIoClientState;
using api::Blob;       /**< Namespace simplification for blob */
struct TagInfo;     /**< Forward declaration of TagInfo */
struct BlobInfo;       /**< Forward declaration of BlobInfo */
struct TraitInfo;    /**< Forward declaration of TraitInfo */

/** Device information (e.g., path) */
using config::DeviceInfo;
/** I/O interface to use for BORG (e.g., POSIX) */
using config::IoInterface;

/** Lock type used for internal metadata */
enum class MdLockType {
  kInternalRead,    /**< Internal read lock used by Hermes */
  kInternalWrite,   /**< Internal write lock by Hermes */
  kExternalRead,    /**< External is used by programs */
  kExternalWrite,   /**< External is used by programs */
};

/** Represents different updates for bucket statistics */
enum class BucketUpdate {
  kNone,      /**< Do not update bucket stats */
  kInternal,  /**< Update the internal bucket stats only */
  kBackend,   /**< Update I/O client bucket stats */
  kBoth       /**< Update both at the same time */
};

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
  int t_slab_;          /**< The index of the slab in the target */
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
        blob_off_(blob_off), blob_size_(blob_size) {}

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
    t_slab_ = other.t_slab_;
    t_off_ = other.t_off_;
    t_size_ = other.t_size_;
    blob_off_ = other.blob_off_;
    blob_size_ = other.blob_size_;
  }
};

/** Represents BlobInfo in shared memory */
template<>
struct ShmHeader<BlobInfo> {
  SHM_CONTAINER_HEADER_TEMPLATE(ShmHeader)
  BlobId blob_id_;   /**< The identifier of this blob */
  TagId tag_id_;  /**< The bucket containing the blob */
  hipc::ShmArchive<hipc::string>
      name_ar_;      /**< SHM pointer to string */
  hipc::ShmArchive<hipc::vector<BufferInfo>>
      buffers_ar_;   /**< SHM pointer to BufferInfo vector */
  hipc::ShmArchive<hipc::slist<TagId>>
      tags_ar_;        /**< SHM pointer to tag list */
  size_t blob_size_;   /**< The overall size of the blob */
  RwLock lock_[2];     /**< Ensures BlobInfo access is synchronized */
  time_t update_time_; /**< Last time statistics updated */
  int access_count_;   /**< The number of times blob accessed */
  /**
   * Estimate when blob will be accessed next (ns)
   * 0 indicates unknown
   * */
  size_t next_access_time_ns_;

  void strong_copy(const ShmHeader &other) {
    blob_id_ = other.blob_id_;
    tag_id_ = other.tag_id_;
    blob_size_ = other.blob_size_;
  }
};

/** Blob metadata */
struct BlobInfo : public hipc::ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE(BlobInfo, BlobInfo, ShmHeader<BlobInfo>);

 public:
  /// The name of the blob
  hipc::Ref<hipc::string> name_;
  /// The BufferInfo vector
  hipc::Ref<hipc::vector<BufferInfo>> buffers_;
  /// The Tag slist
  hipc::Ref<hipc::slist<TagId>> tags_;

  /**====================================
   * Default Constructor
   * ===================================*/

  /** Initialize the data structure */
  explicit BlobInfo(ShmHeader<BlobInfo> *header, hipc::Allocator *alloc) {
    shm_init_header(header, alloc);
    name_ = hipc::make_ref<hipc::string>(header_->name_ar_, alloc_);
    buffers_ = hipc::make_ref<hipc::vector<BufferInfo>>(
        header_->buffers_ar_, alloc_);
    tags_ = hipc::make_ref<hipc::slist<TagId>>(
        header_->tags_ar_, alloc_);
  }

  /** Deserialize pointers */
  void shm_deserialize_main() {
    name_ = hipc::Ref<hipc::string>(header_->name_ar_, alloc_);
    buffers_ = hipc::Ref<hipc::vector<BufferInfo>>(
        header_->buffers_ar_, alloc_);
    tags_ = hipc::Ref<hipc::slist<TagId>>(
        header_->tags_ar_, alloc_);
  }

  /**====================================
   * Destructor
   * ===================================*/

  /** Whether the TagInfo is NULL */
  bool IsNull() { return false; }

  /** Set the TagInfo to NULL */
  void SetNull() {}

  /** Destroy all allocated data */
  void shm_destroy_main();
};

/** Represents TagInfo in shared memory */
template<>
struct ShmHeader<TagInfo> {
  SHM_CONTAINER_HEADER_TEMPLATE(ShmHeader)
  TagId tag_id_;           /**< ID of the tag */
  hipc::ShmArchive<hipc::string>
      name_ar_;            /**< Archive of tag name */
  hipc::ShmArchive<hipc::slist<BlobId>>
      blobs_ar_;           /**< Archive of blob list */
  hipc::ShmArchive<hipc::slist<TraitId>>
      traits_ar_;          /**< Archive of trait list */
  size_t internal_size_;   /**< Current bucket size */
  GlobalIoClientState
      client_state_;       /**< State needed by I/O clients */
  bool owner_;             /**< Whether this tag owns the blobs */

  void strong_copy(const ShmHeader &other) {
    internal_size_ = other.internal_size_;
    client_state_ = other.client_state_;
    owner_ = other.owner_;
  }
};

/** Metadata for a Tag */
struct TagInfo : public hipc::ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE(TagInfo, TagInfo, ShmHeader<TagInfo>);

 public:
  hipc::Ref<hipc::string> name_;           /**< The name of this Tag */
  hipc::Ref<hipc::slist<BlobId>> blobs_;   /**< All blobs of this Tag */
  hipc::Ref<hipc::slist<TraitId>> traits_; /**< All traits of this Tag */

 public:
  /**====================================
   * Default Constructor
   * ===================================*/

  /** SHM constructor. Default. */
  explicit TagInfo(ShmHeader<TagInfo> *header, hipc::Allocator *alloc) {
    shm_init_header(header, alloc);
    name_ = hipc::make_ref<hipc::string>(header_->name_ar_, alloc_);
    blobs_ = hipc::make_ref<hipc::slist<BlobId>>(header_->blobs_ar_, alloc_);
    traits_ = hipc::make_ref<hipc::slist<TraitId>>(header_->traits_ar_, alloc_);
  }

  /**====================================
   * SHM Deserialization
   * ===================================*/

  /** Deserialize pointers */
  void shm_deserialize_main() {
    name_ = hipc::Ref<hipc::string>(header_->name_ar_, alloc_);
    blobs_ = hipc::Ref<hipc::slist<BlobId>>(header_->blobs_ar_, alloc_);
    traits_ = hipc::Ref<hipc::slist<TraitId>>(header_->traits_ar_, alloc_);
  }

  /**====================================
   * Destructor
   * ===================================*/

  /** Whether the TagInfo is NULL */
  bool IsNull() { return false; }

  /** Set the TagInfo to NULL */
  void SetNull() {}

  /** Destroy all allocated data */
  void shm_destroy_main();
};

}  // namespace hermes

#endif  // HERMES_SRC_METADATA_TYPES_H_
