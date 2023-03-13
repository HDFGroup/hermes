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
#define VBUCKET_HASH TEXT_HASH(vbkt_name)

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
struct ShmHeader<BlobInfo> : public hipc::ShmBaseHeader {
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

  /** Default constructor */
  ShmHeader() = default;

  /** Copy constructor */
  ShmHeader(const ShmHeader &other) noexcept {
    strong_copy(other);
  }

  /** Move constructor */
  ShmHeader(ShmHeader &&other) noexcept {
    strong_copy(other);
  }

  /** Copy assignment */
  ShmHeader& operator=(const ShmHeader &other) {
    if (this != &other) {
      strong_copy(other);
    }
    return *this;
  }

  /** Move assignment */
  ShmHeader& operator=(ShmHeader &&other) {
    if (this != &other) {
      strong_copy(other);
    }
    return *this;
  }

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
  hipc::ShmRef<hipc::string> name_;
  /// The BufferInfo vector
  hipc::ShmRef<hipc::vector<BufferInfo>> buffers_;
  /// The Tag slist
  hipc::ShmRef<hipc::slist<TagId>> tags_;

  /** Default constructor. Does nothing. */
  BlobInfo() = default;

  /** Initialize the data structure */
  void shm_init_main(ShmHeader<BlobInfo> *header,
                     hipc::Allocator *alloc) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    shm_deserialize_main();
    name_->shm_init(alloc_);
    buffers_->shm_init(alloc_);
    tags_->shm_init(alloc_);
  }

  /** Destroy all allocated data */
  void shm_destroy_main();

  /** Serialize pointers */
  void shm_serialize_main() const {}

  /** Deserialize pointers */
  void shm_deserialize_main() {
    (*name_) << header_->name_ar_.internal_ref(alloc_);
    (*buffers_) << header_->buffers_ar_.internal_ref(alloc_);
    (*tags_) << header_->tags_ar_.internal_ref(alloc_);
  }

  /** Move pointers into another BlobInfo */
  void shm_weak_move_main(ShmHeader<BlobInfo> *header,
                          hipc::Allocator *alloc,
                          BlobInfo &other) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    shm_deserialize_main();
    (*header_) = (*other.header_);
    (*name_) = std::move(*other.name_);
    (*buffers_) = std::move(*other.buffers_);
    (*tags_) = std::move(*other.tags_);
  }

  /** Deep copy data into another BlobInfo */
  void shm_strong_copy_main(ShmHeader<BlobInfo> *header,
                            hipc::Allocator *alloc,
                            const BlobInfo &other) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    shm_deserialize_main();
    (*header_) = (*other.header_);
    (*name_) = (*other.name_);
    (*buffers_) = (*other.buffers_);
    (*tags_) = (*other.tags_);
  }
};

/** Represents TagInfo in shared memory */
template<>
struct ShmHeader<TagInfo> : public hipc::ShmBaseHeader {
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

  /** Default constructor */
  ShmHeader() : owner_(false) {}

  /** Copy constructor */
  ShmHeader(const ShmHeader &other) noexcept {
    strong_copy(other);
  }

  /** Move constructor */
  ShmHeader(ShmHeader &&other) noexcept {
    strong_copy(other);
  }

  /** Copy assignment */
  ShmHeader& operator=(const ShmHeader &other) {
    if (this != &other) {
      strong_copy(other);
    }
    return *this;
  }

  /** Move assignment */
  ShmHeader& operator=(ShmHeader &&other) {
    if (this != &other) {
      strong_copy(other);
    }
    return *this;
  }

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
  hipc::ShmRef<hipc::string> name_;           /**< The name of this Tag */
  hipc::ShmRef<hipc::slist<BlobId>> blobs_;   /**< All blobs of this Tag */
  hipc::ShmRef<hipc::slist<TraitId>> traits_; /**< All traits of this Tag */

 public:
  /** Default constructor */
  TagInfo() = default;

  /** Initialize the data structure */
  void shm_init_main(ShmHeader<TagInfo> *header,
                     hipc::Allocator *alloc) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    shm_deserialize_main();
    name_->shm_init(alloc);
    blobs_->shm_init(alloc);
    traits_->shm_init(alloc);
  }

  /** Destroy all allocated data */
  void shm_destroy_main();

  /** Serialize pointers */
  void shm_serialize_main() const {}

  /** Deserialize pointers */
  void shm_deserialize_main() {
    (*name_) << header_->name_ar_.internal_ref(alloc_);
    (*blobs_) << header_->blobs_ar_.internal_ref(alloc_);
    (*traits_) << header_->traits_ar_.internal_ref(alloc_);
  }

  /** Move other object into this one */
  void shm_weak_move_main(ShmHeader<TagInfo> *header,
                          hipc::Allocator *alloc,
                          TagInfo &other) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    shm_deserialize_main();
    (*header_) = (*other.header_);
    (*name_) = std::move(*other.name_);
    (*blobs_) = std::move(*other.blobs_);
    (*traits_) = std::move(*other.traits_);
  }

  /** Copy other object into this one */
  void shm_strong_copy_main(ShmHeader<TagInfo> *header,
                            hipc::Allocator *alloc,
                            const TagInfo &other) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    shm_deserialize_main();
    (*header_) = (*other.header_);
    (*name_) = (*other.name_);
    (*blobs_) = (*other.blobs_);
    (*traits_) = (*other.traits_);
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_METADATA_TYPES_H_
