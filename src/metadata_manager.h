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

#ifndef HERMES_SRC_METADATA_MANAGER_H_
#define HERMES_SRC_METADATA_MANAGER_H_

#include "decorator.h"
#include "hermes_types.h"
#include "status.h"
#include "rpc.h"
#include "metadata_types.h"
#include "trait_manager.h"
#include "statuses.h"
#include "rpc_thallium_serialization.h"

namespace hermes {

/** Forward declaration of borg */
class BufferOrganizer;

/**
 * Type name simplification for the various map types
 * */
typedef hipc::unordered_map<hipc::charbuf, BlobId> BLOB_ID_MAP_T;
typedef hipc::unordered_map<hipc::charbuf, TagId> TAG_ID_MAP_T;
typedef hipc::unordered_map<hipc::charbuf, TraitId> TRAIT_ID_MAP_T;
typedef hipc::unordered_map<BlobId, BlobInfo> BLOB_MAP_T;
typedef hipc::unordered_map<TagId, TagInfo> TAG_MAP_T;
typedef hipc::unordered_map<TraitId, hipc::charbuf> TRAIT_MAP_T;
typedef hipc::slist<IoStat> IO_PATTERN_LOG_T;

enum MdmLock {
  kBlobMapLock,
  kBktMapLock,
  kTagMapLock,
  kTraitMapLock,
  kLocalTraitMapLock,
  kIoPatternLog,

  kMdmLockCount
};

/**
 * The SHM representation of the MetadataManager
 * */
template<>
struct ShmHeader<MetadataManager> {
  /// SHM representation of blob id map
  hipc::ShmArchive<BLOB_ID_MAP_T> blob_id_map;
  /// SHM representation of blob map
  hipc::ShmArchive<BLOB_MAP_T> blob_map;
  /// SHM representation of tag id map
  hipc::ShmArchive<TAG_ID_MAP_T> tag_id_map;
  /// SHM representation of tag map
  hipc::ShmArchive<TAG_MAP_T> tag_map;
  /// SHM representation of trait id map
  hipc::ShmArchive<TRAIT_ID_MAP_T> trait_id_map;
  /// SHM representation of trait map
  hipc::ShmArchive<TRAIT_MAP_T> trait_map;
  /// SHM representation of device vector
  hipc::ShmArchive<hipc::vector<DeviceInfo>> devices_;
  /// SHM representation of target info vector
  hipc::ShmArchive<hipc::vector<TargetInfo>> targets_;
  /// SHM representation of prefetcher stats log
  hipc::ShmArchive<IO_PATTERN_LOG_T> io_pattern_log_;
  /// Used to create unique ids. Starts at 1.
  std::atomic<u64> id_alloc_;
  /// Synchronization
  RwLock lock_[kMdmLockCount];
  /// The ID of THIS node
  i32 node_id_;
};

/**
 * Manages the metadata for blobs, buckets, and vbuckets.
 * */
class MetadataManager : public hipc::ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE((MetadataManager),
                         (MetadataManager),
                         (ShmHeader<MetadataManager>))
  RPC_TYPE *rpc_;
  BufferOrganizer *borg_;
  TraitManager *traits_;

  /**====================================
   * Maps
   * ===================================*/
  hipc::Ref<BLOB_ID_MAP_T> blob_id_map_;
  hipc::Ref<TAG_ID_MAP_T> tag_id_map_;
  hipc::Ref<TRAIT_ID_MAP_T> trait_id_map_;
  hipc::Ref<BLOB_MAP_T> blob_map_;
  hipc::Ref<TAG_MAP_T> tag_map_;
  hipc::Ref<TRAIT_MAP_T> trait_map_;
  std::unordered_map<TraitId, Trait*> local_trait_map_;
  RwLock local_lock_;

  /**====================================
   * I/O pattern log
   * ===================================*/
  hipc::Ref<IO_PATTERN_LOG_T> io_pattern_log_;
  bool enable_io_tracing_;
  bool is_mpi_;

  /**====================================
   * Targets + devices
   * ===================================*/
  hipc::Ref<hipc::vector<DeviceInfo>> devices_;
  hipc::Ref<hipc::vector<TargetInfo>> targets_;

 public:
  /**====================================
   * Default Constructor
   * ===================================*/

  /** Initialize local unordered_map */
  void local_init() {
    hipc::Allocator::ConstructObj(local_trait_map_);
    hipc::Allocator::ConstructObj(local_lock_);
  }

  /** Default constructor */
  MetadataManager(ShmHeader<MetadataManager> *header,
                  hipc::Allocator *alloc,
                  ServerConfig *config);

  /**====================================
   * Destructor
   * ===================================*/

  /** Whether the MetadataManager is NULL */
  bool IsNull() { return false; }

  /** Set the MetadataManager to NULL */
  void SetNull() {}

  /**
   * Explicitly destroy the MetadataManager
   * */
  void shm_destroy_main();

  /**====================================
   * SHM Deserialize
   * ===================================*/

  /**
   * Unload the MetadataManager from shared memory
   * */
  void shm_deserialize_main();

  /**
   * Create a unique blob name using TagId
   * */
  template<typename StringT>
  static hipc::uptr<hipc::charbuf> CreateBlobName(
      TagId bkt_id, const StringT &blob_name) {
    auto new_name = hipc::make_uptr<hipc::charbuf>(
        sizeof(uint64_t) + sizeof(uint32_t) + blob_name.size());
    size_t off = 0;
    memcpy(new_name->data() + off, &bkt_id.unique_, sizeof(uint64_t));
    off += sizeof(uint64_t);
    memcpy(new_name->data() + off, &bkt_id.node_id_, sizeof(uint32_t));
    off += sizeof(uint32_t);
    memcpy(new_name->data() + off, blob_name.data(), blob_name.size());
    return new_name;
  }

  /**====================================
   * Bucket Operations
   * ===================================*/

  /**
   * Get the size of a bucket
   * */
  RPC size_t LocalGetBucketSize(TagId bkt_id);
  DEFINE_RPC(size_t, GetBucketSize, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Update \a bkt_id BUCKET stats
   * */
  RPC bool LocalSetBucketSize(TagId bkt_id, size_t new_size);
  DEFINE_RPC(bool, SetBucketSize, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Lock the bucket
   * */
  bool LocalLockBucket(TagId bkt_id, MdLockType lock_type);
  DEFINE_RPC(bool, LockBucket, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Unlock the bucket
   * */
  bool LocalUnlockBucket(TagId bkt_id, MdLockType lock_type);
  DEFINE_RPC(bool, UnlockBucket, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Destroy \a bkt_id bucket
   * */
  RPC bool LocalClearBucket(TagId bkt_id);
  DEFINE_RPC(bool, ClearBucket, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**====================================
   * Blob Operations
   * ===================================*/

  /**
   * Create a blob's metadata
   *
   * @param bkt_id id of the bucket
   * @param blob_name semantic blob name
   * @param data the data being placed
   * @param buffers the buffers to place data in
   * */
  RPC std::tuple<BlobId, bool, size_t> LocalPutBlobMetadata(
      TagId bkt_id, const std::string &blob_name, size_t blob_size,
      std::vector<BufferInfo> &buffers, float score);
  DEFINE_RPC((std::tuple<BlobId, bool, size_t>), PutBlobMetadata, 1,
             STRING_HASH_LAMBDA)

  /**
   * Creates the blob metadata
   *
   * @param bkt_id id of the bucket
   * @param blob_name semantic blob name
   * */
  std::pair<BlobId, bool> LocalTryCreateBlob(TagId bkt_id,
                                             const std::string &blob_name);
  DEFINE_RPC((std::pair<BlobId, bool>), TryCreateBlob, 1,
             STRING_HASH_LAMBDA)

  /**
   * Tag a blob
   *
   * @param blob_id id of the blob being tagged
   * @param tag_name tag name
   * */
  Status LocalTagBlob(BlobId blob_id, TagId tag_id);
  DEFINE_RPC(Status, TagBlob, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Check if blob has a tag
   * */
  bool LocalBlobHasTag(BlobId blob_id, TagId tag_id);
  DEFINE_RPC(bool, BlobHasTag, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Get \a blob_name BLOB from \a bkt_id bucket
   * */
  RPC BlobId LocalGetBlobId(TagId bkt_id, const std::string &blob_name);
  DEFINE_RPC(BlobId, GetBlobId, 1,
             STRING_HASH_LAMBDA)

  /**
   * Get \a blob_name BLOB name from \a blob_id BLOB id
   * */
  RPC std::string LocalGetBlobName(BlobId blob_id);
  DEFINE_RPC(std::string, GetBlobName, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Get \a score from \a blob_id BLOB id
   * */
  RPC float LocalGetBlobScore(BlobId blob_id);
  DEFINE_RPC(float, GetBlobScore, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Lock the blob
   * */
  RPC bool LocalLockBlob(BlobId blob_id, MdLockType lock_type);
  DEFINE_RPC(bool, LockBlob, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Unlock the blob
   * */
  RPC bool LocalUnlockBlob(BlobId blob_id, MdLockType lock_type);
  DEFINE_RPC(bool, UnlockBlob, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Get \a blob_id blob's buffers
   * */
  RPC std::vector<BufferInfo> LocalGetBlobBuffers(BlobId blob_id);
  DEFINE_RPC(std::vector<BufferInfo>, GetBlobBuffers, 0,
             UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Rename \a blob_id blob to \a new_blob_name new blob name
   * in \a bkt_id bucket.
   * */
  RPC bool LocalRenameBlob(TagId bkt_id, BlobId blob_id,
                           const std::string &new_blob_name);
  DEFINE_RPC(bool, RenameBlob, 1, UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Destroy \a blob_id blob in \a bkt_id bucket
   * */
  RPC bool LocalDestroyBlob(TagId bkt_id, BlobId blob_id);
  DEFINE_RPC(bool, DestroyBlob, 1, UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**====================================
   * Bucket + Blob Operations
   * ===================================*/

  /**
   * Destroy all blobs + buckets
   * */
  void LocalClear();

  /**
   * Destroy all blobs + buckets globally
   * */
  void GlobalClear();

  /**====================================
   * Target Operations
   * ===================================*/

  /**
   * Update the capacity of the target device
   * */
  RPC void LocalUpdateTargetCapacity(TargetId tid, off64_t offset) {
    TargetInfo &target = *(*targets_)[tid.GetIndex()];
    target.rem_cap_ += offset;
  }
  DEFINE_RPC(bool, UpdateTargetCapacity, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Update the capacity of the target device
   * */
  RPC std::vector<TargetInfo> LocalGetTargetInfo() {
    return hshm::to_stl_vector<TargetInfo>(*targets_);
  }

  /**
   * Get the TargetInfo for neighborhood
   * */
  // hipc::vector<TargetInfo> GetNeighborhoodTargetInfo() { return {}; }

  /**
   * Get all TargetInfo in the system
   * */
  // hipc::vector<TargetInfo> GetGlobalTargetInfo() { return {}; }

  /**====================================
   * Tag Operations
   * ===================================*/

  /**
   * Create a tag
   * */
  std::pair<TagId, bool> LocalGetOrCreateTag(const std::string &tag_name,
                                             bool owner,
                                             std::vector<TraitId> &traits,
                                             size_t backend_size);
  DEFINE_RPC((std::pair<TagId, bool>),
             GetOrCreateTag, 0, STRING_HASH_LAMBDA)
  TagId GlobalCreateTag(const std::string &tag_name,
                        bool owner,
                        std::vector<TraitId> &traits) {
    return LocalGetOrCreateTag(tag_name, owner, traits, 0).first;
  }

  /**
   * Get the id of a tag
   * */
  TagId LocalGetTagId(const std::string &tag_name);
  DEFINE_RPC(TagId, GetTagId, 0, STRING_HASH_LAMBDA)

  /**
   * Get the name of a tag
   * */
  RPC std::string LocalGetTagName(TagId tag);
  DEFINE_RPC(std::string, GetTagName, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Rename a tag
   * */
  RPC bool LocalRenameTag(TagId tag, const std::string &new_name);
  DEFINE_RPC(bool, RenameTag, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Delete a tag
   * */
  bool LocalDestroyTag(TagId tag);
  DEFINE_RPC(bool, DestroyTag, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Add a blob to a tag index.
   * */
  Status LocalTagAddBlob(TagId tag_id, BlobId blob_id);
  DEFINE_RPC(Status, TagAddBlob, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA);

  /**
   * Remove a blob from a tag index.
   * */
  Status LocalTagRemoveBlob(TagId tag_id, BlobId blob_id);
  DEFINE_RPC(Status, TagRemoveBlob, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA);

  /**
   * Find all blobs pertaining to a tag
   * */
  std::vector<BlobId> LocalGroupByTag(TagId tag_id);
  DEFINE_RPC(std::vector<BlobId>, GroupByTag, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA);

  /**
   * Add a trait to a tag index. Create tag if it does not exist.
   * */
  RPC bool LocalTagAddTrait(TagId tag_id, TraitId trait_id);
  DEFINE_RPC(bool, TagAddTrait, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA);

  /**
   * Find all traits pertaining to a tag
   * */
  std::vector<TraitId> LocalTagGetTraits(TagId tag_id);
  DEFINE_RPC(std::vector<TraitId>, TagGetTraits, 0,
             UNIQUE_ID_TO_NODE_ID_LAMBDA);

  /**====================================
   * Trait Operations
   * ===================================*/

  /**
   * Register a trait. Stores the state of the trait in a way that can
   * be queried by all processes.
   * */
  RPC TraitId LocalRegisterTrait(TraitId trait_id,
                                 const hshm::charbuf &trait_params);
  DEFINE_RPC(TraitId, RegisterTrait, 1, TRAIT_PARAMS_HASH_LAMBDA);

  /**
   * Get trait identifier
   * */
  RPC TraitId LocalGetTraitId(const std::string &trait_uuid);
  DEFINE_RPC((TraitId), GetTraitId, 0, STRING_HASH_LAMBDA);

  /**
   * Get trait parameters
   * */
  RPC hshm::charbuf LocalGetTraitParams(TraitId trait_id);
  DEFINE_RPC((hshm::charbuf),
             GetTraitParams, 0,
             UNIQUE_ID_TO_NODE_ID_LAMBDA);

  /**
   * Get an existing trait. Will load the trait into this process's
   * address space if necessary.
   * */
  Trait* GlobalGetTrait(TraitId trait_id);

  /**====================================
   * Statistics Operations
   * ===================================*/

  /** Add an I/O statistic to the internal log */
  void AddIoStat(TagId tag_id, BlobId blob_id, size_t blob_size, IoType type);

  /** Add an I/O statistic to the internal log */
  void ClearIoStats(size_t count);

  /**====================================
   * Private Operations
   * ===================================*/

 private:
  /** Acquire the external lock to Bucket or Blob */
  template<typename MapFirst, typename MapSecond, typename IdT>
  bool LockMdObject(hipc::unordered_map<MapFirst, MapSecond> &map,
                    IdT id,
                    MdLockType lock_type) {
    auto iter = map.find(id);
    if (iter == map.end()) {
      return false;
    }
    hipc::Ref<hipc::pair<MapFirst, MapSecond>> info = *iter;
    MapSecond &obj_info = *info->second_;
    switch (lock_type) {
      case MdLockType::kExternalRead: {
        obj_info.header_->lock_[1].ReadLock(0);
        return true;
      }
      case MdLockType::kExternalWrite: {
        obj_info.header_->lock_[1].WriteLock(0);
        return true;
      }
      default: {
        return false;
      }
    }
  }

  /** Release the external lock to Bucket or Blob */
  template<typename MapFirst, typename MapSecond, typename IdT>
  bool UnlockMdObject(hipc::unordered_map<MapFirst, MapSecond> &map,
                      IdT id,
                      MdLockType lock_type) {
    auto iter = map.find(id);
    if (iter == map.end()) {
      return false;
    }
    hipc::Ref<hipc::pair<MapFirst, MapSecond>> info = *iter;
    MapSecond &obj_info = *info->second_;
    switch (lock_type) {
      case MdLockType::kExternalRead: {
        obj_info.header_->lock_[1].ReadUnlock();
        return true;
      }
      case MdLockType::kExternalWrite: {
        obj_info.header_->lock_[1].WriteUnlock();
        return true;
      }
      default: {
        return false;
      }
    }
  }

  /**====================================
   * Debugging Logs
   * ===================================*/
 public:
  void PrintDeviceInfo() {
    int id = 0;
    for (hipc::Ref<DeviceInfo> dev_info : *devices_) {
      HILOG(kInfo, "Device {} is mounted on {} with capacity {} bytes",
            id,
            dev_info->mount_point_->str(),
            dev_info->header_->capacity_)
      ++id;
    }
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_METADATA_MANAGER_H_
