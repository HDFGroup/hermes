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
#include "statuses.h"
#include "rpc_thallium_serialization.h"
#include "io_client/io_client_factory.h"

namespace hermes {

/** Namespace simplification for AdapterFactory */
using hermes::adapter::IoClientFactory;

/** Namespace simplification for AbstractAdapter */
using hermes::adapter::IoClientContext;

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
typedef hipc::unordered_map<TraitId, TraitInfo> TRAIT_MAP_T;

enum MdMapLock {
  kBlobMapLock,
  kBktMapLock,
  kTagMapLock,
  kTraitMapLock,
  kLocalTraitMapLock,

  kMdMapLockCount
};

/**
 * The SHM representation of the MetadataManager
 * */
struct MetadataManagerShmHeader {
  /// SHM representation of blob id map
  hipc::TypedPointer<hipc::mptr<BLOB_ID_MAP_T>> blob_id_map_ar_;
  /// SHM representation of tag id map
  hipc::TypedPointer<hipc::mptr<TAG_ID_MAP_T>> tag_id_map_ar_;
  /// SHM representation of trait id map
  hipc::TypedPointer<hipc::mptr<TAG_ID_MAP_T>> trait_id_map_ar_;
  /// SHM representation of blob map
  hipc::TypedPointer<hipc::mptr<BLOB_MAP_T>> blob_map_ar_;
  /// SHM representation of tag map
  hipc::TypedPointer<hipc::mptr<TAG_MAP_T>> tag_map_ar_;
  /// SHM representation of trait map
  hipc::TypedPointer<hipc::mptr<TRAIT_MAP_T>> trait_map_ar_;
  /// SHM representation of device vector
  hipc::TypedPointer<hipc::mptr<hipc::vector<DeviceInfo>>> devices_;
  /// SHM representation of target info vector
  hipc::TypedPointer<hipc::mptr<hipc::vector<TargetInfo>>> targets_;
  /// Used to create unique ids. Starts at 1.
  std::atomic<u64> id_alloc_;
  /// Synchronization
  RwLock lock_[kMdMapLockCount];
};

/**
 * Manages the metadata for blobs, buckets, and vbuckets.
 * */
class MetadataManager {
 public:
  RPC_TYPE *rpc_;
  MetadataManagerShmHeader *header_;
  BufferOrganizer *borg_;

  /**====================================
   * Maps
   * ===================================*/
  hipc::mptr<BLOB_ID_MAP_T> blob_id_map_;
  hipc::mptr<TAG_ID_MAP_T> tag_id_map_;
  hipc::mptr<TRAIT_ID_MAP_T> trait_id_map_;
  hipc::mptr<BLOB_MAP_T> blob_map_;
  hipc::mptr<TAG_MAP_T> tag_map_;
  hipc::mptr<TRAIT_MAP_T> trait_map_;
  hipc::unordered_map<TraitId, void*> local_trait_map_;

  /**====================================
   * Targets + devices
   * ===================================*/
  hipc::mptr<hipc::vector<DeviceInfo>> devices_;
  hipc::mptr<hipc::vector<TargetInfo>> targets_;

 public:
  /**====================================
   * SHM Overrides
   * ===================================*/

  /** Default constructor */
  MetadataManager() = default;

  /**
   * Explicitly initialize the MetadataManager
   * */
  void shm_init(ServerConfig *config, MetadataManagerShmHeader *header);

  /**
   * Explicitly destroy the MetadataManager
   * */
  void shm_destroy();

  /**
   * Store the MetadataManager in shared memory.
   * */
  void shm_serialize();

   /**
    * Unload the MetadtaManager from shared memory
    * */
  void shm_deserialize(MetadataManagerShmHeader *header);

   /**
    * Create a unique blob name using TagId
    * */
  static hipc::charbuf CreateBlobName(TagId bkt_id,
                                      const hipc::charbuf &blob_name) {
    hipc::charbuf new_name(sizeof(uint64_t) + sizeof(uint32_t)
                           + blob_name.size());
    size_t off = 0;
    memcpy(new_name.data_mutable() + off, &bkt_id.unique_, sizeof(uint64_t));
    off += sizeof(uint64_t);
    memcpy(new_name.data_mutable() + off, &bkt_id.node_id_, sizeof(uint32_t));
    off += sizeof(uint32_t);
    memcpy(new_name.data_mutable() + off, blob_name.data(), blob_name.size());
    return new_name;
  }

  /**====================================
   * Bucket Operations
   * ===================================*/

  /**
   * Get the size of a bucket (depends on the IoClient used).
   * */
  RPC size_t LocalGetBucketSize(TagId bkt_id,
                                const IoClientContext &opts);
  DEFINE_RPC(size_t, GetBucketSize, 0,
             UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Destroy \a bkt_id bucket
   * */
  RPC bool LocalClearBucket(TagId bkt_id);
  DEFINE_RPC(bool, ClearBucket, 0,
             UNIQUE_ID_TO_NODE_ID_LAMBDA)


  /**====================================
   * Blob Operations
   * ===================================*/

  /**
   * Put a blob in a bucket
   *
   * @param bkt_id id of the bucket
   * @param blob_name semantic blob name
   * @param data the data being placed
   * @param buffers the buffers to place data in
   * */
  RPC std::tuple<BlobId, bool, size_t>
  LocalPutBlobMetadata(
      TagId bkt_id,
      const hipc::charbuf &blob_name,
      size_t blob_size,
      std::vector<BufferInfo> &buffers);
  DEFINE_RPC((std::tuple<BlobId, bool, size_t>),
             PutBlobMetadata, 0,
             UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Creates the blob metadata
   *
   * @param bkt_id id of the bucket
   * @param blob_name semantic blob name
   * */
  std::pair<BlobId, bool> LocalTryCreateBlob(
      TagId bkt_id,
      const hipc::charbuf &blob_name);
  DEFINE_RPC((std::pair<BlobId, bool>),
             TryCreateBlob, 0,
             UNIQUE_ID_TO_NODE_ID_LAMBDA)

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
  RPC BlobId LocalGetBlobId(TagId bkt_id, const hipc::charbuf &blob_name);
  DEFINE_RPC(BlobId, GetBlobId, 0,
             UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Get \a blob_name BLOB name from \a blob_id BLOB id
   * */
  RPC std::string LocalGetBlobName(BlobId blob_id);
  DEFINE_RPC(std::string, GetBlobName, 0,
             UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Lock the blob
   * */
  RPC bool LocalLockBlob(BlobId blob_id,
                         MdLockType lock_type);
  DEFINE_RPC(bool, LockBlob, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Unlock the blob
   * */
  RPC bool LocalUnlockBlob(BlobId blob_id,
                           MdLockType lock_type);
  DEFINE_RPC(bool, UnlockBlob, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Get \a blob_id blob's buffers
   * */
  RPC std::vector<BufferInfo> LocalGetBlobBuffers(BlobId blob_id);
  DEFINE_RPC(std::vector<BufferInfo>,
             GetBlobBuffers, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Rename \a blob_id blob to \a new_blob_name new blob name
   * in \a bkt_id bucket.
   * */
  RPC bool LocalRenameBlob(TagId bkt_id, BlobId blob_id,
                           hipc::charbuf &new_blob_name);
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
  DEFINE_RPC(bool, UpdateTargetCapacity, 0,
             UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Update the capacity of the target device
   * */
  RPC const hipc::vector<TargetInfo>& LocalGetTargetInfo() {
    return (*targets_);
  }

  /**
   * Get the TargetInfo for neighborhood
   * */
  hipc::vector<TargetInfo> GetNeighborhoodTargetInfo() {
    return {};
  }

  /**
   * Get all TargetInfo in the system
   * */
  hipc::vector<TargetInfo> GetGlobalTargetInfo() {
    return {};
  }

  /**====================================
   * Tag Operations
   * ===================================*/

  /**
   * Create a tag
   * */
  TagId LocalGetOrCreateTag(const std::string &tag_name,
                            bool owner,
                            std::vector<TraitId> &traits);
  DEFINE_RPC(TagId, GetOrCreateTag, 0, std::hash<std::string>{})

  /**
   * Get the id of a tag
   * */
  TagId LocalGetTagId(const std::string &tag_name);
  DEFINE_RPC(TagId, GetTagId, 0, std::hash<std::string>{})

  /**
   * Rename a tag
   * */
  RPC void LocalRenameTag(TagId tag, const std::string &new_name);
  DEFINE_RPC(void, RenameTag, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Delete a tag
   * */
  void LocalDestroyTag(TagId tag);
  DEFINE_RPC(void, DestroyTag, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA)

  /**
   * Add a blob to a tag index.
   * */
  Status LocalTagAddBlob(TagId tag_id,
                         BlobId blob_id);
  DEFINE_RPC(Status, TagAddBlob, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA);

  /**
   * Remove a blob from a tag index.
   * */
  Status LocalTagRemoveBlob(TagId tag_id,
                            BlobId blob_id);
  DEFINE_RPC(Status, TagRemoveBlob, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA);

  /**
   * Find all blobs pertaining to a tag
   * */
  std::vector<BlobId> LocalGroupByTag(TagId tag_id);
  DEFINE_RPC(std::vector<BlobId>, GroupByTag, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA);

  /**
   * Add a trait to a tag index. Create tag if it does not exist.
   * */
  RPC void LocalTagAddTrait(TagId tag_id, TraitId trait_id);
  DEFINE_RPC(void, TagAddTrait, 0, UNIQUE_ID_TO_NODE_ID_LAMBDA);

  /**
   * Find all traits pertaining to a tag
   * */
  hipc::slist<TraitInfo> GlobalTagGetTraits(TagId tag_id);

  /**====================================
   * Trait Operations
   * ===================================*/

  /**
   * Register a trait
   * */
  template<typename TraitT>
  RPC void LocalRegisterTrait(TraitT &trait) {
    TraitInfo info(HERMES_MEMORY_MANAGER->GetDefaultAllocator());
    (*info.trait_uuid_) = trait.GetUuid();
    info.header_->trait_params_ = trait.GetShmPointer();
    TraitId id(header_->id_alloc_.fetch_add(1));
    trait_id_map_->emplace(trait.uuid_, id);
    trait_map_->emplace(id, std::move(info));
  }

  /**
   * Register a trait globally
   * */
  template<typename TraitT>
  void GlobalRegisterTrait(TraitT &trait) {
    for (int i = 0; i < rpc_->hosts_.size(); ++i) {
      int node_id = i + 1;
      if (NODE_ID_IS_LOCAL(node_id)) {
        LocalRegisterTrait(trait);
      } else {
        rpc_->Call<void>("RpcRegister" + trait.GetName(), trait);
      }
    }
  }

  /**
   * Get the identity of a trait
   * */
  TraitId GlobalGetTraitId(const std::string &trait_uuid) {
    auto iter = trait_id_map_->find(hipc::string(trait_uuid));
    if (iter.is_end()) {
      return 0;
    }
    return *(*iter)->second_;
  }

  /**
   * Get the trait
   * */
  // Trait* GetTrait(TraitId trait_id);

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
    hipc::ShmRef<hipc::pair<MapFirst, MapSecond>> info = *iter;
    MapSecond &obj_info = *info->second_;
    switch (lock_type) {
      case MdLockType::kExternalRead: {
        obj_info.header_->lock_[1].ReadLock();
        return true;
      }
      case MdLockType::kExternalWrite: {
        obj_info.header_->lock_[1].WriteLock();
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
    hipc::ShmRef<hipc::pair<MapFirst, MapSecond>> info = *iter;
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
};

}  // namespace hermes

#endif  // HERMES_SRC_METADATA_MANAGER_H_
