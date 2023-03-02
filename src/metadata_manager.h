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
#include "adapter/io_client/io_client_factory.h"

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
typedef hipc::unordered_map<hipc::charbuf, BucketId> BKT_ID_MAP_T;
typedef hipc::unordered_map<hipc::charbuf, VBucketId> VBKT_ID_MAP_T;
typedef hipc::unordered_map<BlobId, BlobInfo> BLOB_MAP_T;
typedef hipc::unordered_map<BucketId, BucketInfo> BKT_MAP_T;
typedef hipc::unordered_map<VBucketId, VBucketInfo> VBKT_MAP_T;

/**
 * The SHM representation of the MetadataManager
 * */
struct MetadataManagerShmHeader {
  /// SHM representation of blob id map
  hipc::TypedPointer<hipc::mptr<BLOB_ID_MAP_T>> blob_id_map_ar_;
  /// SHM representation of bucket id map
  hipc::TypedPointer<hipc::mptr<BKT_ID_MAP_T>> bkt_id_map_ar_;
  /// SHM representation of vbucket id map
  hipc::TypedPointer<hipc::mptr<VBKT_ID_MAP_T>> vbkt_id_map_ar_;
  /// SHM representation of blob map
  hipc::TypedPointer<hipc::mptr<BLOB_MAP_T>> blob_map_ar_;
  /// SHM representation of bucket map
  hipc::TypedPointer<hipc::mptr<BKT_MAP_T>> bkt_map_ar_;
  /// SHM representation of vbucket map
  hipc::TypedPointer<hipc::mptr<VBKT_MAP_T>> vbkt_map_ar_;
  /// SHM representation of device vector
  hipc::TypedPointer<hipc::mptr<hipc::vector<DeviceInfo>>> devices_;
  /// SHM representation of target info vector
  hipc::TypedPointer<hipc::mptr<hipc::vector<TargetInfo>>> targets_;
  /// Used to create unique ids. Starts at 1.
  std::atomic<u64> id_alloc_;
  /// Synchronization
  RwLock lock_;
};

/**
 * Manages the metadata for blobs, buckets, and vbuckets.
 * */
class MetadataManager {
 public:
  RPC_TYPE *rpc_;
  MetadataManagerShmHeader *header_;
  BufferOrganizer *borg_;

  /**
   * The manual pointers representing the different map types.
   * */
  hipc::mptr<BLOB_ID_MAP_T> blob_id_map_;
  hipc::mptr<BKT_ID_MAP_T> bkt_id_map_;
  hipc::mptr<VBKT_ID_MAP_T> vbkt_id_map_;
  hipc::mptr<BLOB_MAP_T> blob_map_;
  hipc::mptr<BKT_MAP_T> bkt_map_;
  hipc::mptr<VBKT_MAP_T> vbkt_map_;

  /**
   * Information about targets and devices
   * */
  hipc::mptr<hipc::vector<DeviceInfo>> devices_;
  hipc::mptr<hipc::vector<TargetInfo>> targets_;

  /** A global lock for simplifying MD management */
  RwLock lock_;

 public:
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
    * Create a unique blob name using BucketId
    * */
  static hipc::charbuf CreateBlobName(BucketId bkt_id,
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

  /**
   * Get or create a bucket with \a bkt_name bucket name
   * */
  RPC BucketId LocalGetOrCreateBucket(hipc::charbuf &bkt_name,
                                      const IoClientContext &opts);
  DEFINE_RPC(BucketId, GetOrCreateBucket, 0, std::hash<hipc::charbuf>{})

  /**
   * Get the BucketId with \a bkt_name bucket name
   * */
  RPC BucketId LocalGetBucketId(hipc::charbuf &bkt_name);
  DEFINE_RPC(BucketId, GetBucketId, 0, std::hash<hipc::charbuf>{})

  /**
   * Get the size of a bucket (depends on the IoClient used).
   * */
  RPC size_t LocalGetBucketSize(BucketId bkt_id,
                                const IoClientContext &opts);
  DEFINE_RPC(size_t, GetBucketSize, 0,
             pack.template Get<0>.GetNodeId())

  /**
   * Lock the bucket
   * */
  RPC void LocalLockBucket(BucketId bkt_id,
                           MdLockType lock_type);
  DEFINE_RPC(void, LockBucket, 0,
             pack.template Get<0>.GetNodeId())

  /**
   * Unlock the bucket
   * */
  RPC void LocalUnlockBucket(BucketId bkt_id,
                             MdLockType lock_type);
  DEFINE_RPC(void, UnlockBucket, 0,
             pack.template Get<0>.GetNodeId())

  /**
   * Check whether or not \a bkt_id bucket contains
   * \a blob_id blob
   * */
  RPC bool LocalBucketContainsBlob(BucketId bkt_id, BlobId blob_id);
  DEFINE_RPC(void, BucketContainsBlob, 1,
             pack.template Get<0>.GetNodeId())

  /**
   * Get the set of all blobs contained in \a bkt_id BUCKET
   * */
  RPC std::vector<BlobId> LocalBucketGetContainedBlobIds(BucketId bkt_id);
  DEFINE_RPC(std::vector<BlobId>,
      BucketGetContainedBlobIds, 0,
             pack.template Get<0>.GetNodeId())

  /**
   * Rename \a bkt_id bucket to \a new_bkt_name new name
   * */
  RPC bool LocalRenameBucket(BucketId bkt_id,
                             hipc::charbuf &new_bkt_name);
  DEFINE_RPC(bool, RenameBucket, 0,
             pack.template Get<0>.GetNodeId())

  /**
   * Destroy \a bkt_id bucket
   * */
  RPC bool LocalDestroyBucket(BucketId bkt_id);
  DEFINE_RPC(bool, DestroyBucket, 0,
             pack.template Get<0>.GetNodeId())

  /**
   * Put a blob in a bucket
   *
   * @param bkt_id id of the bucket
   * @param blob_name semantic blob name
   * @param data the data being placed
   * @param buffers the buffers to place data in
   * */
  RPC std::tuple<BlobId, bool, size_t> LocalBucketPutBlob(
      BucketId bkt_id,
      const hipc::charbuf &blob_name,
      size_t blob_size,
      hipc::vector<BufferInfo> &buffers);
  DEFINE_RPC((std::tuple<BlobId, bool, size_t>),
             BucketPutBlob, 0,
             pack.template Get<0>.GetNodeId())

  /**
   * Registers the existence of a Blob with the Bucket. Required for
   * deletion and statistics.
   * */
  Status LocalBucketRegisterBlobId(BucketId bkt_id,
                                   BlobId blob_id,
                                   size_t orig_blob_size,
                                   size_t new_blob_size,
                                   bool did_create,
                                   const IoClientContext &opts);
  DEFINE_RPC(Status, BucketRegisterBlobId, 0,
             pack.template Get<0>.GetNodeId())

  /**
   * Registers the existence of a Blob with the Bucket. Required for
   * deletion and statistics.
   * */
  Status LocalBucketUnregisterBlobId(BucketId bkt_id,
                                     BlobId blob_id,
                                     const IoClientContext &opts);
  DEFINE_RPC(Status, BucketUnregisterBlobId, 0,
             pack.template Get<0>.GetNodeId())

  /**
   * Creates the blob metadata
   *
   * @param bkt_id id of the bucket
   * @param blob_name semantic blob name
   * */
  std::pair<BlobId, bool> LocalBucketTryCreateBlob(
      BucketId bkt_id,
      const hipc::charbuf &blob_name);
  DEFINE_RPC((std::pair<BlobId, bool>),
             BucketTryCreateBlob, 0,
             pack.template Get<0>.GetNodeId())

  /**
   * Get a blob from a bucket
   *
   * @param bkt_id id of the bucket
   * @param blob_id id of the blob to get
   * */
  RPC Blob LocalBucketGetBlob(BlobId blob_id);
  DEFINE_RPC(Blob, BucketGetBlob, 0,
             pack.template Get<0>.GetNodeId())

  /**
   * Get \a blob_name BLOB from \a bkt_id bucket
   * */
  RPC BlobId LocalGetBlobId(BucketId bkt_id, const hipc::charbuf &blob_name);
  DEFINE_RPC(BlobId, GetBlobId, 0,
             pack.template Get<0>.GetNodeId())

  /**
   * Get \a blob_name BLOB name from \a blob_id BLOB id
   * */
  RPC std::string LocalGetBlobName(BlobId blob_id);
  DEFINE_RPC(std::string, GetBlobName, 0, 
             pack.template Get<0>.GetNodeId())

  /**
   * Lock the blob
   * */
  RPC bool LocalLockBlob(BlobId blob_id,
                         MdLockType lock_type);
  DEFINE_RPC(bool, LockBlob, 0, pack.template Get<0>.GetNodeId())

  /**
   * Unlock the blob
   * */
  RPC bool LocalUnlockBlob(BlobId blob_id,
                           MdLockType lock_type);
  DEFINE_RPC(bool, UnlockBlob, 0, pack.template Get<0>.GetNodeId())

  /**
   * Get \a blob_id blob's buffers
   * */
  RPC std::vector<BufferInfo> LocalGetBlobBuffers(BlobId blob_id);
  DEFINE_RPC(std::vector<BufferInfo>,
             GetBlobBuffers, 0, pack.template Get<0>.GetNodeId())

  /**
   * Rename \a blob_id blob to \a new_blob_name new blob name
   * in \a bkt_id bucket.
   * */
  RPC bool LocalRenameBlob(BucketId bkt_id, BlobId blob_id,
                           hipc::charbuf &new_blob_name);
  DEFINE_RPC(bool, RenameBlob, 0, pack.template Get<1>.GetNodeId())

  /**
   * Destroy \a blob_id blob in \a bkt_id bucket
   * */
  RPC bool LocalDestroyBlob(BucketId bkt_id, BlobId blob_id);
  DEFINE_RPC(bool, DestroyBlob, 0, 
             pack.template Get<1>.GetNodeId())

  /**
   * Get or create \a vbkt_name VBucket
   * */
  RPC VBucketId LocalGetOrCreateVBucket(hipc::charbuf &vbkt_name,
                                        const IoClientContext &opts);
  DEFINE_RPC(bool, GetOrCreateVBucket, 0, std::hash<hipc::charbuf>{})

  /**
   * Get the VBucketId of \a vbkt_name VBucket
   * */
  RPC VBucketId LocalGetVBucketId(hipc::charbuf &vbkt_name);
  DEFINE_RPC(VBucketId, GetVBucketId, 0, std::hash<hipc::charbuf>{})

  /**
   * Link \a vbkt_id VBucketId
   * */
  RPC bool LocalVBucketLinkBlob(VBucketId vbkt_id, BlobId blob_id);
  DEFINE_RPC(bool, VBucketLinkBlob, 0, pack.template Get<0>.GetNodeId())

  /**
   * Unlink \a blob_name Blob of \a bkt_id Bucket
   * from \a vbkt_id VBucket
   * */
  RPC bool LocalVBucketUnlinkBlob(VBucketId vbkt_id, BlobId blob_id);
  DEFINE_RPC(bool, VBucketUnlinkBlob, 0, pack.template Get<0>.GetNodeId())

  /**
   * Get the linked blobs from \a vbkt_id VBucket
   * */
  RPC std::list<BlobId> LocalVBucketGetLinks(VBucketId vbkt_id);
  DEFINE_RPC(std::list<BlobId>,
             VBucketGetLinks, 0, pack.template Get<0>.GetNodeId())

  /**
   * Whether \a vbkt_id VBucket contains \a blob_id blob
   * */
  RPC bool LocalVBucketContainsBlob(VBucketId vbkt_id, BlobId blob_id);
  DEFINE_RPC(bool, VBucketContainsBlob, 0, pack.template Get<0>.GetNodeId())

  /**
   * Rename \a vbkt_id VBucket to \a new_vbkt_name name
   * */
  RPC bool LocalRenameVBucket(VBucketId vbkt_id, hipc::charbuf &new_vbkt_name);
  DEFINE_RPC(bool, RenameVBucket, 0, pack.template Get<0>.GetNodeId())

  /**
   * Destroy \a vbkt_id VBucket
   * */
  RPC bool LocalDestroyVBucket(VBucketId vbkt_id);
  DEFINE_RPC(bool, DestroyVBucket, 0, 
             pack.template Get<0>.GetNodeId())

  /**
   * Update the capacity of the target device
   * */
  RPC void LocalUpdateTargetCapacity(TargetId tid, off64_t offset) {
    TargetInfo &target = *(*targets_)[tid.GetIndex()];
    target.rem_cap_ += offset;
  }
  DEFINE_RPC(bool, UpdateTargetCapacity, 0, 
             pack.template Get<0>.GetNodeId())

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

 private:
  /** Acquire the external lock to Bucket or Blob */
  template<typename MapFirst, typename MapSecond, typename IdT>
  bool LockMdObject(hipc::unordered_map<MapFirst, MapSecond> &map,
                    IdT id,
                    MdLockType lock_type) {
    ScopedRwReadLock md_lock(lock_);
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
    ScopedRwReadLock md_lock(lock_);
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
