//
// Created by lukemartinlogan on 12/4/22.
//

#ifndef HERMES_SRC_METADATA_MANAGER_H_
#define HERMES_SRC_METADATA_MANAGER_H_

#include "decorator.h"
#include "hermes_types.h"
#include "status.h"
#include "rpc.h"
#include "metadata_types.h"
#include "rpc_thallium_serialization.h"

namespace hermes {

/**
 * Type name simplification for the various map types
 * */
typedef lipc::unordered_map<lipc::charbuf, BlobId> BLOB_ID_MAP_T;
typedef lipc::unordered_map<lipc::charbuf, BucketId> BKT_ID_MAP_T;
typedef lipc::unordered_map<lipc::charbuf, VBucketId> VBKT_ID_MAP_T;
typedef lipc::unordered_map<BlobId, BlobInfoShmHeader> BLOB_MAP_T;
typedef lipc::unordered_map<BucketId, BucketInfoShmHeader> BKT_MAP_T;
typedef lipc::unordered_map<VBucketId, VBucketInfoShmHeader> VBKT_MAP_T;

/**
 * The SHM representation of the MetadataManager
 * */
struct MetadataManagerShmHeader {
  /// SHM representation of blob id map
  lipc::ShmArchive<lipc::mptr<BLOB_ID_MAP_T>> blob_id_map_ar_;
  /// SHM representation of bucket id map
  lipc::ShmArchive<lipc::mptr<BKT_ID_MAP_T>> bkt_id_map_ar_;
  /// SHM representation of vbucket id map
  lipc::ShmArchive<lipc::mptr<VBKT_ID_MAP_T>> vbkt_id_map_ar_;
  /// SHM representation of blob map
  lipc::ShmArchive<lipc::mptr<BLOB_MAP_T>> blob_map_ar_;
  /// SHM representation of bucket map
  lipc::ShmArchive<lipc::mptr<BKT_MAP_T>> bkt_map_ar_;
  /// SHM representation of vbucket map
  lipc::ShmArchive<lipc::mptr<VBKT_MAP_T>> vbkt_map_ar_;
  /// SHM representation of device vector
  lipc::ShmArchive<lipc::mptr<lipc::vector<DeviceInfo>>> devices_;
  /// SHM representation of target info vector
  lipc::ShmArchive<lipc::mptr<lipc::vector<TargetInfo>>> targets_;
  /// Used to create unique ids. Starts at 1.
  std::atomic<u64> id_alloc_;
};

/**
 * Manages the metadata for blobs, buckets, and vbuckets.
 * */
class MetadataManager {
 public:
  RPC_TYPE* rpc_;
  MetadataManagerShmHeader *header_;

  /**
   * The manual pointers representing the different map types.
   * */
  lipc::mptr<BLOB_ID_MAP_T> blob_id_map_;
  lipc::mptr<BKT_ID_MAP_T> bkt_id_map_;
  lipc::mptr<VBKT_ID_MAP_T> vbkt_id_map_;
  lipc::mptr<BLOB_MAP_T> blob_map_;
  lipc::mptr<BKT_MAP_T> bkt_map_;
  lipc::mptr<VBKT_MAP_T> vbkt_map_;

  /**
   * Information about targets and devices
   * */
  lipc::mptr<lipc::vector<DeviceInfo>> devices_;
  lipc::mptr<lipc::vector<TargetInfo>> targets_;

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
  lipc::charbuf CreateBlobName(BucketId bkt_id,
                               const lipc::charbuf &blob_name) {
    lipc::charbuf new_name(sizeof(bkt_id) + blob_name.size());
    size_t off = 0;
    memcpy(new_name.data_mutable() + off, &bkt_id, sizeof(BucketId));
    off += sizeof(BucketId);
    memcpy(new_name.data_mutable() + off, blob_name.data(), blob_name.size());
    return new_name;
  }

  /**
   * Get or create a bucket with \a bkt_name bucket name
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC BucketId LocalGetOrCreateBucket(lipc::charbuf &bkt_name);

  /**
   * Get the BucketId with \a bkt_name bucket name
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC BucketId LocalGetBucketId(lipc::charbuf &bkt_name);

  /**
   * Check whether or not \a bkt_id bucket contains
   * \a blob_id blob
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalBucketContainsBlob(BucketId bkt_id, BlobId blob_id);

  /**
   * Rename \a bkt_id bucket to \a new_bkt_name new name
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalRenameBucket(BucketId bkt_id, lipc::charbuf &new_bkt_name);

  /**
   * Destroy \a bkt_id bucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalDestroyBucket(BucketId bkt_id);

  /**
   * Put a blob in a bucket
   *
   * @param bkt_id id of the bucket
   * @param blob_name semantic blob name
   * @param data the data being placed
   * @param buffers the buffers to place data in
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC BlobId LocalBucketPutBlob(BucketId bkt_id, const lipc::charbuf &blob_name,
                                Blob &data, lipc::vector<BufferInfo> &buffers);

  /**
   * Get \a blob_name blob from \a bkt_id bucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC BlobId LocalGetBlobId(BucketId bkt_id, lipc::charbuf &blob_name);

  /**
   * Change \a blob_id blob's buffers to \the buffers
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalSetBlobBuffers(BlobId blob_id,
                               lipc::vector<BufferInfo> &buffers);

  /**
   * Get \a blob_id blob's buffers
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC lipc::vector<BufferInfo> LocalGetBlobBuffers(BlobId blob_id);

  /**
   * Rename \a blob_id blob to \a new_blob_name new blob name
   * in \a bkt_id bucket.
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalRenameBlob(BucketId bkt_id, BlobId blob_id,
                           lipc::charbuf &new_blob_name);

  /**
   * Destroy \a blob_id blob in \a bkt_id bucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalDestroyBlob(BucketId bkt_id, lipc::charbuf &blob_name);

  /**
   * Acquire \a blob_id blob's write lock
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalWriteLockBlob(BlobId blob_id);

  /**
   * Release \a blob_id blob's write lock
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalWriteUnlockBlob(BlobId blob_id);

  /**
   * Acquire \a blob_id blob's read lock
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalReadLockBlob(BlobId blob_id);

  /**
   * Release \a blob_id blob's read lock
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalReadUnlockBlob(BlobId blob_id);

  /**
   * Get or create \a vbkt_name VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC VBucketId LocalGetOrCreateVBucket(lipc::charbuf &vbkt_name);

  /**
   * Get the VBucketId of \a vbkt_name VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC VBucketId LocalGetVBucketId(lipc::charbuf &vbkt_name);

  /**
   * Link \a vbkt_id VBucketId
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC VBucketId LocalVBucketLinkBlob(VBucketId vbkt_id, BucketId bkt_id,
                                     lipc::charbuf &blob_name);

  /**
   * Unlink \a blob_name Blob of \a bkt_id Bucket
   * from \a vbkt_id VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC VBucketId LocalVBucketUnlinkBlob(VBucketId vbkt_id, BucketId bkt_id,
                                       lipc::charbuf &blob_name);

  /**
   * Get the linked blobs from \a vbkt_id VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC std::list<BlobId> LocalVBucketGetLinks(VBucketId vbkt_id);

  /**
   * Rename \a vbkt_id VBucket to \a new_vbkt_name name
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalRenameVBucket(VBucketId vbkt_id, lipc::charbuf &new_vbkt_name);

  /**
   * Destroy \a vbkt_id VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalDestroyVBucket(VBucketId vbkt_id);


  /**
   * Update the capacity of the target device
   * */
  RPC void LocalUpdateTargetCapacity(TargetId tid, off64_t offset) {
    auto &target = (*targets_)[tid.GetIndex()];
    target.rem_cap_ += offset;
  }

  /**
   * Update the capacity of the target device
   * */
  RPC const lipc::vector<TargetInfo>& LocalGetTargetInfo() {
    return (*targets_);
  }

  /**
   * Get the TargetInfo for neighborhood
   * */
  lipc::vector<TargetInfo> GetNeighborhoodTargetInfo() {
  }

  /**
   * Get all TargetInfo in the system
   * */
  lipc::vector<TargetInfo> GetGlobalTargetInfo() {
  }

 public:
  RPC_AUTOGEN_START
  BucketId GetOrCreateBucket(lipc::charbuf& bkt_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalGetOrCreateBucket(
        bkt_name);
    } else {
      return rpc_->Call<BucketId>(
        target_node, "GetOrCreateBucket",
        bkt_name);
    }
  }
  BucketId GetBucketId(lipc::charbuf& bkt_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalGetBucketId(
        bkt_name);
    } else {
      return rpc_->Call<BucketId>(
        target_node, "GetBucketId",
        bkt_name);
    }
  }
  bool BucketContainsBlob(BucketId bkt_id, BlobId blob_id) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalBucketContainsBlob(
        bkt_id, blob_id);
    } else {
      return rpc_->Call<bool>(
        target_node, "BucketContainsBlob",
        bkt_id, blob_id);
    }
  }
  bool RenameBucket(BucketId bkt_id, lipc::charbuf& new_bkt_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalRenameBucket(
        bkt_id, new_bkt_name);
    } else {
      return rpc_->Call<bool>(
        target_node, "RenameBucket",
        bkt_id, new_bkt_name);
    }
  }
  bool DestroyBucket(BucketId bkt_id) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalDestroyBucket(
        bkt_id);
    } else {
      return rpc_->Call<bool>(
        target_node, "DestroyBucket",
        bkt_id);
    }
  }
  BlobId BucketPutBlob(BucketId bkt_id, lipc::charbuf& blob_name, Blob& data, lipc::vector<BufferInfo>& buffers) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalBucketPutBlob(
        bkt_id, blob_name, data, buffers);
    } else {
      return rpc_->Call<BlobId>(
        target_node, "BucketPutBlob",
        bkt_id, blob_name, data, buffers);
    }
  }
  BlobId GetBlobId(BucketId bkt_id, lipc::charbuf& blob_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalGetBlobId(
        bkt_id, blob_name);
    } else {
      return rpc_->Call<BlobId>(
        target_node, "GetBlobId",
        bkt_id, blob_name);
    }
  }
  bool SetBlobBuffers(BlobId blob_id, lipc::vector<BufferInfo>& buffers) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalSetBlobBuffers(
        blob_id, buffers);
    } else {
      return rpc_->Call<bool>(
        target_node, "SetBlobBuffers",
        blob_id, buffers);
    }
  }
  lipc::vector<BufferInfo> GetBlobBuffers(BlobId blob_id) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalGetBlobBuffers(
        blob_id);
    } else {
      return rpc_->Call<lipc::vector<BufferInfo>>(
        target_node, "GetBlobBuffers",
        blob_id);
    }
  }
  bool RenameBlob(BucketId bkt_id, BlobId blob_id, lipc::charbuf& new_blob_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalRenameBlob(
        bkt_id, blob_id, new_blob_name);
    } else {
      return rpc_->Call<bool>(
        target_node, "RenameBlob",
        bkt_id, blob_id, new_blob_name);
    }
  }
  bool DestroyBlob(BucketId bkt_id, lipc::charbuf& blob_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalDestroyBlob(
        bkt_id, blob_name);
    } else {
      return rpc_->Call<bool>(
        target_node, "DestroyBlob",
        bkt_id, blob_name);
    }
  }
  bool WriteLockBlob(BlobId blob_id) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalWriteLockBlob(
        blob_id);
    } else {
      return rpc_->Call<bool>(
        target_node, "WriteLockBlob",
        blob_id);
    }
  }
  bool WriteUnlockBlob(BlobId blob_id) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalWriteUnlockBlob(
        blob_id);
    } else {
      return rpc_->Call<bool>(
        target_node, "WriteUnlockBlob",
        blob_id);
    }
  }
  bool ReadLockBlob(BlobId blob_id) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalReadLockBlob(
        blob_id);
    } else {
      return rpc_->Call<bool>(
        target_node, "ReadLockBlob",
        blob_id);
    }
  }
  bool ReadUnlockBlob(BlobId blob_id) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalReadUnlockBlob(
        blob_id);
    } else {
      return rpc_->Call<bool>(
        target_node, "ReadUnlockBlob",
        blob_id);
    }
  }
  VBucketId GetOrCreateVBucket(lipc::charbuf& vbkt_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalGetOrCreateVBucket(
        vbkt_name);
    } else {
      return rpc_->Call<VBucketId>(
        target_node, "GetOrCreateVBucket",
        vbkt_name);
    }
  }
  VBucketId GetVBucketId(lipc::charbuf& vbkt_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalGetVBucketId(
        vbkt_name);
    } else {
      return rpc_->Call<VBucketId>(
        target_node, "GetVBucketId",
        vbkt_name);
    }
  }
  VBucketId VBucketLinkBlob(VBucketId vbkt_id, BucketId bkt_id, lipc::charbuf& blob_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalVBucketLinkBlob(
        vbkt_id, bkt_id, blob_name);
    } else {
      return rpc_->Call<VBucketId>(
        target_node, "VBucketLinkBlob",
        vbkt_id, bkt_id, blob_name);
    }
  }
  VBucketId VBucketUnlinkBlob(VBucketId vbkt_id, BucketId bkt_id, lipc::charbuf& blob_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalVBucketUnlinkBlob(
        vbkt_id, bkt_id, blob_name);
    } else {
      return rpc_->Call<VBucketId>(
        target_node, "VBucketUnlinkBlob",
        vbkt_id, bkt_id, blob_name);
    }
  }
  std::list<BlobId> VBucketGetLinks(VBucketId vbkt_id) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalVBucketGetLinks(
        vbkt_id);
    } else {
      return rpc_->Call<std::list<BlobId>>(
        target_node, "VBucketGetLinks",
        vbkt_id);
    }
  }
  bool RenameVBucket(VBucketId vbkt_id, lipc::charbuf& new_vbkt_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalRenameVBucket(
        vbkt_id, new_vbkt_name);
    } else {
      return rpc_->Call<bool>(
        target_node, "RenameVBucket",
        vbkt_id, new_vbkt_name);
    }
  }
  bool DestroyVBucket(VBucketId vbkt_id) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalDestroyVBucket(
        vbkt_id);
    } else {
      return rpc_->Call<bool>(
        target_node, "DestroyVBucket",
        vbkt_id);
    }
  }
  RPC_AUTOGEN_END
};

}  // namespace hermes

#endif  // HERMES_SRC_METADATA_MANAGER_H_