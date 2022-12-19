//
// Created by lukemartinlogan on 12/4/22.
//

#ifndef HERMES_SRC_METADATA_MANAGER_H_
#define HERMES_SRC_METADATA_MANAGER_H_

#include "decorator.h"
#include "hermes_types.h"
#include "hermes_status.h"
#include "rpc.h"
#include "metadata_types.h"
#include "rpc_thallium_serialization.h"

namespace hermes {

/**
 * Type name simplification for the various map types
 * */
typedef lipc::unordered_map<lipc::charbuf, BlobID> BLOB_ID_MAP_T;
typedef lipc::unordered_map<lipc::charbuf, BucketID> BKT_ID_MAP_T;
typedef lipc::unordered_map<lipc::charbuf, VBucketID> VBKT_ID_MAP_T;
typedef lipc::unordered_map<BlobID, BlobInfoShmHeader> BLOB_MAP_T;
typedef lipc::unordered_map<BucketID, BucketInfoShmHeader> BKT_MAP_T;
typedef lipc::unordered_map<VBucketID, VBucketInfoShmHeader> VBKT_MAP_T;

/**
 * The SHM representation of the MetadataManager
 * */
struct MetadataManagerShmHeader {
  /// SHM representation of blob id map
  lipc::ShmArchive<lipc::uptr<BLOB_ID_MAP_T>> blob_id_map_ar_;
  /// SHM representation of bucket id map
  lipc::ShmArchive<lipc::uptr<BKT_ID_MAP_T>> bkt_id_map_ar_;
  /// SHM representation of vbucket id map
  lipc::ShmArchive<lipc::uptr<VBKT_ID_MAP_T>> vbkt_id_map_ar_;
  /// SHM representation of blob map
  lipc::ShmArchive<lipc::uptr<BLOB_MAP_T>> blob_map_ar_;
  /// SHM representation of bucket map
  lipc::ShmArchive<lipc::uptr<BKT_MAP_T>> bkt_map_ar_;
  /// SHM representation of vbucket map
  lipc::ShmArchive<lipc::uptr<VBKT_MAP_T>> vbkt_map_ar_;
  /// Used to create unique ids. Starts at 1.
  std::atomic<u64> id_alloc_;
};

/**
 * Manages the metadata for blobs, buckets, and vbuckets.
 * */
class MetadataManager {
 private:
  RPC_TYPE* rpc_;
  MetadataManagerShmHeader *header_;

  /**
   * The unique pointers representing the different map types.
   * They are created in shm_init. They are destroyed automatically when the
   * MetadataManager (on the Hermes core) is destroyed. This avoids having
   * to manually "delete" the MetadataManager.
   * */
  lipc::uptr<BLOB_ID_MAP_T> blob_id_map_owner_;
  lipc::uptr<BKT_ID_MAP_T> bkt_id_map_owner_;
  lipc::uptr<VBKT_ID_MAP_T> vbkt_id_map_owner_;
  lipc::uptr<BLOB_MAP_T> blob_map_owner_;
  lipc::uptr<BKT_MAP_T> bkt_map_owner_;
  lipc::uptr<VBKT_MAP_T> vbkt_map_owner_;

  /**
   * The manual pointers representing the different map types.
   * These are references to the objects stored in lipc::uptr
   * objects above.
   * */
  lipc::mptr<BLOB_ID_MAP_T> blob_id_map_;
  lipc::mptr<BKT_ID_MAP_T> bkt_id_map_;
  lipc::mptr<VBKT_ID_MAP_T> vbkt_id_map_;
  lipc::mptr<BLOB_MAP_T> blob_map_;
  lipc::mptr<BKT_MAP_T> bkt_map_;
  lipc::mptr<VBKT_MAP_T> vbkt_map_;

 public:
  MetadataManager() = default;

  /**
   * Explicitly initialize the MetadataManager
   * */
  void shm_init(MetadataManagerShmHeader *header);

  /**
   * Store the MetadataManager in shared memory.
   * */
  void shm_serialize();

   /**
    * Unload the MetadtaManager from shared memory
    * */
  void shm_deserialize(MetadataManagerShmHeader *header);

   /**
    * Create a unique blob name using BucketID
    * */
  lipc::charbuf CreateBlobName(BucketID bkt_id, lipc::charbuf &blob_name) {
    lipc::charbuf new_name(sizeof(bkt_id) + blob_name.size());
    size_t off = 0;
    memcpy(new_name.data_mutable() + off, &bkt_id, sizeof(BucketID));
    off += sizeof(BucketID);
    memcpy(blob_name.data_mutable() + off, blob_name.data(), blob_name.size());
    return new_name;
  }

  /**
   * Get or create a bucket with \a bkt_name bucket name
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC BucketID LocalGetOrCreateBucket(lipc::charbuf &bkt_name);

  /**
   * Get the BucketID with \a bkt_name bucket name
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC BucketID LocalGetBucketId(lipc::charbuf &bkt_name);

  /**
   * Check whether or not \a bkt_id bucket contains
   * \a blob_id blob
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalBucketContainsBlob(BucketID bkt_id, BlobID blob_id);

  /**
   * Rename \a bkt_id bucket to \a new_bkt_name new name
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalRenameBucket(BucketID bkt_id, lipc::charbuf &new_bkt_name);

  /**
   * Destroy \a bkt_id bucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalDestroyBucket(BucketID bkt_id);

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
  RPC BlobID LocalBucketPutBlob(BucketID bkt_id, lipc::charbuf &blob_name,
                                Blob &data, lipc::vector<BufferInfo> &buffers);

  /**
   * Get \a blob_name blob from \a bkt_id bucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC BlobID LocalGetBlobId(BucketID bkt_id, lipc::charbuf &blob_name);

  /**
   * Change \a blob_id blob's buffers to \the buffers
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalSetBlobBuffers(BlobID blob_id,
                               lipc::vector<BufferInfo> &buffers);

  /**
   * Get \a blob_id blob's buffers
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC lipc::vector<BufferInfo> LocalGetBlobBuffers(BlobID blob_id);

  /**
   * Rename \a blob_id blob to \a new_blob_name new blob name
   * in \a bkt_id bucket.
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalRenameBlob(BucketID bkt_id, BlobID blob_id,
                           lipc::charbuf &new_blob_name);

  /**
   * Destroy \a blob_id blob in \a bkt_id bucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalDestroyBlob(BucketID bkt_id, lipc::charbuf &blob_name);

  /**
   * Acquire \a blob_id blob's write lock
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalWriteLockBlob(BlobID blob_id);

  /**
   * Release \a blob_id blob's write lock
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalWriteUnlockBlob(BlobID blob_id);

  /**
   * Acquire \a blob_id blob's read lock
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalReadLockBlob(BlobID blob_id);

  /**
   * Release \a blob_id blob's read lock
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalReadUnlockBlob(BlobID blob_id);

  /**
   * Get or create \a vbkt_name VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC VBucketID LocalGetOrCreateVBucket(lipc::charbuf &vbkt_name);

  /**
   * Get the VBucketID of \a vbkt_name VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC VBucketID LocalGetVBucketId(lipc::charbuf &vbkt_name);

  /**
   * Link \a vbkt_id VBucketID
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC VBucketID LocalVBucketLinkBlob(VBucketID vbkt_id, BucketID bkt_id,
                                     lipc::charbuf &blob_name);

  /**
   * Unlink \a blob_name Blob of \a bkt_id Bucket
   * from \a vbkt_id VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC VBucketID LocalVBucketUnlinkBlob(VBucketID vbkt_id, BucketID bkt_id,
                                       lipc::charbuf &blob_name);

  /**
   * Get the linked blobs from \a vbkt_id VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC std::list<BlobID> LocalVBucketGetLinks(VBucketID vbkt_id);

  /**
   * Rename \a vbkt_id VBucket to \a new_vbkt_name name
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalRenameVBucket(VBucketID vbkt_id, lipc::charbuf &new_vbkt_name);

  /**
   * Destroy \a vbkt_id VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalDestroyVBucket(VBucketID vbkt_id);

 public:
  RPC_AUTOGEN_START
  BucketID GetOrCreateBucket(lipc::charbuf& bkt_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalGetOrCreateBucket(
        bkt_name);
    } else {
      return rpc_->Call<BucketID>(
        target_node, "GetOrCreateBucket",
        bkt_name);
    }
  }
  BucketID GetBucketId(lipc::charbuf& bkt_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalGetBucketId(
        bkt_name);
    } else {
      return rpc_->Call<BucketID>(
        target_node, "GetBucketId",
        bkt_name);
    }
  }
  bool BucketContainsBlob(BucketID bkt_id, BlobID blob_id) {
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
  bool RenameBucket(BucketID bkt_id, lipc::charbuf& new_bkt_name) {
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
  bool DestroyBucket(BucketID bkt_id) {
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
  BlobID BucketPutBlob(BucketID bkt_id, lipc::charbuf& blob_name, Blob& data, lipc::vector<BufferInfo>& buffers) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalBucketPutBlob(
        bkt_id, blob_name, data, buffers);
    } else {
      return rpc_->Call<BlobID>(
        target_node, "BucketPutBlob",
        bkt_id, blob_name, data, buffers);
    }
  }
  BlobID GetBlobId(BucketID bkt_id, lipc::charbuf& blob_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalGetBlobId(
        bkt_id, blob_name);
    } else {
      return rpc_->Call<BlobID>(
        target_node, "GetBlobId",
        bkt_id, blob_name);
    }
  }
  bool SetBlobBuffers(BlobID blob_id, lipc::vector<BufferInfo>& buffers) {
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
  lipc::vector<BufferInfo> GetBlobBuffers(BlobID blob_id) {
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
  bool RenameBlob(BucketID bkt_id, BlobID blob_id, lipc::charbuf& new_blob_name) {
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
  bool DestroyBlob(BucketID bkt_id, lipc::charbuf& blob_name) {
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
  bool WriteLockBlob(BlobID blob_id) {
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
  bool WriteUnlockBlob(BlobID blob_id) {
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
  bool ReadLockBlob(BlobID blob_id) {
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
  bool ReadUnlockBlob(BlobID blob_id) {
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
  VBucketID GetOrCreateVBucket(lipc::charbuf& vbkt_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalGetOrCreateVBucket(
        vbkt_name);
    } else {
      return rpc_->Call<VBucketID>(
        target_node, "GetOrCreateVBucket",
        vbkt_name);
    }
  }
  VBucketID GetVBucketId(lipc::charbuf& vbkt_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalGetVBucketId(
        vbkt_name);
    } else {
      return rpc_->Call<VBucketID>(
        target_node, "GetVBucketId",
        vbkt_name);
    }
  }
  VBucketID VBucketLinkBlob(VBucketID vbkt_id, BucketID bkt_id, lipc::charbuf& blob_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalVBucketLinkBlob(
        vbkt_id, bkt_id, blob_name);
    } else {
      return rpc_->Call<VBucketID>(
        target_node, "VBucketLinkBlob",
        vbkt_id, bkt_id, blob_name);
    }
  }
  VBucketID VBucketUnlinkBlob(VBucketID vbkt_id, BucketID bkt_id, lipc::charbuf& blob_name) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalVBucketUnlinkBlob(
        vbkt_id, bkt_id, blob_name);
    } else {
      return rpc_->Call<VBucketID>(
        target_node, "VBucketUnlinkBlob",
        vbkt_id, bkt_id, blob_name);
    }
  }
  std::list<BlobID> VBucketGetLinks(VBucketID vbkt_id) {
    u32 target_node = rpc_->node_id_;
    if (target_node == rpc_->node_id_) {
      return LocalVBucketGetLinks(
        vbkt_id);
    } else {
      return rpc_->Call<std::list<BlobID>>(
        target_node, "VBucketGetLinks",
        vbkt_id);
    }
  }
  bool RenameVBucket(VBucketID vbkt_id, lipc::charbuf& new_vbkt_name) {
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
  bool DestroyVBucket(VBucketID vbkt_id) {
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