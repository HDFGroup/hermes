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
#include "rpc_thallium_serialization.h"
#include "adapter/io_client/io_client_factory.h"

namespace hermes {

/** Namespace simplification for AdapterFactory */
using hermes::adapter::IoClientFactory;

/** Namespace simplification for IoClientContext */
using hermes::adapter::IoClientContext;

/** Namespace simplification for AbstractAdapter */
using hermes::adapter::IoClientOptions;

/** Forward declaration of borg */
class BufferOrganizer;

/**
 * Type name simplification for the various map types
 * */
typedef lipc::unordered_map<lipc::charbuf, BlobId> BLOB_ID_MAP_T;
typedef lipc::unordered_map<lipc::charbuf, BucketId> BKT_ID_MAP_T;
typedef lipc::unordered_map<lipc::charbuf, VBucketId> VBKT_ID_MAP_T;
typedef lipc::unordered_map<BlobId, BlobInfo> BLOB_MAP_T;
typedef lipc::unordered_map<BucketId, BucketInfo> BKT_MAP_T;
typedef lipc::unordered_map<VBucketId, VBucketInfo> VBKT_MAP_T;

/**
 * The SHM representation of the MetadataManager
 * */
struct MetadataManagerShmHeader {
  /// SHM representation of blob id map
  lipc::TypedPointer<lipc::mptr<BLOB_ID_MAP_T>> blob_id_map_ar_;
  /// SHM representation of bucket id map
  lipc::TypedPointer<lipc::mptr<BKT_ID_MAP_T>> bkt_id_map_ar_;
  /// SHM representation of vbucket id map
  lipc::TypedPointer<lipc::mptr<VBKT_ID_MAP_T>> vbkt_id_map_ar_;
  /// SHM representation of blob map
  lipc::TypedPointer<lipc::mptr<BLOB_MAP_T>> blob_map_ar_;
  /// SHM representation of bucket map
  lipc::TypedPointer<lipc::mptr<BKT_MAP_T>> bkt_map_ar_;
  /// SHM representation of vbucket map
  lipc::TypedPointer<lipc::mptr<VBKT_MAP_T>> vbkt_map_ar_;
  /// SHM representation of device vector
  lipc::TypedPointer<lipc::mptr<lipc::vector<DeviceInfo>>> devices_;
  /// SHM representation of target info vector
  lipc::TypedPointer<lipc::mptr<lipc::vector<TargetInfo>>> targets_;
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
  BufferOrganizer *borg_;

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
  RPC BucketId LocalGetOrCreateBucket(lipc::charbuf &bkt_name,
                                      const IoClientOptions &opts);

  /**
   * Get the BucketId with \a bkt_name bucket name
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC BucketId LocalGetBucketId(lipc::charbuf &bkt_name);

  /**
   * Get the size of a bucket (depends on the IoClient used).
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC size_t LocalGetBucketSize(BucketId bkt_id,
                                const IoClientOptions &opts);

  /**
   * Check whether or not \a bkt_id bucket contains
   * \a blob_id blob
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  bool LocalBucketContainsBlob(BucketId bkt_id, BlobId blob_id);

  /**
   * Rename \a bkt_id bucket to \a new_bkt_name new name
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalRenameBucket(BucketId bkt_id,
                             lipc::charbuf &new_bkt_name);

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
  RPC std::tuple<BlobId, bool, size_t> LocalBucketPutBlob(
      BucketId bkt_id,
      const lipc::charbuf &blob_name,
      size_t blob_size,
      lipc::vector<BufferInfo> &buffers);

  /**
   * Registers the existence of a Blob with the Bucket. Required for
   * deletion and statistics.
   * */
  Status LocalBucketRegisterBlobId(BucketId bkt_id,
                                   BlobId blob_id,
                                   size_t orig_blob_size,
                                   size_t new_blob_size,
                                   bool did_create,
                                   const IoClientOptions &opts);

  /**
   * Registers the existence of a Blob with the Bucket. Required for
   * deletion and statistics.
   * */
  Status LocalBucketUnregisterBlobId(BucketId bkt_id,
                                     BlobId blob_id,
                                     const IoClientContext &io_ctx);

  /**
   * Get a blob from a bucket
   *
   * @param bkt_id id of the bucket
   * @param blob_id id of the blob to get
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC Blob LocalBucketGetBlob(BlobId blob_id);

  /**
   * Get \a blob_name blob from \a bkt_id bucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC BlobId LocalGetBlobId(BucketId bkt_id, lipc::charbuf &blob_name);

  /**
   * Get \a blob_id blob's buffers
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC std::vector<BufferInfo> LocalGetBlobBuffers(BlobId blob_id);

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
  RPC bool LocalDestroyBlob(BucketId bkt_id, BlobId blob_id);

  /**
   * Get or create \a vbkt_name VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC VBucketId LocalGetOrCreateVBucket(lipc::charbuf &vbkt_name,
                                        const IoClientOptions &opts);

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
  RPC bool LocalVBucketLinkBlob(VBucketId vbkt_id, BlobId blob_id);

  /**
   * Unlink \a blob_name Blob of \a bkt_id Bucket
   * from \a vbkt_id VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalVBucketUnlinkBlob(VBucketId vbkt_id, BlobId blob_id);

  /**
   * Get the linked blobs from \a vbkt_id VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC std::list<BlobId> LocalVBucketGetLinks(VBucketId vbkt_id);

  /**
   * Whether \a vbkt_id VBucket contains \a blob_id blob
   * */
  RPC bool LocalVBucketContainsBlob(VBucketId vbkt_id, BlobId blob_id);

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
    TargetInfo &target = *(*targets_)[tid.GetIndex()];
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
    return {};
  }

  /**
   * Get all TargetInfo in the system
   * */
  lipc::vector<TargetInfo> GetGlobalTargetInfo() {
    return {};
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_METADATA_MANAGER_H_
