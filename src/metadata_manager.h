//
// Created by lukemartinlogan on 12/4/22.
//

#ifndef HERMES_SRC_METADATA_MANAGER_H_
#define HERMES_SRC_METADATA_MANAGER_H_

#include "decorator.h"
#include "hermes_types.h"
#include "hermes_status.h"
#include "rpc.h"

namespace hermes {

using api::Blob;

struct BufferInfo {
  size_t off_;
  size_t size_;
  TargetID target_;
};

struct BlobInfo {
  std::string name_;
  std::vector<BufferInfo> buffers_;
  RwLock rwlock_;
};

struct BucketInfo {
  std::string name_;
  std::vector<BlobID> blobs_;
};

struct VBucketInfo {
  std::vector<char> name_;
  std::unordered_set<BlobID> blobs_;
};

class MetadataManager {
 private:
  RPC_TYPE* rpc_;
  std::unordered_map<std::string, BlobID> blob_id_map_;
  std::unordered_map<std::string, BucketID> bkt_id_map_;
  std::unordered_map<std::string, VBucketID> vbkt_id_map_;

  std::unordered_map<BlobID, BlobInfo> blob_map_;
  std::unordered_map<BucketID, BucketInfo> bkt_map_;
  std::unordered_map<VBucketID, VBucketInfo> vbkt_map_;

 public:
  MetadataManager() = default;
  void Init();

  /**
   * Get or create a bucket with \a bkt_name bucket name
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC BucketID LocalGetOrCreateBucket(std::string bkt_name);

  /**
   * Get the BucketID with \a bkt_name bucket name
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC BucketID LocalGetBucketId(std::string bkt_name);

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
  RPC bool LocalRenameBucket(BucketID bkt_id, std::string new_bkt_name);

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
  RPC BlobID LocalBucketPutBlob(BucketID bkt_id, std::string blob_name,
                                Blob data, std::vector<BufferInfo> buffers);

  /**
   * Get \a blob_name blob from \a bkt_id bucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC BlobID LocalGetBlobId(BucketID bkt_id, std::string blob_name);

  /**
   * Change \a blob_id blob's buffers to \the buffers
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalSetBlobBuffers(BlobID blob_id, std::vector<BufferInfo> buffers);

  /**
   * Get \a blob_id blob's buffers
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC std::vector<BufferInfo>& LocalGetBlobBuffers(BlobID blob_id);

  /**
   * Rename \a blob_id blob to \a new_blob_name new blob name
   * in \a bkt_id bucket.
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalRenameBlob(BucketID bkt_id, BlobID blob_id,
                           std::string new_blob_name);

  /**
   * Destroy \a blob_id blob in \a bkt_id bucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalDestroyBlob(BucketID bkt_id, std::string blob_name);

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
  RPC VBucketID LocalGetOrCreateVBucket(std::string vbkt_name);

  /**
   * Get the VBucketID of \a vbkt_name VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC VBucketID LocalGetVBucketId(std::string vbkt_name);

  /**
   * Link \a vbkt_id VBucketID
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC VBucketID LocalVBucketLinkBlob(VBucketID vbkt_id, BucketID bkt_id,
                                     std::string blob_name);

  /**
   * Unlink \a blob_name Blob of \a bkt_id Bucket
   * from \a vbkt_id VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC VBucketID LocalVBucketUnlinkBlob(VBucketID vbkt_id, BucketID bkt_id,
                                       std::string blob_name);

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
  RPC bool LocalRenameVBucket(VBucketID vbkt_id, std::string new_vbkt_name);

  /**
   * Destroy \a vbkt_id VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
  RPC bool LocalDestroyVBucket(VBucketID vbkt_id);

 public:
  RPC_AUTOGEN_START
  RPC_AUTOGEN_END
};

}  // namespace hermes

#endif  // HERMES_SRC_METADATA_MANAGER_H_