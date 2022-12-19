//
// Created by lukemartinlogan on 12/4/22.
//

#include "hermes.h"
#include "metadata_manager.h"

namespace hermes {

/**
 * Explicitly initialize the MetadataManager
 * */
void MetadataManager::shm_init(MetadataManagerShmHeader *header) {
  header_ = header;
  rpc_ = &HERMES->rpc_;
  header_->id_alloc_ = 1;

  blob_id_map_owner_ =
      lipc::make_uptr<lipc::unordered_map<lipc::charbuf, BlobID>>(nullptr);
  bkt_id_map_owner_ =
      lipc::make_uptr<lipc::unordered_map<lipc::charbuf, BucketID>>(nullptr);
  vbkt_id_map_owner_ =
      lipc::make_uptr<lipc::unordered_map<lipc::charbuf, VBucketID>>(nullptr);
  blob_map_owner_ =
      lipc::make_uptr<lipc::unordered_map<BlobID, BlobInfo>>(nullptr);
  bkt_map_owner_ =
      lipc::make_uptr<lipc::unordered_map<BucketID, BucketInfo>>(nullptr);
  vbkt_map_owner_ =
      lipc::make_uptr<lipc::unordered_map<VBucketID, VBucketInfo>>(nullptr);
}

/**
 * Store the MetadataManager in shared memory.
 * */
void MetadataManager::shm_serialize() {
  blob_id_map_owner_ >> header_->blob_id_map_ar_;
  bkt_id_map_owner_ >> header_->bkt_id_map_ar_;
  vbkt_id_map_owner_ >> header_->vbkt_id_map_ar_;
  blob_map_owner_ >> header_->blob_map_ar_;
  bkt_map_owner_ >> header_->bkt_map_ar_;
  vbkt_map_owner_ >> header_->vbkt_map_ar_;
}

/**
 * Store the MetadataManager in shared memory.
 * */
void MetadataManager::shm_deserialize(MetadataManagerShmHeader *header) {
  header_ = header;
  rpc_ = &HERMES->rpc_;
  blob_id_map_ << header_->blob_id_map_ar_;
  bkt_id_map_ << header_->bkt_id_map_ar_;
  vbkt_id_map_ << header_->vbkt_id_map_ar_;
  blob_map_ << header_->blob_map_ar_;
  bkt_map_ << header_->bkt_map_ar_;
  vbkt_map_ << header_->vbkt_map_ar_;
}

/**
   * Get or create a bucket with \a bkt_name bucket name
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
BucketID MetadataManager::LocalGetOrCreateBucket(lipc::charbuf &bkt_name) {
  // Create unique ID for the Bucket
  BucketID bkt_id;
  bkt_id.unique_ = header_->id_alloc_.fetch_add(1);
  bkt_id.node_id_ = rpc_->node_id_;

  // Emplace bucket if it does not already exist
  if (bkt_id_map_->try_emplace(bkt_name, bkt_id)) {
  } else {
    auto iter = bkt_id_map_->find(bkt_name);
    if (iter == bkt_id_map_->end()) {
      return BucketID::GetNull();
    }
    bkt_id = (*iter).val_;
  }

  return bkt_id;
}

/**
   * Get the BucketID with \a bkt_name bucket name
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
BucketID MetadataManager::LocalGetBucketId(lipc::charbuf &bkt_name) {
  BucketID bkt_id;
  auto iter = bkt_id_map_->find(bkt_name);
  if (iter == bkt_id_map_->end()) {
    return BucketID::GetNull();
  }
  bkt_id = (*iter).val_;
  return bkt_id;
}

/**
   * Check whether or not \a bkt_id bucket contains
   * \a blob_id blob
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
bool MetadataManager::LocalBucketContainsBlob(BucketID bkt_id, BlobID blob_id) {
}

/**
   * Rename \a bkt_id bucket to \a new_bkt_name new name
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
bool MetadataManager::LocalRenameBucket(BucketID bkt_id,
                                        lipc::charbuf &new_bkt_name) {
}


/**
   * Destroy \a bkt_id bucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
bool MetadataManager::LocalDestroyBucket(BucketID bkt_id) {
}


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
BlobID MetadataManager::LocalBucketPutBlob(BucketID bkt_id,
                                           lipc::charbuf &blob_name,
                                           Blob &data,
                                           lipc::vector<BufferInfo> &buffers) {
}

/**
   * Get \a blob_name blob from \a bkt_id bucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
BlobID MetadataManager::LocalGetBlobId(BucketID bkt_id,
                                       lipc::charbuf &blob_name) {
}

/**
   * Change \a blob_id blob's buffers to \the buffers
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
bool MetadataManager::LocalSetBlobBuffers(BlobID blob_id,
                                          lipc::vector<BufferInfo> &buffers) {
}

/**
   * Get \a blob_id blob's buffers
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
lipc::vector<BufferInfo> MetadataManager::LocalGetBlobBuffers(BlobID blob_id) {
}

/**
   * Rename \a blob_id blob to \a new_blob_name new blob name
   * in \a bkt_id bucket.
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
bool MetadataManager::LocalRenameBlob(BucketID bkt_id, BlobID blob_id,
                                      lipc::charbuf &new_blob_name) {
}

/**
   * Destroy \a blob_id blob in \a bkt_id bucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
bool MetadataManager::LocalDestroyBlob(BucketID bkt_id,
                                       lipc::charbuf &blob_name) {
}

/**
   * Acquire \a blob_id blob's write lock
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
bool MetadataManager::LocalWriteLockBlob(BlobID blob_id) {
}

/**
   * Release \a blob_id blob's write lock
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
bool MetadataManager::LocalWriteUnlockBlob(BlobID blob_id) {
}

/**
   * Acquire \a blob_id blob's read lock
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
bool MetadataManager::LocalReadLockBlob(BlobID blob_id) {
}

/**
   * Release \a blob_id blob's read lock
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
bool MetadataManager::LocalReadUnlockBlob(BlobID blob_id) {
}

/**
   * Get or create \a vbkt_name VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
VBucketID MetadataManager::LocalGetOrCreateVBucket(lipc::charbuf &vbkt_name) {
}

/**
   * Get the VBucketID of \a vbkt_name VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
VBucketID MetadataManager::LocalGetVBucketId(lipc::charbuf &vbkt_name) {
}

/**
   * Link \a vbkt_id VBucketID
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
VBucketID MetadataManager::LocalVBucketLinkBlob(VBucketID vbkt_id,
                                                BucketID bkt_id,
                                                lipc::charbuf &blob_name) {
}

/**
   * Unlink \a blob_name Blob of \a bkt_id Bucket
   * from \a vbkt_id VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
VBucketID MetadataManager::LocalVBucketUnlinkBlob(VBucketID vbkt_id,
                                                  BucketID bkt_id,
                                                  lipc::charbuf &blob_name) {
}

/**
   * Get the linked blobs from \a vbkt_id VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
std::list<BlobID> MetadataManager::LocalVBucketGetLinks(VBucketID vbkt_id) {
}

/**
   * Rename \a vbkt_id VBucket to \a new_vbkt_name name
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
bool MetadataManager::LocalRenameVBucket(VBucketID vbkt_id,
                                         lipc::charbuf &new_vbkt_name) {
}

/**
   * Destroy \a vbkt_id VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
bool MetadataManager::LocalDestroyVBucket(VBucketID vbkt_id) {
}

}  // namespace hermes