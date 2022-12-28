//
// Created by lukemartinlogan on 12/4/22.
//

#include "hermes.h"
#include "metadata_manager.h"
#include "buffer_organizer.h"

namespace hermes {

/**
 * Explicitly initialize the MetadataManager
 * Doesn't require anything to be initialized.
 * */
void MetadataManager::shm_init(ServerConfig *config,
                               MetadataManagerShmHeader *header) {
  header_ = header;
  rpc_ = &HERMES->rpc_;
  borg_ = &HERMES->borg_;
  header_->id_alloc_ = 1;

  // Create the metadata maps
  blob_id_map_ = lipc::make_mptr<BLOB_ID_MAP_T>(nullptr);
  bkt_id_map_ = lipc::make_mptr<BKT_ID_MAP_T>(nullptr);
  vbkt_id_map_ = lipc::make_mptr<VBKT_ID_MAP_T>(nullptr);
  blob_map_ = lipc::make_mptr<BLOB_MAP_T>(nullptr);
  bkt_map_ = lipc::make_mptr<BKT_MAP_T>(nullptr);
  vbkt_map_ = lipc::make_mptr<VBKT_MAP_T>(nullptr);

  // Create the DeviceInfo vector
  devices_ = lipc::make_mptr<lipc::vector<DeviceInfo>>(
      nullptr, HERMES->server_config_.devices_);
  targets_ = lipc::make_mptr<lipc::vector<TargetInfo>>(nullptr);

  // Create the TargetInfo vector
  targets_->reserve(devices_->size());
  int dev_id = 0;
  for (auto &dev_info : config->devices_) {
    targets_->emplace_back(
        TargetId(rpc_->node_id_, dev_id, dev_id),
        dev_info.capacity_,
        dev_info.capacity_,
        dev_info.bandwidth_,
        dev_info.latency_);
    ++dev_id;
  }

  // Ensure all local processes can access data structures
  shm_serialize();
  shm_deserialize(header_);
}

/**
 * Explicitly destroy the MetadataManager
 * */
void MetadataManager::shm_destroy() {
  blob_id_map_.shm_destroy();
  bkt_id_map_.shm_destroy();
  vbkt_id_map_.shm_destroy();
  blob_map_.shm_destroy();
  bkt_map_.shm_destroy();
  vbkt_map_.shm_destroy();
  targets_.shm_destroy();
  devices_.shm_destroy();
}

/**
 * Store the MetadataManager in shared memory.
 * */
void MetadataManager::shm_serialize() {
  blob_id_map_ >> header_->blob_id_map_ar_;
  bkt_id_map_ >> header_->bkt_id_map_ar_;
  vbkt_id_map_ >> header_->vbkt_id_map_ar_;
  blob_map_ >> header_->blob_map_ar_;
  bkt_map_ >> header_->bkt_map_ar_;
  vbkt_map_ >> header_->vbkt_map_ar_;
  targets_ >> header_->targets_;
  devices_ >> header_->devices_;
}

/**
 * Store the MetadataManager in shared memory.
 * */
void MetadataManager::shm_deserialize(MetadataManagerShmHeader *header) {
  header_ = header;
  rpc_ = &HERMES->rpc_;
  borg_ = &HERMES->borg_;
  blob_id_map_ << header_->blob_id_map_ar_;
  bkt_id_map_ << header_->bkt_id_map_ar_;
  vbkt_id_map_ << header_->vbkt_id_map_ar_;
  blob_map_ << header_->blob_map_ar_;
  bkt_map_ << header_->bkt_map_ar_;
  vbkt_map_ << header_->vbkt_map_ar_;
  targets_ << header_->targets_;
  devices_ << header_->devices_;
}

/**
   * Get or create a bucket with \a bkt_name bucket name
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
BucketId MetadataManager::LocalGetOrCreateBucket(lipc::charbuf &bkt_name) {
  // Create unique ID for the Bucket
  BucketId bkt_id;
  bkt_id.unique_ = header_->id_alloc_.fetch_add(1);
  bkt_id.node_id_ = rpc_->node_id_;

  // Emplace bucket if it does not already exist
  if (bkt_id_map_->try_emplace(bkt_name, bkt_id)) {
    BucketInfo info;
    info.name_ = lipc::make_mptr<lipc::string>(bkt_name);
    BucketInfoShmHeader hdr;
    info.name_ >> hdr.name_ar_;
    bkt_map_->emplace(bkt_id, hdr);
  } else {
    auto iter = bkt_id_map_->find(bkt_name);
    if (iter == bkt_id_map_->end()) {
      return BucketId::GetNull();
    }
    bkt_id = (*iter).val_;
  }

  return bkt_id;
}

/**
   * Get the BucketId with \a bkt_name bucket name
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
BucketId MetadataManager::LocalGetBucketId(lipc::charbuf &bkt_name) {
  BucketId bkt_id;
  auto iter = bkt_id_map_->find(bkt_name);
  if (iter == bkt_id_map_->end()) {
    return BucketId::GetNull();
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
bool MetadataManager::LocalBucketContainsBlob(BucketId bkt_id, BlobId blob_id) {
  auto iter = blob_map_->find(blob_id);
  if (iter == blob_map_->end()) {
    return false;
  }
  // Get the blob info
  BlobInfoShmHeader &hdr = (*iter).val_.get_ref();
  BlobInfo info(hdr);
  return info.bkt_id_ == bkt_id;
}

/**
   * Rename \a bkt_id bucket to \a new_bkt_name new name
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
bool MetadataManager::LocalRenameBucket(BucketId bkt_id,
                                        lipc::charbuf &new_bkt_name) {
  return true;
}


/**
   * Destroy \a bkt_id bucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
bool MetadataManager::LocalDestroyBucket(BucketId bkt_id) {
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
BlobId MetadataManager::LocalBucketPutBlob(BucketId bkt_id,
                                           const lipc::charbuf &blob_name,
                                           Blob &data,
                                           lipc::vector<BufferInfo> &buffers) {
  lipc::charbuf internal_blob_name = CreateBlobName(bkt_id, blob_name);

  // Create unique ID for the Blob
  BlobId blob_id;
  blob_id.unique_ = header_->id_alloc_.fetch_add(1);
  blob_id.node_id_ = rpc_->node_id_;
  if (blob_id_map_->try_emplace(internal_blob_name, blob_id)) {
    BlobInfo info;
    info.bkt_id_ = bkt_id;
    info.name_ = lipc::make_mptr<lipc::string>(std::move(internal_blob_name));
    info.buffers_ = lipc::make_mptr<lipc::vector<BufferInfo>>(buffers);
    auto &buffers = *info.buffers_;
    BlobInfoShmHeader hdr;
    info.shm_serialize(hdr);
    blob_map_->emplace(blob_id, std::move(hdr));
  } else {
    blob_id = (*blob_id_map_)[internal_blob_name];
    auto iter = blob_map_->find(blob_id);
    BlobInfoShmHeader &hdr = (*iter).val_.get_ref();
    BlobInfo info(hdr);
    *(info.buffers_) = buffers;
    info.shm_serialize(hdr);
    (*iter).val_ = std::move(hdr);
  }

  return blob_id;
}

/**
   * Get \a blob_name blob from \a bkt_id bucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
Blob MetadataManager::LocalBucketGetBlob(BlobId blob_id) {
  auto iter = blob_map_->find(blob_id);
  BlobInfoShmHeader &hdr = (*iter).val_.get_ref();
  BlobInfo info(hdr);
  auto &buffers = *info.buffers_;
  return borg_->LocalReadBlobFromBuffers(buffers);
}

/**
   * Get \a blob_name blob from \a bkt_id bucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
BlobId MetadataManager::LocalGetBlobId(BucketId bkt_id,
                                       lipc::charbuf &blob_name) {
  lipc::charbuf internal_blob_name = CreateBlobName(bkt_id, blob_name);
  auto iter = blob_id_map_->find(internal_blob_name);
  if (iter == blob_id_map_->end()) {
    return BlobId::GetNull();
  }
  return (*iter).val_;
}

/**
   * Change \a blob_id blob's buffers to \the buffers
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
bool MetadataManager::LocalSetBlobBuffers(BlobId blob_id,
                                          lipc::vector<BufferInfo> &buffers) {
}

/**
   * Get \a blob_id blob's buffers
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
lipc::vector<BufferInfo> MetadataManager::LocalGetBlobBuffers(BlobId blob_id) {
}

/**
   * Rename \a blob_id blob to \a new_blob_name new blob name
   * in \a bkt_id bucket.
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
bool MetadataManager::LocalRenameBlob(BucketId bkt_id, BlobId blob_id,
                                      lipc::charbuf &new_blob_name) {
}

/**
   * Destroy \a blob_id blob in \a bkt_id bucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
bool MetadataManager::LocalDestroyBlob(BucketId bkt_id,
                                       lipc::charbuf &blob_name) {
}

/**
   * Acquire \a blob_id blob's write lock
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
bool MetadataManager::LocalWriteLockBlob(BlobId blob_id) {
}

/**
   * Release \a blob_id blob's write lock
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
bool MetadataManager::LocalWriteUnlockBlob(BlobId blob_id) {
}

/**
   * Acquire \a blob_id blob's read lock
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
bool MetadataManager::LocalReadLockBlob(BlobId blob_id) {
}

/**
   * Release \a blob_id blob's read lock
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
bool MetadataManager::LocalReadUnlockBlob(BlobId blob_id) {
}

/**
   * Get or create \a vbkt_name VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
VBucketId MetadataManager::LocalGetOrCreateVBucket(lipc::charbuf &vbkt_name) {
}

/**
   * Get the VBucketId of \a vbkt_name VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
VBucketId MetadataManager::LocalGetVBucketId(lipc::charbuf &vbkt_name) {
}

/**
   * Link \a vbkt_id VBucketId
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
VBucketId MetadataManager::LocalVBucketLinkBlob(VBucketId vbkt_id,
                                                BucketId bkt_id,
                                                lipc::charbuf &blob_name) {
}

/**
   * Unlink \a blob_name Blob of \a bkt_id Bucket
   * from \a vbkt_id VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
VBucketId MetadataManager::LocalVBucketUnlinkBlob(VBucketId vbkt_id,
                                                  BucketId bkt_id,
                                                  lipc::charbuf &blob_name) {
}

/**
   * Get the linked blobs from \a vbkt_id VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
std::list<BlobId> MetadataManager::LocalVBucketGetLinks(VBucketId vbkt_id) {
}

/**
   * Rename \a vbkt_id VBucket to \a new_vbkt_name name
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
bool MetadataManager::LocalRenameVBucket(VBucketId vbkt_id,
                                         lipc::charbuf &new_vbkt_name) {
}

/**
   * Destroy \a vbkt_id VBucket
   *
   * @RPC_TARGET_NODE rpc_->node_id_
   * @RPC_CLASS_INSTANCE mdm
   * */
bool MetadataManager::LocalDestroyVBucket(VBucketId vbkt_id) {
}

}  // namespace hermes