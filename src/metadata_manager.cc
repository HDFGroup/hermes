//
// Created by lukemartinlogan on 12/4/22.
//

#include "hermes.h"
#include "metadata_manager.h"
#include "buffer_organizer.h"
#include "api/bucket.h"

namespace hermes {

/** Namespace simplification for Bucket */
using api::Bucket;

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
  blob_id_map_ = lipc::make_mptr<BLOB_ID_MAP_T>(16384);
  bkt_id_map_ = lipc::make_mptr<BKT_ID_MAP_T>(16384);
  vbkt_id_map_ = lipc::make_mptr<VBKT_ID_MAP_T>(16384);
  blob_map_ = lipc::make_mptr<BLOB_MAP_T>(16384);
  bkt_map_ = lipc::make_mptr<BKT_MAP_T>(16384);
  vbkt_map_ = lipc::make_mptr<VBKT_MAP_T>(16384);

  // Create the DeviceInfo vector
  devices_ = lipc::make_mptr<lipc::vector<DeviceInfo>>(
      HERMES->main_alloc_, config->devices_);
  targets_ = lipc::make_mptr<lipc::vector<TargetInfo>>(
      HERMES->main_alloc_);

  // Create the TargetInfo vector
  targets_->reserve(devices_->size());
  int dev_id = 0;
  for (auto &dev_info : config->devices_) {
    targets_->emplace_back(
        TargetId(rpc_->node_id_, dev_id, dev_id),
        dev_info.header_->capacity_,
        dev_info.header_->capacity_,
        dev_info.header_->bandwidth_,
        dev_info.header_->latency_);
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
BucketId MetadataManager::LocalGetOrCreateBucket(
    lipc::charbuf &bkt_name,
    const IoClientOptions &opts) {
  // Create unique ID for the Bucket
  BucketId bkt_id;
  bkt_id.unique_ = header_->id_alloc_.fetch_add(1);
  bkt_id.node_id_ = rpc_->node_id_;

  // Emplace bucket if it does not already exist
  if (bkt_id_map_->try_emplace(bkt_name, bkt_id)) {
    BucketInfo info(HERMES->main_alloc_);
    (*info.name_) = bkt_name;
    info.header_->internal_size_ = 0;
    auto io_client = IoClientFactory::Get(opts.type_);
    if (io_client) {
      io_client->InitBucketState(bkt_name,
                                 opts,
                                 info.header_->client_state_);
    }
    bkt_map_->emplace(bkt_id, std::move(info));
  } else {
    auto iter = bkt_id_map_->find(bkt_name);
    if (iter == bkt_id_map_->end()) {
      return BucketId::GetNull();
    }
    lipc::ShmRef<lipc::pair<lipc::charbuf, BucketId>> info = (*iter);
    bkt_id = *info->second_;
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
  auto iter = bkt_id_map_->find(bkt_name);
  if (iter == bkt_id_map_->end()) {
    return BucketId::GetNull();
  }
  lipc::ShmRef<lipc::pair<lipc::charbuf, BucketId>> info = (*iter);
  BucketId bkt_id = *info->second_;
  return bkt_id;
}

/**
 * Get the size of the bucket. May consider the impact the bucket has
 * on the backing storage system's statistics using the io_ctx.
 *
 * @RPC_TARGET_NODE rpc_->node_id_
 * @RPC_CLASS_INSTANCE mdm
 * */
size_t MetadataManager::LocalGetBucketSize(BucketId bkt_id,
                                           const IoClientOptions &opts) {
  auto iter = bkt_map_->find(bkt_id);
  if (iter == bkt_map_->end()) {
    return 0;
  }
  lipc::ShmRef<lipc::pair<BucketId, BucketInfo>> info = (*iter);
  BucketInfo &bkt_info = *info->second_;
  auto io_client = IoClientFactory::Get(opts.type_);
  if (io_client) {
    return bkt_info.header_->client_state_.true_size_;
  } else {
    return bkt_info.header_->internal_size_;
  }
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
  lipc::ShmRef<lipc::pair<BlobId, BlobInfo>> info = (*iter);
  BlobInfo &blob_info = *info->second_;
  return blob_info.bkt_id_ == bkt_id;
}

/**
 * Rename \a bkt_id bucket to \a new_bkt_name new name
 *
 * @RPC_TARGET_NODE rpc_->node_id_
 * @RPC_CLASS_INSTANCE mdm
 * */
bool MetadataManager::LocalRenameBucket(BucketId bkt_id,
                                        lipc::charbuf &new_bkt_name) {
  auto iter = bkt_map_->find(bkt_id);
  if (iter == bkt_map_->end()) {
    return true;
  }
  lipc::ShmRef<lipc::pair<BucketId, BucketInfo>> info = (*iter);
  lipc::string &old_bkt_name = *info->second_->name_;
  bkt_id_map_->emplace(new_bkt_name, bkt_id);
  bkt_id_map_->erase(old_bkt_name);
  return true;
}

/**
 * Destroy \a bkt_id bucket
 *
 * @RPC_TARGET_NODE rpc_->node_id_
 * @RPC_CLASS_INSTANCE mdm
 * */
bool MetadataManager::LocalDestroyBucket(BucketId bkt_id) {
  bkt_map_->erase(bkt_id);
  return true;
}

/** Registers a blob with the bucket */
Status MetadataManager::LocalBucketRegisterBlobId(
    BucketId bkt_id,
    BlobId blob_id,
    size_t orig_blob_size,
    size_t new_blob_size,
    bool did_create,
    const IoClientOptions &opts) {
  auto iter = bkt_map_->find(bkt_id);
  if (iter == bkt_map_->end()) {
    return Status();
  }
  lipc::ShmRef<lipc::pair<BucketId, BucketInfo>> info = (*iter);
  BucketInfo &bkt_info = *info->second_;
  // Update I/O client bucket stats
  auto io_client = IoClientFactory::Get(opts.type_);
  if (io_client) {
    io_client->UpdateBucketState(opts, bkt_info.header_->client_state_);
  }
  // Update internal bucket size
  bkt_info.header_->internal_size_ += new_blob_size - orig_blob_size;
  // Add blob to ID vector if it didn't already exist
  if (!did_create) { return Status(); }
  // TODO(llogan): add blob id
  return Status();
}

/** Unregister a blob from a bucket */
Status MetadataManager::LocalBucketUnregisterBlobId(
    BucketId bkt_id, BlobId blob_id,
    const IoClientContext &io_ctx) {
  // TODO(llogan)
}

/**
 * Creates the blob metadata
 *
 * @param bkt_id id of the bucket
 * @param blob_name semantic blob name
 * @param data the data being placed
 * @param buffers the buffers to place data in
 *
 * @RPC_TARGET_NODE rpc_->node_id_
 * @RPC_CLASS_INSTANCE mdm
 * */
std::tuple<BlobId, bool, size_t> MetadataManager::LocalBucketPutBlob(
    BucketId bkt_id,
    const lipc::charbuf &blob_name,
    size_t blob_size,
    lipc::vector<BufferInfo> &buffers) {
  size_t orig_blob_size = 0;

  // Get internal blob name
  lipc::charbuf internal_blob_name = CreateBlobName(bkt_id, blob_name);

  // Create unique ID for the Blob
  BlobId blob_id;
  blob_id.unique_ = header_->id_alloc_.fetch_add(1);
  blob_id.node_id_ = rpc_->node_id_;
  bool did_create = blob_id_map_->try_emplace(internal_blob_name, blob_id);
  if (did_create) {
    BlobInfo blob_info(HERMES->main_alloc_);
    blob_info.bkt_id_ = bkt_id;
    (*blob_info.name_) = std::move(internal_blob_name);
    (*blob_info.buffers_) = std::move(buffers);
    blob_info.header_->blob_size_ = blob_size;
    blob_map_->emplace(blob_id, std::move(blob_info));
  } else {
    blob_id = *(*blob_id_map_)[internal_blob_name];
    auto iter = blob_map_->find(blob_id);
    lipc::ShmRef<lipc::pair<BlobId, BlobInfo>> info = (*iter);
    BlobInfo &blob_info = *info->second_;
    (*blob_info.buffers_) = std::move(buffers);
  }
  return std::tuple<BlobId, bool, size_t>(blob_id, did_create, orig_blob_size);
}

/**
 * Get \a blob_name blob from \a bkt_id bucket
 *
 * @RPC_TARGET_NODE rpc_->node_id_
 * @RPC_CLASS_INSTANCE mdm
 * */
Blob MetadataManager::LocalBucketGetBlob(BlobId blob_id) {
  auto iter = blob_map_->find(blob_id);
  lipc::ShmRef<lipc::pair<BlobId, BlobInfo>> info = (*iter);
  BlobInfo &blob_info = *info->second_;
  lipc::vector<BufferInfo> &buffers = *blob_info.buffers_;
  return borg_->LocalReadBlobFromBuffers(buffers);;
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
  lipc::ShmRef<lipc::pair<lipc::charbuf, BlobId>> info = *iter;
  return *info->second_;
}

/**
 * Get \a blob_id blob's buffers
 *
 * @RPC_TARGET_NODE rpc_->node_id_
 * @RPC_CLASS_INSTANCE mdm
 * */
std::vector<BufferInfo> MetadataManager::LocalGetBlobBuffers(BlobId blob_id) {
  auto iter = blob_map_->find(blob_id);
  if (iter == blob_map_->end()) {
    return std::vector<BufferInfo>();
  }
  lipc::ShmRef<lipc::pair<BlobId, BlobInfo>> info = (*iter);
  BlobInfo &blob_info = *info->second_;

  // TODO(llogan): make this internal to the lipc::vec
  std::vector<BufferInfo> v;
  v.reserve(blob_info.buffers_->size());
  for (lipc::ShmRef<BufferInfo> buffer_info : *blob_info.buffers_) {
    v.emplace_back(*buffer_info);
  }
  return v;
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
  auto iter = (*blob_map_).find(blob_id);
  if (iter == blob_map_->end()) {
    return true;
  }
  lipc::ShmRef<lipc::pair<BlobId, BlobInfo>> info = (*iter);
  BlobInfo &blob_info = *info->second_;
  lipc::charbuf &old_blob_name = (*blob_info.name_);
  lipc::charbuf internal_blob_name = CreateBlobName(bkt_id, new_blob_name);
  blob_id_map_->erase(old_blob_name);
  blob_id_map_->emplace(internal_blob_name, blob_id);
  return true;
}

/**
 * Destroy \a blob_id blob in \a bkt_id bucket
 *
 * @RPC_TARGET_NODE rpc_->node_id_
 * @RPC_CLASS_INSTANCE mdm
 * */
bool MetadataManager::LocalDestroyBlob(BucketId bkt_id,
                                       BlobId blob_id) {
  (void)bkt_id;
  auto iter = (*blob_map_).find(blob_id);
  if (iter == blob_map_->end()) {
    return true;
  }
  lipc::ShmRef<lipc::pair<BlobId, BlobInfo>> info = (*iter);
  BlobInfo &blob_info = *info->second_;
  lipc::charbuf &blob_name = (*blob_info.name_);
  blob_id_map_->erase(blob_name);
  blob_map_->erase(blob_id);
  return true;
}

/**
 * Get or create \a vbkt_name VBucket
 *
 * @RPC_TARGET_NODE rpc_->node_id_
 * @RPC_CLASS_INSTANCE mdm
 * */
VBucketId MetadataManager::LocalGetOrCreateVBucket(
    lipc::charbuf &vbkt_name,
    const IoClientOptions &opts) {
  (void) opts;
  // Create unique ID for the Bucket
  VBucketId vbkt_id;
  vbkt_id.unique_ = header_->id_alloc_.fetch_add(1);
  vbkt_id.node_id_ = rpc_->node_id_;

  // Emplace bucket if it does not already exist
  if (vbkt_id_map_->try_emplace(vbkt_name, vbkt_id)) {
    VBucketInfo info(HERMES->main_alloc_);
    (*info.name_) = vbkt_name;
    vbkt_map_->emplace(vbkt_id, std::move(info));
  } else {
    auto iter = vbkt_id_map_->find(vbkt_name);
    if (iter == vbkt_id_map_->end()) {
      return VBucketId::GetNull();
    }
    lipc::ShmRef<lipc::pair<lipc::charbuf, VBucketId>> info = (*iter);
    vbkt_id = *info->second_;
  }
  return vbkt_id;
}

/**
 * Get the VBucketId of \a vbkt_name VBucket
 *
 * @RPC_TARGET_NODE rpc_->node_id_
 * @RPC_CLASS_INSTANCE mdm
 * */
VBucketId MetadataManager::LocalGetVBucketId(lipc::charbuf &vbkt_name) {
  auto iter = vbkt_id_map_->find(vbkt_name);
  if (iter == vbkt_id_map_->end()) {
    return VBucketId::GetNull();
  }
  lipc::ShmRef<lipc::pair<lipc::charbuf, VBucketId>> info = (*iter);
  VBucketId vbkt_id = *info->second_;
  return vbkt_id;
}

/**
 * Link \a vbkt_id VBucketId
 *
 * @RPC_TARGET_NODE rpc_->node_id_
 * @RPC_CLASS_INSTANCE mdm
 * */
bool MetadataManager::LocalVBucketLinkBlob(VBucketId vbkt_id,
                                           BlobId blob_id) {
  auto iter = vbkt_map_->find(vbkt_id);
  if (iter == vbkt_map_->end()) {
    return true;
  }
  lipc::ShmRef<lipc::pair<VBucketId, VBucketInfo>> info = (*iter);
  VBucketInfo &vbkt_info = *info->second_;
  vbkt_info.blobs_->emplace(blob_id, blob_id);
  return true;
}

/**
 * Unlink \a blob_name Blob of \a bkt_id Bucket
 * from \a vbkt_id VBucket
 *
 * @RPC_TARGET_NODE rpc_->node_id_
 * @RPC_CLASS_INSTANCE mdm
 * */
bool MetadataManager::LocalVBucketUnlinkBlob(VBucketId vbkt_id,
                                             BlobId blob_id) {
  auto iter = vbkt_map_->find(vbkt_id);
  if (iter == vbkt_map_->end()) {
    return true;
  }
  lipc::ShmRef<lipc::pair<VBucketId, VBucketInfo>> info = (*iter);
  VBucketInfo &vbkt_info = *info->second_;
  vbkt_info.blobs_->erase(blob_id);
  return true;
}

/**
 * Get the linked blobs from \a vbkt_id VBucket
 *
 * @RPC_TARGET_NODE rpc_->node_id_
 * @RPC_CLASS_INSTANCE mdm
 * */
std::list<BlobId> MetadataManager::LocalVBucketGetLinks(VBucketId vbkt_id) {
  // TODO(llogan)
  return {};
}

/**
 * Whether \a vbkt_id VBucket contains \a blob_id blob
 * */
bool MetadataManager::LocalVBucketContainsBlob(VBucketId vbkt_id,
                                               BlobId blob_id) {
  auto iter = vbkt_map_->find(vbkt_id);
  if (iter == vbkt_map_->end()) {
    return true;
  }
  lipc::ShmRef<lipc::pair<VBucketId, VBucketInfo>> info = (*iter);
  VBucketInfo &vbkt_info = *info->second_;
  auto link_iter = vbkt_info.blobs_->find(blob_id);
  return link_iter != vbkt_info.blobs_->end();
}

/**
 * Rename \a vbkt_id VBucket to \a new_vbkt_name name
 *
 * @RPC_TARGET_NODE rpc_->node_id_
 * @RPC_CLASS_INSTANCE mdm
 * */
bool MetadataManager::LocalRenameVBucket(VBucketId vbkt_id,
                                         lipc::charbuf &new_vbkt_name) {
  auto iter = vbkt_map_->find(vbkt_id);
  if (iter == vbkt_map_->end()) {
    return true;
  }
  lipc::ShmRef<lipc::pair<VBucketId, VBucketInfo>> info = (*iter);
  VBucketInfo &vbkt_info = *info->second_;
  lipc::string &old_bkt_name = *vbkt_info.name_;
  vbkt_id_map_->emplace(new_vbkt_name, vbkt_id);
  vbkt_id_map_->erase(old_bkt_name);
  return true;
}

/**
 * Destroy \a vbkt_id VBucket
 *
 * @RPC_TARGET_NODE rpc_->node_id_
 * @RPC_CLASS_INSTANCE mdm
 * */
bool MetadataManager::LocalDestroyVBucket(VBucketId vbkt_id) {
  vbkt_map_->erase(vbkt_id);
  return true;
}

}  // namespace hermes