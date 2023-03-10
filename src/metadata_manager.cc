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
  blob_id_map_ = hipc::make_mptr<BLOB_ID_MAP_T>(16384);
  tag_id_map_ = hipc::make_mptr<TAG_ID_MAP_T>(16384);
  trait_id_map_ = hipc::make_mptr<TRAIT_ID_MAP_T>(16384);
  blob_map_ = hipc::make_mptr<BLOB_MAP_T>(16384);
  tag_map_ = hipc::make_mptr<TAG_MAP_T>(256);
  trait_map_ = hipc::make_mptr<TRAIT_MAP_T>(256);

  // Create the DeviceInfo vector
  devices_ = hipc::make_mptr<hipc::vector<DeviceInfo>>(
      HERMES->main_alloc_, config->devices_);
  targets_ = hipc::make_mptr<hipc::vector<TargetInfo>>(
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
  tag_id_map_.shm_destroy();
  blob_map_.shm_destroy();
  tag_map_.shm_destroy();
  tag_map_.shm_destroy();
  targets_.shm_destroy();
  devices_.shm_destroy();
}

/**
 * Store the MetadataManager in shared memory.
 * */
void MetadataManager::shm_serialize() {
  blob_id_map_ >> header_->blob_id_map_ar_;
  tag_id_map_ >> header_->tag_id_map_ar_;
  blob_map_ >> header_->blob_map_ar_;
  tag_map_ >> header_->tag_map_ar_;
  tag_map_ >> header_->tag_map_ar_;
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
  tag_id_map_ << header_->tag_id_map_ar_;
  blob_map_ << header_->blob_map_ar_;
  tag_map_ << header_->tag_map_ar_;
  tag_map_ << header_->tag_map_ar_;
  targets_ << header_->targets_;
  devices_ << header_->devices_;
}

/**====================================
 * Bucket Operations
 * ===================================*/

/**
 * Get the size of the bucket. May consider the impact the bucket has
 * on the backing storage system's statistics using the io_ctx.
 * */
size_t MetadataManager::LocalGetBucketSize(TagId bkt_id,
                                           const IoClientContext &opts) {
  // Acquire MD read lock (reading tag_map)
  ScopedRwReadLock tag_map_lock(header_->lock_[kTagMapLock]);
  auto iter = tag_map_->find(bkt_id);
  if (iter == tag_map_->end()) {
    return 0;
  }
  hipc::ShmRef<hipc::pair<TagId, TagInfo>> info = (*iter);
  TagInfo &bkt_info = *info->second_;
  auto io_client = IoClientFactory::Get(opts.type_);
  if (io_client) {
    return bkt_info.header_->client_state_.true_size_;
  } else {
    return bkt_info.header_->internal_size_;
  }
}

/**
 * Clear \a bkt_id bucket
 * */
bool MetadataManager::LocalClearBucket(TagId bkt_id) {
  ScopedRwWriteLock tag_map_lock(header_->lock_[kTagMapLock]);
  auto iter = tag_map_->find(bkt_id);
  if (iter == tag_map_->end()) {
    return true;
  }
  hipc::ShmRef<hipc::pair<TagId, TagInfo>> info = (*iter);
  TagInfo &bkt_info = *info->second_;
  for (hipc::ShmRef<BlobId> blob_id : *bkt_info.blobs_) {
    GlobalDestroyBlob(bkt_id, *blob_id);
  }
  return true;
}

/**====================================
 * Blob Operations
 * ===================================*/

/**
 * Creates the blob metadata
 *
 * @param bkt_id id of the bucket
 * @param blob_name semantic blob name
 * */
std::pair<BlobId, bool> MetadataManager::LocalTryCreateBlob(
    TagId bkt_id,
    const hipc::charbuf &blob_name) {
  size_t orig_blob_size = 0;
  // Acquire MD write lock (modify blob_map_)
  ScopedRwWriteLock blob_map_lock(header_->lock_[kBlobMapLock]);
  // Get internal blob name
  hipc::charbuf internal_blob_name = CreateBlobName(bkt_id, blob_name);
  // Create unique ID for the Blob
  BlobId blob_id;
  blob_id.unique_ = header_->id_alloc_.fetch_add(1);
  blob_id.node_id_ = rpc_->node_id_;
  bool did_create = blob_id_map_->try_emplace(internal_blob_name, blob_id);
  if (did_create) {
    BlobInfo blob_info(HERMES->main_alloc_);
    (*blob_info.name_) = blob_name;
    blob_info.header_->tag_id_ = bkt_id;
    blob_info.header_->blob_size_ = 0;
    blob_id_map_->emplace(internal_blob_name, blob_id);
    blob_map_->emplace(blob_id, std::move(blob_info));
  }
  return std::pair<BlobId, bool>(blob_id, did_create);
}

/**
 * Tag a blob
 *
 * @param blob_id id of the blob being tagged
 * @param tag_name tag name
 * */
Status MetadataManager::LocalTagBlob(
    BlobId blob_id, TagId tag_id) {
  // Acquire MD read lock (read blob_map_)
  ScopedRwReadLock blob_map_lock(header_->lock_[kBlobMapLock]);
  auto iter = blob_map_->find(blob_id);
  if (iter == blob_map_->end()) {
    return Status();  // TODO(llogan): error status
  }
  hipc::ShmRef<hipc::pair<BlobId, BlobInfo>> info = (*iter);
  BlobInfo &blob_info = *info->second_;
  // Acquire blob_info read lock (read buffers)
  ScopedRwReadLock blob_info_lock(blob_info.header_->lock_[0]);
  blob_info.tags_->emplace_back(tag_id);
  return Status();
}

/**
 * Check whether or not \a blob_id BLOB has \a tag_id TAG
 * */
bool MetadataManager::LocalBlobHasTag(BlobId blob_id,
                                      TagId tag_id) {
  // Acquire MD read lock (reading blob_map_)
  ScopedRwReadLock blob_map_lock(header_->lock_[kBlobMapLock]);
  auto iter = blob_map_->find(blob_id);
  if (iter == blob_map_->end()) {
    return false;
  }
  // Get the blob info
  hipc::ShmRef<hipc::pair<BlobId, BlobInfo>> info = (*iter);
  BlobInfo &blob_info = *info->second_;
  // Iterate over tags
  for (hipc::ShmRef<TagId> tag_cmp : *blob_info.tags_) {
    if (blob_info.header_->tag_id_ == tag_id) {
      return true;
    }
  }
  return false;
}

/**
 * Creates the blob metadata
 *
 * @param bkt_id id of the bucket
 * @param blob_name semantic blob name
 * @param data the data being placed
 * @param buffers the buffers to place data in
 * */
std::tuple<BlobId, bool, size_t> MetadataManager::LocalPutBlobMetadata(
    TagId bkt_id,
    const hipc::charbuf &blob_name,
    size_t blob_size,
    std::vector<BufferInfo> &buffers) {
  size_t orig_blob_size = 0;
  // Acquire MD write lock (modify blob_map_)
  ScopedRwWriteLock blob_map_lock(header_->lock_[kBlobMapLock]);
  // Get internal blob name
  hipc::charbuf internal_blob_name = CreateBlobName(bkt_id, blob_name);
  // Create unique ID for the Blob
  BlobId blob_id;
  blob_id.unique_ = header_->id_alloc_.fetch_add(1);
  blob_id.node_id_ = rpc_->node_id_;
  bool did_create = blob_id_map_->try_emplace(internal_blob_name, blob_id);
  if (did_create) {
    BlobInfo blob_info(HERMES->main_alloc_);
    (*blob_info.name_) = blob_name;
    (*blob_info.buffers_) = hipc::vector<BufferInfo>(buffers);
    blob_info.header_->blob_id_ = blob_id;
    blob_info.header_->tag_id_ = bkt_id;
    blob_info.header_->blob_size_ = blob_size;
    blob_id_map_->emplace(internal_blob_name, blob_id);
    blob_map_->emplace(blob_id, std::move(blob_info));
  } else {
    blob_id = *(*blob_id_map_)[internal_blob_name];
    auto iter = blob_map_->find(blob_id);
    hipc::ShmRef<hipc::pair<BlobId, BlobInfo>> info = (*iter);
    BlobInfo &blob_info = *info->second_;
    // Acquire blob_info write lock before modifying buffers
    ScopedRwWriteLock(blob_info.header_->lock_[0]);
    (*blob_info.buffers_) = hipc::vector<BufferInfo>(buffers);
  }
  return std::tuple<BlobId, bool, size_t>(blob_id, did_create, orig_blob_size);
}

/**
 * Get \a blob_name blob from \a bkt_id bucket
 * */
BlobId MetadataManager::LocalGetBlobId(TagId bkt_id,
                                       const hipc::charbuf &blob_name) {
  // Acquire MD read lock (read blob_id_map_)
  ScopedRwReadLock blob_map_lock(header_->lock_[kBlobMapLock]);
  hipc::charbuf internal_blob_name = CreateBlobName(bkt_id, blob_name);
  auto iter = blob_id_map_->find(internal_blob_name);
  if (iter == blob_id_map_->end()) {
    return BlobId::GetNull();
  }
  hipc::ShmRef<hipc::pair<hipc::charbuf, BlobId>> info = *iter;
  return *info->second_;
}

/**
 * Get \a blob_name BLOB name from \a blob_id BLOB id
 * */
RPC std::string MetadataManager::LocalGetBlobName(BlobId blob_id) {
  // Acquire MD read lock (read blob_id_map_)
  ScopedRwReadLock blob_map_lock(header_->lock_[kBlobMapLock]);
  auto iter = blob_map_->find(blob_id);
  if (iter == blob_map_->end()) {
    return "";
  }
  hipc::ShmRef<hipc::pair<BlobId, BlobInfo>> info = *iter;
  BlobInfo &blob_info = *info->second_;
  return blob_info.name_->str();
}

/**
 * Lock the blob
 * */
bool MetadataManager::LocalLockBlob(BlobId blob_id,
                                    MdLockType lock_type) {
  if (blob_id.IsNull()) {
    return false;
  }
  ScopedRwReadLock blob_map_lock(header_->lock_[kBlobMapLock]);
  return LockMdObject(*blob_map_, blob_id, lock_type);
}

/**
 * Unlock the blob
 * */
bool MetadataManager::LocalUnlockBlob(BlobId blob_id,
                                      MdLockType lock_type) {
  if (blob_id.IsNull()) {
    return false;
  }
  ScopedRwReadLock blob_map_lock(header_->lock_[kBlobMapLock]);
  return UnlockMdObject(*blob_map_, blob_id, lock_type);
}

/**
 * Get \a blob_id blob's buffers
 * */
std::vector<BufferInfo> MetadataManager::LocalGetBlobBuffers(BlobId blob_id) {
  // Acquire MD read lock (read blob_map_)
  ScopedRwReadLock blob_map_lock(header_->lock_[kBlobMapLock]);
  auto iter = blob_map_->find(blob_id);
  if (iter == blob_map_->end()) {
    return std::vector<BufferInfo>();
  }
  hipc::ShmRef<hipc::pair<BlobId, BlobInfo>> info = (*iter);
  BlobInfo &blob_info = *info->second_;
  // Acquire blob_info read lock
  ScopedRwReadLock blob_info_lock(blob_info.header_->lock_[0]);
  auto vec = blob_info.buffers_->vec();
  return vec;
}

/**
 * Rename \a blob_id blob to \a new_blob_name new blob name
 * in \a bkt_id bucket.
 * */
bool MetadataManager::LocalRenameBlob(TagId bkt_id, BlobId blob_id,
                                      hipc::charbuf &new_blob_name) {
  // Acquire MD write lock (modify blob_id_map_)
  ScopedRwWriteLock blob_map_lock(header_->lock_[kBlobMapLock]);
  auto iter = (*blob_map_).find(blob_id);
  if (iter == blob_map_->end()) {
    return true;
  }
  hipc::ShmRef<hipc::pair<BlobId, BlobInfo>> info = (*iter);
  BlobInfo &blob_info = *info->second_;
  hipc::charbuf old_blob_name = CreateBlobName(bkt_id, *blob_info.name_);
  hipc::charbuf internal_blob_name = CreateBlobName(bkt_id, new_blob_name);
  blob_id_map_->erase(old_blob_name);
  blob_id_map_->emplace(internal_blob_name, blob_id);
  return true;
}

/**
 * Destroy \a blob_id blob in \a bkt_id bucket
 * */
bool MetadataManager::LocalDestroyBlob(TagId bkt_id,
                                       BlobId blob_id) {
  // Acquire MD write lock (modify blob_id_map & blob_map_)
  ScopedRwWriteLock blob_map_lock(header_->lock_[kBlobMapLock]);
  (void)bkt_id;
  auto iter = (*blob_map_).find(blob_id);
  if (iter == blob_map_->end()) {
    return true;
  }
  hipc::ShmRef<hipc::pair<BlobId, BlobInfo>> info = (*iter);
  BlobInfo &blob_info = *info->second_;
  hipc::charbuf blob_name = CreateBlobName(bkt_id, *blob_info.name_);
  blob_id_map_->erase(blob_name);
  blob_map_->erase(blob_id);
  return true;
}

/**====================================
 * Bucket + Blob Operations
 * ===================================*/

/**
 * Destroy all blobs + buckets
 * */
void MetadataManager::LocalClear() {
  LOG(INFO) << "Clearing all buckets and blobs" << std::endl;
  ScopedRwWriteLock tag_map_lock(header_->lock_[kTagMapLock]);
  tag_id_map_.shm_destroy();
  tag_map_.shm_destroy();
  ScopedRwWriteLock blob_map_lock(header_->lock_[kBlobMapLock]);
  blob_id_map_.shm_destroy();
  blob_map_.shm_destroy();
}

/**
 * Destroy all blobs + buckets globally
 * */
void MetadataManager::GlobalClear() {
  for (int i = 0; i < rpc_->hosts_.size(); ++i) {
    int node_id = i + 1;
    if (NODE_ID_IS_LOCAL(node_id)) {
      LocalClear();
    } else {
      rpc_->Call<void>(node_id, "RpcLocalClear");
    }
  }
}

/**====================================
 * Tag Operations
 * ===================================*/

/**
 * Get or create a bucket with \a bkt_name bucket name
 * */
TagId MetadataManager::LocalGetOrCreateTag(const std::string &tag_name,
                                           bool owner,
                                           std::vector<TraitId> &traits) {
  // Acquire MD write lock (modifying tag_map)
  ScopedRwWriteLock tag_map_lock(header_->lock_[kTagMapLock]);

  // Create unique ID for the Bucket
  TagId bkt_id;
  bkt_id.unique_ = header_->id_alloc_.fetch_add(1);
  bkt_id.node_id_ = rpc_->node_id_;
  hipc::string tag_name_shm(tag_name);

  // Emplace bucket if it does not already exist
  auto iter = tag_id_map_->find(tag_name_shm);
  if (iter.is_end()) {
    LOG(INFO) << "Creating tag for the first time: "
              << tag_name << std::endl;
    tag_id_map_->emplace(hipc::string(tag_name), bkt_id);
    TagInfo info(HERMES->main_alloc_);
    (*info.name_) = tag_name_shm;
    info.header_->internal_size_ = 0;
    info.header_->tag_id_ = bkt_id;
    tag_map_->emplace(bkt_id, std::move(info));
  } else {
    LOG(INFO) << "Found existing tag: "
              << tag_name << std::endl;
    hipc::ShmRef<hipc::pair<hipc::charbuf, TagId>> id_info = (*iter);
    bkt_id = *id_info->second_;
  }

  return bkt_id;
}

/**
 * Get the TagId with \a bkt_name bucket name
 * */
TagId MetadataManager::LocalGetTagId(const std::string &tag_name) {
  // Acquire MD read lock (reading tag_id_map)
  ScopedRwReadLock tag_map_lock(header_->lock_[kTagMapLock]);
  hipc::string tag_name_shm(tag_name);
  auto iter = tag_id_map_->find(tag_name_shm);
  if (iter == tag_id_map_->end()) {
    return TagId::GetNull();
  }
  hipc::ShmRef<hipc::pair<hipc::charbuf, TagId>> info = (*iter);
  TagId bkt_id = *info->second_;
  return bkt_id;
}

/**
 * Rename a tag
 * */
void MetadataManager::LocalRenameTag(TagId tag_id,
                                     const std::string &new_name) {
  // Acquire MD write lock (modifying tag_map_)
  ScopedRwWriteLock tag_map_lock(header_->lock_[kTagMapLock]);
  auto iter = tag_map_->find(tag_id);
  if (iter == tag_map_->end()) {
    return;
  }
  hipc::string new_name_shm(new_name);
  hipc::ShmRef<hipc::pair<TagId, TagInfo>> info = (*iter);
  hipc::string &old_bkt_name = *info->second_->name_;
  tag_id_map_->emplace(new_name_shm, tag_id);
  tag_id_map_->erase(old_bkt_name);
  return;
}

/**
 * Delete a tag
 * */
void MetadataManager::LocalDestroyTag(TagId tag_id) {
  // Acquire MD write lock (modifying tag_map_)
  ScopedRwWriteLock tag_map_lock(header_->lock_[kTagMapLock]);
  tag_map_->erase(tag_id);
}

/**
 * Add a blob to a tag index
 * */
Status MetadataManager::LocalTagAddBlob(TagId tag_id,
                                        BlobId blob_id) {
  // Acquire MD write lock (modify tag_map_)
  ScopedRwWriteLock tag_map_lock(header_->lock_[kTagMapLock]);
  auto iter = tag_map_->find(tag_id);
  if (iter.is_end()) {
    return Status();
  }
  hipc::ShmRef<hipc::pair<TagId, TagInfo>> blob_list = (*iter);
  blob_list->second_->blobs_->emplace_back(blob_id);
  return Status();
}

/**
 * Remove a blob from a tag index.
 * */
Status MetadataManager::LocalTagRemoveBlob(TagId tag_id,
                                           BlobId blob_id) {
  // Acquire MD write lock (modify tag_map_)
  ScopedRwWriteLock tag_map_lock(header_->lock_[kTagMapLock]);
  auto iter = tag_map_->find(tag_id);
  if (iter.is_end()) {
    return Status();
  }
  hipc::ShmRef<hipc::pair<TagId, TagInfo>> blob_list = (*iter);
  blob_list->second_->blobs_->erase(blob_id);
  return Status();
}


/**
 * Find all blobs pertaining to a tag
 * */
std::vector<BlobId> MetadataManager::LocalGroupByTag(TagId tag_id) {
  // Acquire MD read lock (read tag_map_)
  ScopedRwReadLock tag_map_lock(header_->lock_[kTagMapLock]);;
  // Get the tag info
  hipc::ShmRef<TagInfo> tag_info = (*tag_map_)[tag_id];
  // Convert slist into std::vector
  std::vector<BlobId> group;
  group.reserve(tag_info->blobs_->size());
  for (hipc::ShmRef<BlobId> blob_id : (*tag_info->blobs_)) {
    group.emplace_back(*blob_id);
  }
  return group;
}

}  // namespace hermes
