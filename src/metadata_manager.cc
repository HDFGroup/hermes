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
 * Requires RPC to be initialized
 * */
MetadataManager::MetadataManager(
    ShmHeader<MetadataManager> *header, hipc::Allocator *alloc,
    ServerConfig *config) {
  shm_init_header(header, alloc);
  rpc_ = &HERMES->rpc_;
  borg_ = HERMES->borg_.get();
  traits_ = &HERMES->traits_;
  header_->id_alloc_ = 1;

  // Put the node_id in SHM
  header_->node_id_ = rpc_->node_id_;

  // Create the metadata maps
  blob_id_map_ = hipc::make_ref<BLOB_ID_MAP_T>(header_->blob_id_map,
                                               alloc_, 16384);
  blob_map_ = hipc::make_ref<BLOB_MAP_T>(header_->blob_map,
                                         alloc_, 16384);
  tag_id_map_ = hipc::make_ref<TAG_ID_MAP_T>(header_->tag_id_map,
                                             alloc_, 256);
  tag_map_ = hipc::make_ref<TAG_MAP_T>(header_->tag_map,
                                       alloc_, 256);
  trait_id_map_ = hipc::make_ref<TRAIT_ID_MAP_T>(header_->trait_id_map,
                                                 alloc_, 256);
  trait_map_ = hipc::make_ref<TRAIT_MAP_T>(header_->trait_map,
                                           alloc_, 256);

  // Create the DeviceInfo vector
  devices_ = hipc::make_ref<hipc::vector<DeviceInfo>>(
      header_->devices_, HERMES->main_alloc_, *config->devices_);

  // Create the TargetInfo vector
  targets_ = hipc::make_ref<hipc::vector<TargetInfo>>(
      header_->targets_, HERMES->main_alloc_);
  targets_->reserve(devices_->size());
  int dev_id = 0;
  float maxbw = 0;
  for (hipc::Ref<DeviceInfo> dev_info : *config->devices_) {
    targets_->emplace_back(
        TargetId(rpc_->node_id_, dev_id, dev_id),
        dev_info->header_->capacity_,
        dev_info->header_->capacity_,
        dev_info->header_->bandwidth_,
        dev_info->header_->latency_);
    if (maxbw < dev_info->header_->bandwidth_) {
      maxbw = dev_info->header_->bandwidth_;
    }
    ++dev_id;
  }

  // Assign a score to each target
  for (hipc::Ref<TargetInfo> target_info : *targets_) {
    target_info->score_ = (target_info->bandwidth_ / maxbw);
  }

  // Create the log used to track I/O pattern
  io_pattern_log_ = hipc::make_ref<IO_PATTERN_LOG_T>(
      header_->io_pattern_log_, alloc_);

  // Initialize local maps
  local_init();
}

/**
 * Explicitly destroy the MetadataManager
 * */
void MetadataManager::shm_destroy_main() {
  blob_id_map_.shm_destroy();
  blob_map_.shm_destroy();
  tag_id_map_.shm_destroy();
  tag_map_.shm_destroy();
  trait_id_map_.shm_destroy();
  trait_map_.shm_destroy();
  targets_.shm_destroy();
  devices_.shm_destroy();
  io_pattern_log_.shm_destroy();
}

/**====================================
 * SHM Deserialize
 * ===================================*/

/**
 * Store the MetadataManager in shared memory.
 * Does not require anything to be initialized.
 * */
void MetadataManager::shm_deserialize_main() {
  rpc_ = &HERMES->rpc_;
  borg_ = HERMES->borg_.get();
  traits_ = &HERMES->traits_;
  auto alloc = HERMES->main_alloc_;
  blob_id_map_ = hipc::Ref<BLOB_ID_MAP_T>(header_->blob_id_map, alloc);
  blob_map_ = hipc::Ref<BLOB_MAP_T>(header_->blob_map, alloc);
  tag_id_map_ = hipc::Ref<TAG_ID_MAP_T>(header_->tag_id_map, alloc);
  tag_map_ = hipc::Ref<TAG_MAP_T>(header_->tag_map, alloc);
  trait_id_map_ = hipc::Ref<TRAIT_ID_MAP_T>(header_->trait_id_map, alloc);
  trait_map_ = hipc::Ref<TRAIT_MAP_T>(header_->trait_map, alloc);
  targets_ = hipc::Ref<hipc::vector<TargetInfo>>(header_->targets_, alloc);
  devices_ = hipc::Ref<hipc::vector<DeviceInfo>>(header_->devices_, alloc);
  io_pattern_log_ = hipc::Ref<IO_PATTERN_LOG_T>(header_->io_pattern_log_,
                                                alloc);

  // Initialize local maps
  local_init();
}

/**====================================
 * Bucket Operations
 * ===================================*/

/**
 * Get the size of the bucket. May consider the impact the bucket has
 * on the backing storage system's statistics using the io_ctx.
 * */
size_t MetadataManager::LocalGetBucketSize(TagId bkt_id,
                                           bool backend) {
  AUTO_TRACE(1);
  // Acquire MD read lock (reading tag_map)
  ScopedRwReadLock tag_map_lock(header_->lock_[kTagMapLock]);
  auto iter = tag_map_->find(bkt_id);
  if (iter == tag_map_->end()) {
    return 0;
  }
  hipc::Ref<hipc::pair<TagId, TagInfo>> info = (*iter);
  TagInfo &bkt_info = *info->second_;
  if (backend) {
    return bkt_info.header_->client_state_.true_size_;
  } else {
    return bkt_info.header_->internal_size_;
  }
}

/**
 * Update \a bkt_id BUCKET stats
 * */
bool MetadataManager::LocalUpdateBucketSize(TagId bkt_id,
                                            ssize_t delta,
                                            BucketUpdate mode) {
  AUTO_TRACE(1);
  // Acquire MD read lock (reading tag_map)
  ScopedRwReadLock tag_map_lock(header_->lock_[kTagMapLock]);
  auto iter = tag_map_->find(bkt_id);
  if (iter == tag_map_->end()) {
    return 0;
  }
  hipc::Ref<hipc::pair<TagId, TagInfo>> info = (*iter);
  TagInfo &bkt_info = *info->second_;
  switch (mode) {
    case BucketUpdate::kInternal: {
      bkt_info.header_->internal_size_ += delta;
      break;
    }
    case BucketUpdate::kBackend: {
      bkt_info.header_->client_state_.true_size_ = std::max(
          bkt_info.header_->client_state_.true_size_, (size_t)delta);
      break;
    }
    case BucketUpdate::kNone: {
      HELOG(kFatal, "Cannot have kNone bucket type")
    }
  }
  HILOG(kDebug, "Updating the size of bucket: {} by: {} bytes"
                " New internal size: {}"
                " New backend size: {}",
        bkt_info.name_->str(), delta,
        bkt_info.header_->internal_size_,
        bkt_info.header_->client_state_.true_size_)
  return true;
}

/**
 * Clear \a bkt_id bucket
 * */
bool MetadataManager::LocalClearBucket(TagId bkt_id, bool backend) {
  AUTO_TRACE(1);
  ScopedRwWriteLock tag_map_lock(header_->lock_[kTagMapLock]);
  auto iter = tag_map_->find(bkt_id);
  if (iter == tag_map_->end()) {
    return true;
  }
  hipc::Ref<hipc::pair<TagId, TagInfo>> info = (*iter);
  TagInfo &bkt_info = *info->second_;
  for (hipc::Ref<BlobId> blob_id : *bkt_info.blobs_) {
    GlobalDestroyBlob(bkt_id, *blob_id);
  }
  bkt_info.blobs_->clear();
  bkt_info.header_->internal_size_ = 0;
  if (backend) {
    bkt_info.header_->client_state_.true_size_ = 0;
  }
  return true;
}

/**====================================
 * Blob Operations
 * ===================================*/

/**
 * Tag a blob
 *
 * @param blob_id id of the blob being tagged
 * @param tag_name tag name
 * */
Status MetadataManager::LocalTagBlob(
    BlobId blob_id, TagId tag_id) {
  AUTO_TRACE(1);
  // Acquire MD read lock (read blob_map_)
  ScopedRwReadLock blob_map_lock(header_->lock_[kBlobMapLock]);
  auto iter = blob_map_->find(blob_id);
  if (iter == blob_map_->end()) {
    return Status();  // TODO(llogan): error status
  }
  hipc::Ref<hipc::pair<BlobId, BlobInfo>> info = (*iter);
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
  AUTO_TRACE(1);
  // Acquire MD read lock (reading blob_map_)
  ScopedRwReadLock blob_map_lock(header_->lock_[kBlobMapLock]);
  auto iter = blob_map_->find(blob_id);
  if (iter == blob_map_->end()) {
    return false;
  }
  // Get the blob info
  hipc::Ref<hipc::pair<BlobId, BlobInfo>> info = (*iter);
  BlobInfo &blob_info = *info->second_;
  // Iterate over tags
  for (hipc::Ref<TagId> tag_cmp : *blob_info.tags_) {
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
 * */
std::pair<BlobId, bool> MetadataManager::LocalTryCreateBlob(
    TagId bkt_id,
    const std::string &blob_name) {
  AUTO_TRACE(1);
  // Acquire MD write lock (modify blob_map_)
  ScopedRwWriteLock blob_map_lock(header_->lock_[kBlobMapLock]);
  // Get internal blob name
  hipc::uptr<hipc::charbuf> internal_blob_name =
      CreateBlobName(bkt_id, blob_name);
  // Create unique ID for the Blob
  BlobId blob_id;
  blob_id.unique_ = header_->id_alloc_.fetch_add(1);
  blob_id.node_id_ = rpc_->node_id_;
  bool did_create = blob_id_map_->try_emplace(*internal_blob_name, blob_id);
  if (did_create) {
    blob_map_->emplace(blob_id);
    auto iter = blob_map_->find(blob_id);
    hipc::Ref<hipc::pair<BlobId, BlobInfo>> info = (*iter);
    BlobInfo &blob_info = *info->second_;
    (*blob_info.name_) = blob_name;
    blob_info.header_->tag_id_ = bkt_id;
    blob_info.header_->blob_size_ = 0;
  }
  return std::pair<BlobId, bool>(blob_id, did_create);
}

/**
 * Creates the blob metadata
 *
 * @param bkt_id id of the bucket
 * @param blob_name semantic blob name
 * @param data the data being placed
 * @param buffers the buffers to place data in
 * */
std::tuple<BlobId, bool, size_t>
MetadataManager::LocalPutBlobMetadata(TagId bkt_id,
                                      const std::string &blob_name,
                                      size_t blob_size,
                                      std::vector<BufferInfo> &buffers,
                                      float score) {
  AUTO_TRACE(1);
  size_t orig_blob_size = 0;
  // Acquire MD write lock (modify blob_map_)
  ScopedRwWriteLock blob_map_lock(header_->lock_[kBlobMapLock]);
  // Get internal blob name
  hipc::uptr<hipc::charbuf> internal_blob_name =
      CreateBlobName(bkt_id, blob_name);
  // Create unique ID for the Blob
  BlobId blob_id;
  blob_id.unique_ = header_->id_alloc_.fetch_add(1);
  blob_id.node_id_ = rpc_->node_id_;
  bool did_create = blob_id_map_->try_emplace(*internal_blob_name, blob_id);
  if (did_create) {
    HILOG(kDebug, "Creating new blob: {}", blob_name)
    blob_map_->emplace(blob_id);
    auto iter = blob_map_->find(blob_id);
    hipc::Ref<hipc::pair<BlobId, BlobInfo>> info = (*iter);
    BlobInfo &blob_info = *info->second_;
    (*blob_info.name_) = blob_name;
    (*blob_info.buffers_) = buffers;
    blob_info.header_->blob_id_ = blob_id;
    blob_info.header_->tag_id_ = bkt_id;
    blob_info.header_->blob_size_ = blob_size;
    blob_info.header_->score_ = score;
    blob_info.header_->mod_count_ = 1;
    blob_info.header_->last_flush_ = 0;
  } else {
    HILOG(kDebug, "Found existing blob: {}", blob_name)
    blob_id = *(*blob_id_map_)[*internal_blob_name];
    auto iter = blob_map_->find(blob_id);
    hipc::Ref<hipc::pair<BlobId, BlobInfo>> info = (*iter);
    BlobInfo &blob_info = *info->second_;
    // Acquire blob_info write lock before modifying buffers
    ScopedRwWriteLock(blob_info.header_->lock_[0]);
    (*blob_info.buffers_) = buffers;
    blob_info.header_->score_ = score;
    blob_info.header_->mod_count_.fetch_add(1);
  }
  return std::tuple<BlobId, bool, size_t>(blob_id, did_create, orig_blob_size);
}

/**
 * Get \a blob_name blob from \a bkt_id bucket
 * */
BlobId MetadataManager::LocalGetBlobId(TagId bkt_id,
                                       const std::string &blob_name) {
  AUTO_TRACE(1);
  // Acquire MD read lock (read blob_id_map_)
  ScopedRwReadLock blob_map_lock(header_->lock_[kBlobMapLock]);
  hipc::uptr<hipc::charbuf> internal_blob_name =
      CreateBlobName(bkt_id, blob_name);
  auto iter = blob_id_map_->find(*internal_blob_name);
  if (iter == blob_id_map_->end()) {
    return BlobId::GetNull();
  }
  hipc::Ref<hipc::pair<hipc::charbuf, BlobId>> info = *iter;
  return *info->second_;
}

/**
 * Get \a blob_name BLOB name from \a blob_id BLOB id
 * */
std::string MetadataManager::LocalGetBlobName(BlobId blob_id) {
  AUTO_TRACE(1);
  // Acquire MD read lock (read blob_id_map_)
  ScopedRwReadLock blob_map_lock(header_->lock_[kBlobMapLock]);
  auto iter = blob_map_->find(blob_id);
  if (iter == blob_map_->end()) {
    return "";
  }
  hipc::Ref<hipc::pair<BlobId, BlobInfo>> info = *iter;
  BlobInfo &blob_info = *info->second_;
  return blob_info.name_->str();
}

/**
   * Get \a score from \a blob_id BLOB id
   * */
float MetadataManager::LocalGetBlobScore(BlobId blob_id) {
  AUTO_TRACE(1);
  // Acquire MD read lock (read blob_id_map_)
  ScopedRwReadLock blob_map_lock(header_->lock_[kBlobMapLock]);
  auto iter = blob_map_->find(blob_id);
  if (iter == blob_map_->end()) {
    return -1;
  }
  hipc::Ref<hipc::pair<BlobId, BlobInfo>> info = *iter;
  BlobInfo &blob_info = *info->second_;
  return blob_info.header_->score_;
}

/**
 * Lock the blob
 * */
bool MetadataManager::LocalLockBlob(BlobId blob_id,
                                    MdLockType lock_type) {
  AUTO_TRACE(1);
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
  AUTO_TRACE(1);
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
  AUTO_TRACE(1);
  // Acquire MD read lock (read blob_map_)
  ScopedRwReadLock blob_map_lock(header_->lock_[kBlobMapLock]);
  auto iter = blob_map_->find(blob_id);
  if (iter == blob_map_->end()) {
    return std::vector<BufferInfo>();
  }
  hipc::Ref<hipc::pair<BlobId, BlobInfo>> info = (*iter);
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
                                      const std::string &new_blob_name) {
  AUTO_TRACE(1);
  // Acquire MD write lock (modify blob_id_map_)
  ScopedRwWriteLock blob_map_lock(header_->lock_[kBlobMapLock]);
  auto iter = (*blob_map_).find(blob_id);
  if (iter == blob_map_->end()) {
    return true;
  }
  hipc::Ref<hipc::pair<BlobId, BlobInfo>> info = (*iter);
  BlobInfo &blob_info = *info->second_;
  hipc::uptr<hipc::charbuf> old_blob_name_internal =
      CreateBlobName(bkt_id, *blob_info.name_);
  hipc::uptr<hipc::charbuf> new_blob_name_internal =
      CreateBlobName(bkt_id, new_blob_name);
  blob_id_map_->erase(*old_blob_name_internal);
  blob_id_map_->emplace(*new_blob_name_internal, blob_id);
  return true;
}

/**
 * Destroy \a blob_id blob in \a bkt_id bucket
 * */
bool MetadataManager::LocalDestroyBlob(TagId bkt_id,
                                       BlobId blob_id) {
  AUTO_TRACE(1);
  // Acquire MD write lock (modify blob_id_map & blob_map_)
  ScopedRwWriteLock blob_map_lock(header_->lock_[kBlobMapLock]);
  (void)bkt_id;
  auto iter = (*blob_map_).find(blob_id);
  if (iter.is_end()) {
    return true;
  }
  hipc::Ref<hipc::pair<BlobId, BlobInfo>> info = (*iter);
  BlobInfo &blob_info = *info->second_;
  hipc::uptr<hipc::charbuf> blob_name =
      CreateBlobName(bkt_id, *blob_info.name_);
  blob_id_map_->erase(*blob_name);
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
  AUTO_TRACE(1);
  HILOG(kDebug, "Clearing all buckets and blobs")
  ScopedRwWriteLock tag_map_lock(header_->lock_[kTagMapLock]);
  tag_id_map_->clear();
  tag_map_->clear();
  ScopedRwWriteLock blob_map_lock(header_->lock_[kBlobMapLock]);
  blob_id_map_->clear();
  blob_map_->clear();
}

/**
 * Destroy all blobs + buckets globally
 * */
void MetadataManager::GlobalClear() {
  AUTO_TRACE(1);
  for (i32 i = 0; i < (i32)rpc_->hosts_.size(); ++i) {
    i32 node_id = i + 1;
    if (NODE_ID_IS_LOCAL(node_id)) {
      LocalClear();
    } else {
      rpc_->Call<bool>(node_id, "RpcClear");
    }
  }
}

/**====================================
 * Tag Operations
 * ===================================*/

/**
 * Get or create a bucket with \a bkt_name bucket name
 * */
std::pair<TagId, bool>
MetadataManager::LocalGetOrCreateTag(const std::string &tag_name,
                                     bool owner,
                                     std::vector<TraitId> &traits,
                                     size_t backend_size) {
  AUTO_TRACE(1);
  // Acquire MD write lock (modifying tag_map)
  ScopedRwWriteLock tag_map_lock(header_->lock_[kTagMapLock]);

  // Create unique ID for the Bucket
  TagId tag_id;
  tag_id.unique_ = header_->id_alloc_.fetch_add(1);
  tag_id.node_id_ = rpc_->node_id_;
  auto tag_name_shm = hipc::make_uptr<hipc::string>(tag_name);
  bool did_create = tag_id_map_->try_emplace(*tag_name_shm, tag_id);

  // Emplace bucket if it does not already exist
  if (did_create) {
    HILOG(kDebug, "Creating tag for the first time: {}", tag_name)
    tag_map_->emplace(tag_id);
    auto iter = tag_map_->find(tag_id);
    hipc::Ref<hipc::pair<TagId, TagInfo>> info_pair = *iter;
    TagInfo &info = *info_pair->second_;
    (*info.name_) = *tag_name_shm;
    info.header_->internal_size_ = 0;
    info.header_->client_state_.true_size_ = backend_size;
    info.header_->tag_id_ = tag_id;
    info.header_->owner_ = owner;
  } else {
    HILOG(kDebug, "Found existing tag: {}", tag_name)
    auto iter = tag_id_map_->find(*tag_name_shm);
    hipc::Ref<hipc::pair<hipc::charbuf, TagId>> id_info = (*iter);
    tag_id = *id_info->second_;
  }

  return {tag_id, did_create};
}

/**
 * Get the TagId with \a bkt_name bucket name
 * */
TagId MetadataManager::LocalGetTagId(const std::string &tag_name) {
  AUTO_TRACE(1);
  // Acquire MD read lock (reading tag_id_map)
  ScopedRwReadLock tag_map_lock(header_->lock_[kTagMapLock]);
  auto tag_name_shm = hipc::make_uptr<hipc::string>(tag_name);
  auto iter = tag_id_map_->find(*tag_name_shm);
  if (iter == tag_id_map_->end()) {
    return TagId::GetNull();
  }
  hipc::Ref<hipc::pair<hipc::charbuf, TagId>> info = (*iter);
  TagId bkt_id = *info->second_;
  return bkt_id;
}

/**
 * Rename a tag
 * */
bool MetadataManager::LocalRenameTag(TagId tag_id,
                                     const std::string &new_name) {
  AUTO_TRACE(1);
  // Acquire MD write lock (modifying tag_map_)
  ScopedRwWriteLock tag_map_lock(header_->lock_[kTagMapLock]);
  auto iter = tag_map_->find(tag_id);
  if (iter == tag_map_->end()) {
    return true;
  }
  auto new_name_shm = hipc::make_uptr<hipc::string>(new_name);
  hipc::Ref<hipc::pair<TagId, TagInfo>> info = (*iter);
  hipc::string &old_bkt_name = *info->second_->name_;
  tag_id_map_->erase(old_bkt_name);
  tag_id_map_->emplace(*new_name_shm, tag_id);
  return true;
}

/**
 * Delete a tag
 * */
bool MetadataManager::LocalDestroyTag(TagId tag_id) {
  AUTO_TRACE(1);
  // Acquire MD write lock (modifying tag_map_)
  ScopedRwWriteLock tag_map_lock(header_->lock_[kTagMapLock]);
  auto iter = tag_map_->find(tag_id);
  if (iter.is_end()) {
    return true;
  }
  hipc::Ref<hipc::pair<TagId, TagInfo>> info_pair = *iter;
  auto &tag_info = *info_pair->second_;
  tag_id_map_->erase(*tag_info.name_);
  tag_map_->erase(tag_id);
  return true;
}

/**
 * Add a blob to a tag index
 * */
Status MetadataManager::LocalTagAddBlob(TagId tag_id,
                                        BlobId blob_id) {
  AUTO_TRACE(1);
  // Acquire MD write lock (modify tag_map_)
  ScopedRwWriteLock tag_map_lock(header_->lock_[kTagMapLock]);
  auto iter = tag_map_->find(tag_id);
  if (iter.is_end()) {
    return Status();
  }
  hipc::Ref<hipc::pair<TagId, TagInfo>> blob_list = (*iter);
  TagInfo &info = *blob_list->second_;
  info.blobs_->emplace_back(blob_id);
  return Status();
}

/**
 * Remove a blob from a tag index.
 * */
Status MetadataManager::LocalTagRemoveBlob(TagId tag_id,
                                           BlobId blob_id) {
  AUTO_TRACE(1);
  // Acquire MD write lock (modify tag_map_)
  ScopedRwWriteLock tag_map_lock(header_->lock_[kTagMapLock]);
  auto iter = tag_map_->find(tag_id);
  if (iter.is_end()) {
    return Status();
  }
  hipc::Ref<hipc::pair<TagId, TagInfo>> blob_list = (*iter);
  TagInfo &info = *blob_list->second_;
  info.blobs_->erase(blob_id);
  return Status();
}


/**
 * Find all blobs pertaining to a tag
 * */
std::vector<BlobId> MetadataManager::LocalGroupByTag(TagId tag_id) {
  AUTO_TRACE(1);
  // Acquire MD read lock (read tag_map_)
  ScopedRwReadLock tag_map_lock(header_->lock_[kTagMapLock]);
  // Get the tag info
  auto iter = tag_map_->find(tag_id);
  if (iter.is_end()) {
    return std::vector<BlobId>();
  }
  auto tag_info_pair = *iter;
  hipc::Ref<TagInfo> &tag_info = tag_info_pair->second_;
  // Convert slist into std::vector
  return hshm::to_stl_vector<BlobId>(*tag_info->blobs_);
}

/**
 * Add a trait to a tag index. Create tag if it does not exist.
 * */
bool MetadataManager::LocalTagAddTrait(TagId tag_id, TraitId trait_id) {
  AUTO_TRACE(1);
  HILOG(kDebug, "Adding trait {}.{} to tag {}.{}",
        trait_id.node_id_, trait_id.unique_,
        tag_id.node_id_, tag_id.unique_)
  // Acquire MD read lock (read tag_map_)
  ScopedRwReadLock tag_map_lock(header_->lock_[kTagMapLock]);
  auto iter = tag_map_->find(tag_id);
  if (iter.is_end()) {
    return true;
  }
  auto tag_info_pair = *iter;
  hipc::Ref<TagInfo> &tag_info = tag_info_pair->second_;
  tag_info->traits_->emplace_back(trait_id);
  return true;
}

/**
 * Find all traits pertaining to a tag
 * */
std::vector<TraitId> MetadataManager::LocalTagGetTraits(TagId tag_id) {
  AUTO_TRACE(1);
  // Acquire MD read lock (read tag_map_)
  ScopedRwReadLock tag_map_lock(header_->lock_[kTagMapLock]);
  auto iter = tag_map_->find(tag_id);
  if (iter.is_end()) {
    return std::vector<TraitId>();
  }
  auto tag_info_pair = *iter;
  hipc::Ref<TagInfo> &tag_info = tag_info_pair->second_;
  return hshm::to_stl_vector<TraitId>(*tag_info->traits_);
}

/**====================================
 * Statistics Operations
 * ===================================*/

/** Add an I/O statistic to the internal log */
void MetadataManager::AddIoStat(TagId tag_id,
                                BlobId blob_id,
                                size_t blob_size,
                                IoType type) {
  if (!enable_io_tracing_) {
    return;
  }
  ScopedRwWriteLock io_pattern_lock(header_->lock_[kIoPatternLog]);
  IoStat stat;
  stat.blob_id_ = blob_id;
  stat.tag_id_ = tag_id;
  stat.blob_size_ = blob_size;
  stat.type_ = type;
  stat.rank_ = 0;
  if (is_mpi_) {
    MPI_Comm_rank(MPI_COMM_WORLD, &stat.rank_);
  }
  io_pattern_log_->emplace_back(stat);
}

/** Add an I/O statistic to the internal log */
void MetadataManager::ClearIoStats(size_t count) {
  ScopedRwWriteLock io_pattern_lock(header_->lock_[kIoPatternLog]);
  auto first = io_pattern_log_->begin();
  auto end = io_pattern_log_->begin() + count;
  io_pattern_log_->erase(first, end);
}

}  // namespace hermes
