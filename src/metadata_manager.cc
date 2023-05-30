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
#include <mpi.h>

namespace hermes {

/** Namespace simplification for Bucket */
using api::Bucket;

/**
 * Explicitly initialize the MetadataManager
 * Requires RPC to be initialized
 * */
void MetadataManager::shm_init(hipc::ShmArchive<MetadataManagerShm> &header,
                               hipc::Allocator *alloc,
                               ServerConfig *config) {
  shm_deserialize(header);
  header_->id_alloc_ = 1;

  // Put the node_id in SHM
  header_->node_id_ = rpc_->node_id_;

  // Create the metadata maps
  HSHM_MAKE_AR(header_->blob_id_map_, alloc, 32000000)
  HSHM_MAKE_AR(header_->blob_map_, alloc, 32000000)
  HSHM_MAKE_AR(header_->tag_id_map_, alloc, 32000000)
  HSHM_MAKE_AR(header_->tag_map_, alloc, 32000000)
  HSHM_MAKE_AR(header_->trait_id_map_, alloc, 256)
  HSHM_MAKE_AR(header_->trait_map_, alloc, 256)

  // Create the DeviceInfo vector
  HSHM_MAKE_AR(header_->devices_, alloc, *config->devices_)

  // Create the TargetInfo vector
  HSHM_MAKE_AR0(header_->targets_, alloc)
  targets_->reserve(devices_->size());
  int dev_id = 0;
  float maxbw = 0;
  for (DeviceInfo &dev_info : *config->devices_) {
    targets_->emplace_back(
        TargetId(rpc_->node_id_, dev_id, dev_id),
        dev_info.capacity_,
        dev_info.capacity_,
        dev_info.bandwidth_,
        dev_info.latency_);
    if (maxbw < dev_info.bandwidth_) {
      maxbw = dev_info.bandwidth_;
    }
    ++dev_id;
  }

  // Assign a score to each target
  for (TargetInfo &target_info : *targets_) {
    target_info.score_ = (target_info.bandwidth_ / maxbw);
  }

  // Create the log used to track I/O pattern
  HSHM_MAKE_AR(header_->io_pattern_log_, alloc, 8192);
}

/**====================================
 * SHM Deserialize
 * ===================================*/

/**
 * Store the MetadataManager in shared memory.
 * Does not require anything to be initialized.
 * */
void MetadataManager::shm_deserialize(
    hipc::ShmArchive<MetadataManagerShm> &header) {
  rpc_ = &HERMES->rpc_;
  borg_ = &HERMES->borg_;
  traits_ = &HERMES->traits_;

  header_ = header.get();
  blob_id_map_ = header_->blob_id_map_.get();
  blob_map_ = header_->blob_map_.get();
  tag_id_map_ = header_->tag_id_map_.get();
  tag_map_ = header_->tag_map_.get();
  trait_id_map_ = header_->trait_id_map_.get();
  trait_map_ = header_->trait_map_.get();
  targets_ = header_->targets_.get();
  devices_ = header_->devices_.get();
  io_pattern_log_ = header_->io_pattern_log_.get();
}

/**====================================
 * Destructor
 * ===================================*/

/**
 * Explicitly destroy the MetadataManager
 * */
void MetadataManager::shm_destroy() {
  (*blob_id_map_).shm_destroy();
  (*blob_map_).shm_destroy();
  (*tag_id_map_).shm_destroy();
  (*tag_map_).shm_destroy();
  (*trait_id_map_).shm_destroy();
  (*trait_map_).shm_destroy();
  (*targets_).shm_destroy();
  (*devices_).shm_destroy();
  (*io_pattern_log_).shm_destroy();
}

/**====================================
 * Bucket Operations
 * ===================================*/

/**
 * Get the size of the bucket. May consider the impact the bucket has
 * on the backing storage system's statistics using the io_ctx.
 * */
size_t MetadataManager::LocalGetBucketSize(TagId bkt_id) {
  AUTO_TRACE(1);
  // Acquire MD read lock (reading tag_map)
  ScopedRwReadLock tag_map_lock(header_->lock_[kTagMapLock],
                                kMDM_LocalGetBucketSize);
  auto iter = tag_map_->find(bkt_id);
  if (iter == tag_map_->end()) {
    return 0;
  }
  hipc::pair<TagId, TagInfo> &info = (*iter);
  TagInfo &bkt_info = info.GetSecond();
  return bkt_info.internal_size_;
}

/**
 * Update \a bkt_id BUCKET stats
 * */
bool MetadataManager::LocalSetBucketSize(TagId bkt_id,
                                         size_t new_size) {
  AUTO_TRACE(1);
  // Acquire MD read lock (reading tag_map)
  ScopedRwReadLock tag_map_lock(header_->lock_[kTagMapLock],
                                kMDM_LocalSetBucketSize);
  auto iter = tag_map_->find(bkt_id);
  if (iter == tag_map_->end()) {
    return false;
  }
  hipc::pair<TagId, TagInfo> &info = (*iter);
  TagInfo &bkt_info = info.GetSecond();
  bkt_info.internal_size_ = new_size;
  HILOG(kDebug, "The new size of the bucket {} ({}) is {}",
        bkt_info.name_->str(), bkt_id,
        bkt_info.internal_size_)
  return true;
}

/**
 * Lock the bucket
 * */
bool MetadataManager::LocalLockBucket(TagId bkt_id,
                                      MdLockType lock_type) {
  AUTO_TRACE(1);
  if (bkt_id.IsNull()) {
    return false;
  }
  ScopedRwReadLock tag_map_lock(header_->lock_[kTagMapLock],
                                kMDM_LocalLockBucket);
  return LockMdObject(*tag_map_, bkt_id, lock_type);
}

/**
 * Unlock the bucket
 * */
bool MetadataManager::LocalUnlockBucket(TagId bkt_id,
                                        MdLockType lock_type) {
  AUTO_TRACE(1);
  if (bkt_id.IsNull()) {
    return false;
  }
  ScopedRwReadLock tag_map_lock(header_->lock_[kTagMapLock],
                                kMDM_LocalUnlockBucket);
  return UnlockMdObject(*tag_map_, bkt_id, lock_type);
}

/**
 * Clear \a bkt_id bucket
 * */
bool MetadataManager::LocalClearBucket(TagId bkt_id) {
  AUTO_TRACE(1);
  HILOG(kDebug, "Clearing the bucket {}", bkt_id)
  // Avoid flushing
  ScopedRwReadLock flush_lock(header_->lock_[kFlushLock], kMDM_LocalClear);
  // Acquire tag map write lock
  ScopedRwWriteLock tag_map_lock(header_->lock_[kTagMapLock],
                                 kMDM_LocalClearBucket);
  auto iter = tag_map_->find(bkt_id);
  if (iter == tag_map_->end()) {
    return true;
  }
  hipc::pair<TagId, TagInfo> &info = (*iter);
  TagInfo &bkt_info = info.GetSecond();
  for (BlobId &blob_id : *bkt_info.blobs_) {
    GlobalDestroyBlob(bkt_id, blob_id);
  }
  bkt_info.blobs_->clear();
  bkt_info.internal_size_ = 0;
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
  ScopedRwReadLock blob_map_lock(header_->lock_[kBlobMapLock],
                                 kMDM_LocalTagBlob);
  auto iter = blob_map_->find(blob_id);
  if (iter == blob_map_->end()) {
    return Status();  // TODO(llogan): error status
  }
  hipc::pair<BlobId, BlobInfo>& info = (*iter);
  BlobInfo &blob_info = info.GetSecond();
  // Acquire blob_info read lock (read buffers)
  ScopedRwReadLock blob_info_lock(blob_info.lock_[0],
                                  kMDM_LocalTagBlob);
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
  ScopedRwReadLock blob_map_lock(header_->lock_[kBlobMapLock],
                                 kMDM_LocalBlobHasTag);
  auto iter = blob_map_->find(blob_id);
  if (iter == blob_map_->end()) {
    return false;
  }
  // Get the blob info
  hipc::pair<BlobId, BlobInfo>& info = (*iter);
  BlobInfo &blob_info = info.GetSecond();
  // Iterate over tags
  for (TagId &tag_cmp : *blob_info.tags_) {
    if (tag_cmp == tag_id) {
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
  // Avoid flushing
  ScopedRwReadLock flush_lock(header_->lock_[kFlushLock], kMDM_LocalClear);
  // Acquire MD write lock (modify blob_map_)
  ScopedRwWriteLock blob_map_lock(header_->lock_[kBlobMapLock],
                                  kMDM_LocalTryCreateBlob);
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
    hipc::pair<BlobId, BlobInfo>& info = (*iter);
    BlobInfo &blob_info = info.GetSecond();
    (*blob_info.name_) = blob_name;
    blob_info.blob_id_ = blob_id;
    blob_info.tag_id_ = bkt_id;
    blob_info.blob_size_ = 0;
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
  // Avoid flushing
  ScopedRwReadLock flush_lock(header_->lock_[kFlushLock], kMDM_LocalClear);
  // Acquire MD write lock (modify blob_map_)
  ScopedRwWriteLock blob_map_lock(header_->lock_[kBlobMapLock],
                                  kMDM_LocalPutBlobMetadata);
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
    HILOG(kDebug, "Creating new blob: {}. Total num blobs: {}",
          blob_name, blob_map_->size())
    auto iter = blob_map_->find(blob_id);
    hipc::pair<BlobId, BlobInfo>& info = (*iter);
    BlobInfo &blob_info = info.GetSecond();
    (*blob_info.name_) = blob_name;
    (*blob_info.buffers_) = buffers;
    blob_info.blob_id_ = blob_id;
    blob_info.tag_id_ = bkt_id;
    blob_info.blob_size_ = blob_size;
    blob_info.score_ = score;
    blob_info.mod_count_ = 0;
    blob_info.access_freq_ = 0;
    blob_info.last_flush_ = 0;
    blob_info.UpdateWriteStats();
  } else {
    HILOG(kDebug, "Found existing blob: {}. Total num blobs: {}",
          blob_name, blob_map_->size())
    blob_id = (*blob_id_map_)[*internal_blob_name];
    auto iter = blob_map_->find(blob_id);
    hipc::pair<BlobId, BlobInfo>& info = (*iter);
    BlobInfo &blob_info = info.GetSecond();
    // Acquire blob_info write lock before modifying buffers
    ScopedRwWriteLock blob_info_lock(blob_info.lock_[0],
                                     kMDM_LocalPutBlobMetadata);
    (*blob_info.buffers_) = buffers;
    blob_info.blob_size_ = blob_size;
    blob_info.score_ = score;
    blob_info.UpdateWriteStats();
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
  ScopedRwReadLock blob_map_lock(header_->lock_[kBlobMapLock],
                                 kMDM_LocalGetBlobId);
  hipc::uptr<hipc::charbuf> internal_blob_name =
      CreateBlobName(bkt_id, blob_name);
  auto iter = blob_id_map_->find(*internal_blob_name);
  if (iter == blob_id_map_->end()) {
    return BlobId::GetNull();
  }
  hipc::pair<hipc::charbuf, BlobId> &info = *iter;
  return info.GetSecond();
}

/**
 * Get \a blob_name BLOB name from \a blob_id BLOB id
 * */
std::string MetadataManager::LocalGetBlobName(BlobId blob_id) {
  AUTO_TRACE(1);
  // Acquire MD read lock (read blob_id_map_)
  ScopedRwReadLock blob_map_lock(header_->lock_[kBlobMapLock],
                                 kMDM_LocalGetBlobName);
  auto iter = blob_map_->find(blob_id);
  if (iter == blob_map_->end()) {
    return "";
  }
  hipc::pair<BlobId, BlobInfo>& info = *iter;
  BlobInfo &blob_info = info.GetSecond();
  return blob_info.name_->str();
}

/**
   * Get \a score from \a blob_id BLOB id
   * */
float MetadataManager::LocalGetBlobScore(BlobId blob_id) {
  AUTO_TRACE(1);
  // Acquire MD read lock (read blob_id_map_)
  ScopedRwReadLock blob_map_lock(header_->lock_[kBlobMapLock],
                                 kMDM_LocalGetBlobScore);
  auto iter = blob_map_->find(blob_id);
  if (iter == blob_map_->end()) {
    return -1;
  }
  hipc::pair<BlobId, BlobInfo>& info = *iter;
  BlobInfo &blob_info = info.GetSecond();
  return blob_info.score_;
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
  ScopedRwReadLock blob_map_lock(header_->lock_[kBlobMapLock],
                                 kMDM_LocalLockBlob);
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
  ScopedRwReadLock blob_map_lock(header_->lock_[kBlobMapLock],
                                 kMDM_LocalUnlockBlob);
  return UnlockMdObject(*blob_map_, blob_id, lock_type);
}

/**
 * Get \a blob_id blob's buffers
 * */
std::vector<BufferInfo> MetadataManager::LocalGetBlobBuffers(BlobId blob_id) {
  AUTO_TRACE(1);
  // Acquire MD read lock (read blob_map_)
  ScopedRwReadLock blob_map_lock(header_->lock_[kBlobMapLock],
                                 kMDM_LocalGetBlobBuffers);
  auto iter = blob_map_->find(blob_id);
  if (iter == blob_map_->end()) {
    return std::vector<BufferInfo>();
  }
  hipc::pair<BlobId, BlobInfo>& info = (*iter);
  BlobInfo &blob_info = info.GetSecond();
  // Acquire blob_info read lock
  ScopedRwReadLock blob_info_lock(blob_info.lock_[0],
                                  kMDM_LocalGetBlobBuffers);
  blob_info.UpdateReadStats();
  auto vec = blob_info.buffers_->vec();
  return vec;
}

/**
 * Rename \a blob_id blob to \a new_blob_name new blob name
 * in \a bkt_id bucket.
 * */
bool MetadataManager::LocalRenameBlob(TagId bkt_id, BlobId blob_id,
                                      const std::string &new_blob_name) {
  HILOG(kDebug, "Renaming the blob: {}.{}", blob_id.node_id_, blob_id.unique_)
  AUTO_TRACE(1);
  // Avoid flushing
  ScopedRwReadLock flush_lock(header_->lock_[kFlushLock], kMDM_LocalClear);
  // Acquire MD write lock (modify blob_id_map_)
  ScopedRwWriteLock blob_map_lock(header_->lock_[kBlobMapLock],
                                  kMDM_LocalRenameBlob);
  auto iter = (*blob_map_).find(blob_id);
  if (iter == blob_map_->end()) {
    return true;
  }
  hipc::pair<BlobId, BlobInfo>& info = (*iter);
  BlobInfo &blob_info = info.GetSecond();
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
  HILOG(kDebug, "Destroying the blob: {}", blob_id)
  AUTO_TRACE(1);
  // Avoid flushing
  ScopedRwReadLock flush_lock(header_->lock_[kFlushLock], kMDM_LocalClear);
  // Acquire MD write lock (modify blob_id_map & blob_map_)
  ScopedRwWriteLock blob_map_lock(header_->lock_[kBlobMapLock],
                                  kMDM_LocalDestroyBlob);
  auto iter = (*blob_map_).find(blob_id);
  if (iter.is_end()) {
    return true;
  }
  hipc::pair<BlobId, BlobInfo>& info = (*iter);
  BlobInfo &blob_info = info.GetSecond();
  hipc::uptr<hipc::charbuf> blob_name_internal =
      CreateBlobName(bkt_id, *blob_info.name_);
  blob_id_map_->erase(*blob_name_internal);
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
  // Avoid flushing
  ScopedRwReadLock flush_lock(header_->lock_[kFlushLock], kMDM_LocalClear);
  // Modify tag map
  ScopedRwWriteLock tag_map_lock(header_->lock_[kTagMapLock],
                                 kMDM_LocalClear);
  tag_id_map_->clear();
  tag_map_->clear();
  // Modify blob map
  ScopedRwWriteLock blob_map_lock(header_->lock_[kBlobMapLock],
                                  kMDM_LocalClear);
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
  // Avoid flushing
  ScopedRwReadLock flush_lock(header_->lock_[kFlushLock], kMDM_LocalClear);
  // Acquire MD write lock (modifying tag_map)
  ScopedRwWriteLock tag_map_lock(header_->lock_[kTagMapLock],
                                 kMDM_LocalGetOrCreateTag);

  // Create unique ID for the Bucket
  TagId tag_id;
  tag_id.unique_ = header_->id_alloc_.fetch_add(1);
  tag_id.node_id_ = rpc_->node_id_;
  auto tag_name_shm = hipc::make_uptr<hipc::string>(tag_name);
  bool did_create = tag_id_map_->try_emplace(*tag_name_shm, tag_id);

  // Emplace bucket if it does not already exist
  if (did_create) {
    HILOG(kDebug, "Creating tag for the first time: {} {}", tag_name, tag_id)
    tag_map_->emplace(tag_id);
    auto iter = tag_map_->find(tag_id);
    hipc::pair<TagId, TagInfo> &info_pair = *iter;
    TagInfo &info = info_pair.GetSecond();
    (*info.name_) = *tag_name_shm;
    info.tag_id_ = tag_id;
    info.owner_ = owner;
    info.internal_size_ = backend_size;
  } else {
    HILOG(kDebug, "Found existing tag: {}", tag_name)
    auto iter = tag_id_map_->find(*tag_name_shm);
    hipc::pair<hipc::charbuf, TagId> &id_info = (*iter);
    tag_id = id_info.GetSecond();
  }

  return {tag_id, did_create};
}

/**
 * Get the TagId with \a bkt_name bucket name
 * */
TagId MetadataManager::LocalGetTagId(const std::string &tag_name) {
  AUTO_TRACE(1);
  // Acquire MD read lock (reading tag_id_map)
  ScopedRwReadLock tag_map_lock(header_->lock_[kTagMapLock],
                                kMDM_LocalGetTagId);
  auto tag_name_shm = hipc::make_uptr<hipc::string>(tag_name);
  auto iter = tag_id_map_->find(*tag_name_shm);
  if (iter == tag_id_map_->end()) {
    return TagId::GetNull();
  }
  hipc::pair<hipc::charbuf, TagId> &info_pair = (*iter);
  TagId bkt_id = info_pair.GetSecond();
  return bkt_id;
}

/**
 * Get the name of a tag
 * */
std::string MetadataManager::LocalGetTagName(TagId tag_id) {
  HILOG(kDebug, "Finding the name of tag: {}.{}",
        tag_id.node_id_, tag_id.unique_)
  AUTO_TRACE(1);
  // Acquire MD read lock (reading tag_map_)
  ScopedRwReadLock tag_map_lock(header_->lock_[kTagMapLock],
                                kMDM_LocalGetTagName);
  auto iter = tag_map_->find(tag_id);
  if (iter == tag_map_->end()) {
    return "";
  }
  hipc::pair<TagId, TagInfo> &info_pair = (*iter);
  hipc::string &bkt_name = *info_pair.GetSecond().name_;
  return bkt_name.str();
}

/**
 * Rename a tag
 * */
bool MetadataManager::LocalRenameTag(TagId tag_id,
                                     const std::string &new_name) {
  HILOG(kDebug, "Renaming the tag: {}.{} to {}",
        tag_id.node_id_, tag_id.unique_, new_name)
  AUTO_TRACE(1);
  // Avoid flushing
  ScopedRwReadLock flush_lock(header_->lock_[kFlushLock], kMDM_LocalClear);
  // Acquire MD write lock (modifying tag_map_)
  ScopedRwWriteLock tag_map_lock(header_->lock_[kTagMapLock],
                                 kMDM_LocalRenameTag);
  auto iter = tag_map_->find(tag_id);
  if (iter == tag_map_->end()) {
    return true;
  }
  auto new_name_shm = hipc::make_uptr<hipc::string>(new_name);
  hipc::pair<TagId, TagInfo> &info_pair = (*iter);
  hipc::string &old_bkt_name = *info_pair.GetSecond().name_;
  tag_id_map_->erase(old_bkt_name);
  tag_id_map_->emplace(*new_name_shm, tag_id);
  return true;
}

/**
 * Delete a tag
 * */
bool MetadataManager::LocalDestroyTag(TagId tag_id) {
  HILOG(kDebug, "Destroying the tag: {}", tag_id)
  AUTO_TRACE(1);
  // Avoid flushing
  ScopedRwReadLock flush_lock(header_->lock_[kFlushLock], kMDM_LocalClear);
  // Acquire MD write lock (modifying tag_map_)
  ScopedRwWriteLock tag_map_lock(header_->lock_[kTagMapLock],
                                 kMDM_LocalDestroyTag);
  auto iter = tag_map_->find(tag_id);
  if (iter.is_end()) {
    HILOG(kDebug, "Could NOT destroy: {}", tag_id)
    return true;
  }
  hipc::pair<TagId, TagInfo> &info_pair = *iter;
  auto &tag_info = info_pair.GetSecond();
  tag_id_map_->erase(*tag_info.name_);
  tag_map_->erase(tag_id);
  HILOG(kDebug, "Destroyed the tag: {}", tag_id)
  return true;
}

/**
 * Add a blob to a tag index
 * */
Status MetadataManager::LocalTagAddBlob(TagId tag_id,
                                        BlobId blob_id) {
  AUTO_TRACE(1);
  // Acquire MD write lock (modify tag_map_)
  ScopedRwReadLock tag_map_lock(header_->lock_[kTagMapLock],
                                kMDM_LocalTagAddBlob);
  auto iter = tag_map_->find(tag_id);
  if (iter.is_end()) {
    return Status();
  }
  hipc::pair<TagId, TagInfo> &blob_list = (*iter);
  TagInfo &info = blob_list.GetSecond();
  ScopedRwWriteLock tag_lock(info.lock_[0], kMDM_LocalTagRemoveBlob);
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
  ScopedRwReadLock tag_map_lock(header_->lock_[kTagMapLock],
                                kMDM_LocalTagRemoveBlob);
  auto iter = tag_map_->find(tag_id);
  if (iter.is_end()) {
    return Status();
  }
  hipc::pair<TagId, TagInfo> &blob_list = (*iter);
  TagInfo &info = blob_list.GetSecond();
  ScopedRwWriteLock tag_lock(info.lock_[0], kMDM_LocalTagRemoveBlob);
  info.blobs_->erase(blob_id);
  return Status();
}


/**
 * Find all blobs pertaining to a tag
 * */
std::vector<BlobId> MetadataManager::LocalGroupByTag(TagId tag_id) {
  AUTO_TRACE(1);
  // Acquire MD read lock (read tag_map_)
  ScopedRwReadLock tag_map_lock(header_->lock_[kTagMapLock],
                                kMDM_LocalGroupByTag);
  // Get the tag info
  auto iter = tag_map_->find(tag_id);
  if (iter.is_end()) {
    return std::vector<BlobId>();
  }
  auto &tag_info_pair = *iter;
  TagInfo &tag_info = tag_info_pair.GetSecond();
  // Convert slist into std::vector
  return hshm::to_stl_vector<BlobId>(*tag_info.blobs_);
}

/**
 * Add a trait to a tag index. Create tag if it does not exist.
 * */
bool MetadataManager::LocalTagAddTrait(TagId tag_id, TraitId trait_id) {
  AUTO_TRACE(1);
  HILOG(kDebug, "Adding trait {} to tag {}",
        trait_id, tag_id);
  // Acquire MD read lock (read tag_map_)
  ScopedRwReadLock tag_map_lock(header_->lock_[kTagMapLock],
                                kMDM_LocalTagAddTrait);
  auto iter = tag_map_->find(tag_id);
  if (iter.is_end()) {
    HILOG(kDebug, "Could NOT add trait to tag {}")
    return true;
  }
  auto &tag_info_pair = *iter;
  TagInfo &tag_info = tag_info_pair.GetSecond();
  ScopedRwWriteLock tag_lock(tag_info.lock_[0], kMDM_LocalTagAddTrait);
  tag_info.traits_->emplace_back(trait_id);
  HILOG(kDebug, "Added trait {} to tag {}",
        trait_id, tag_id);
  return true;
}

/**
 * Find all traits pertaining to a tag
 * */
std::vector<TraitId> MetadataManager::LocalTagGetTraits(TagId tag_id) {
  AUTO_TRACE(1);
  // Acquire MD read lock (read tag_map_)
  ScopedRwReadLock tag_map_lock(header_->lock_[kTagMapLock],
                                kMDM_LocalTagGetTraits);
  auto iter = tag_map_->find(tag_id);
  if (iter.is_end()) {
    return std::vector<TraitId>();
  }
  auto &tag_info_pair = *iter;
  TagInfo &tag_info = tag_info_pair.GetSecond();
  return hshm::to_stl_vector<TraitId>(*tag_info.traits_);
}

/**====================================
 * Trait Operations
 * ===================================*/

/**
 * Register a trait. Stores the state of the trait in a way that can
 * be queried by all processes.
 * */
RPC TraitId MetadataManager::LocalRegisterTrait(
    TraitId trait_id,
    const hshm::charbuf &trait_params) {
  HILOG(kDebug, "Registering trait {}", trait_id)
  // Acquire md write lock (modifying trait map)
  ScopedRwWriteLock md_lock(header_->lock_[kTraitMapLock],
                            kMDM_LocalRegisterTrait);
  auto hdr = reinterpret_cast<TraitHeader*>(trait_params.data());
  std::string trait_uuid = hdr->trait_uuid_;

  // Check if trait exists
  auto trait_uuid_shm = hipc::make_uptr<hipc::charbuf>(trait_uuid);
  auto iter = trait_id_map_->find(*trait_uuid_shm);
  if (!iter.is_end()) {
    trait_id = (*iter).GetSecond();
    return trait_id;
  }

  // Create new trait
  if (trait_id.IsNull()) {
    trait_id.unique_ = header_->id_alloc_.fetch_add(1);
    trait_id.node_id_ = rpc_->node_id_;
  }
  trait_id_map_->emplace(*trait_uuid_shm, trait_id);
  trait_map_->emplace(trait_id, trait_params);
  return trait_id;
}

/**
 * Get trait identifier
 * */
RPC TraitId
MetadataManager::LocalGetTraitId(const std::string &trait_uuid) {
  HILOG(kDebug, "Getting trait id for {}", trait_uuid)
  // Acquire md read lock (reading trait_id_map)
  ScopedRwReadLock md_lock(header_->lock_[kTraitMapLock],
                           kMDM_LocalGetTraitId);
  // Check if trait exists
  auto trait_uuid_shm = hipc::make_uptr<hipc::charbuf>(trait_uuid);
  auto iter = trait_id_map_->find(*trait_uuid_shm);
  if (iter.is_end()) {
    return TraitId::GetNull();
  }
  TraitId trait_id = (*iter).GetSecond();
  return trait_id;
}

/**
 * Get trait parameters
 * */
RPC hshm::charbuf
MetadataManager::LocalGetTraitParams(TraitId trait_id) {
  // Acquire md read lock (reading trait_map)
  ScopedRwReadLock md_lock(header_->lock_[kTraitMapLock],
                           kMDM_LocalGetTraitParams);

  // Get the trait parameters from the map
  auto iter = trait_map_->find(trait_id);
  if (iter.is_end()) {
    return hshm::charbuf();
  }
  hipc::pair<TraitId, hipc::charbuf> &trait_pair = *iter;
  return hshm::to_charbuf(trait_pair.GetSecond());
}

/**
 * Get an existing trait
 * */
Trait* MetadataManager::GlobalGetTrait(TraitId trait_id) {
  // HILOG(kDebug, "Getting the trait {}", trait_id)
  Trait *trait = nullptr;

  // Check if trait is already constructed
  ScopedRwReadLock trait_lock(header_->lock_[kTraitMapLock],
                              kMDM_GlobalGetTrait);
  auto iter = local_trait_map_.find(trait_id);
  if (iter != local_trait_map_.end()) {
    std::pair<TraitId, Trait*> trait_pair = *iter;
    trait = trait_pair.second;
    trait_lock.Unlock();
    return trait;
  }
  trait_lock.Unlock();

  // Get the trait state locally or globally
  hshm::charbuf params = LocalGetTraitParams(trait_id);
  if (params.size() == 0) {
    params = GlobalGetTraitParams(trait_id);
    if (params.size() == 0) {
      HELOG(kError, "Could not find the trait {}.{}",
            trait_id.node_id_, trait_id.unique_)
      return nullptr;
    }
    LocalRegisterTrait(trait_id, params);
  }

  // Construct the trait based on the parameters
  ScopedRwWriteLock trait_lock_w(header_->lock_[kTraitMapLock],
                                 kMDM_GlobalGetTrait);
  HILOG(kDebug, "Acquired trait lock w for {}", trait_id)

  // Check if the trait exists now
  iter = local_trait_map_.find(trait_id);
  if (iter != local_trait_map_.end()) {
    std::pair<TraitId, Trait*> trait_pair = *iter;
    trait = trait_pair.second;
    return trait;
  }

  // Construct the trait
  trait = traits_->ConstructTrait(params);
  if (trait == nullptr) {
    return nullptr;
  }

  // Induct the trait
  local_trait_map_.emplace(trait_id, trait);
  return trait;
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
  IoStat stat;
  stat.blob_id_ = blob_id;
  stat.tag_id_ = tag_id;
  stat.blob_size_ = blob_size;
  stat.type_ = type;
  stat.rank_ = 0;
  if (is_mpi_) {
    MPI_Comm_rank(MPI_COMM_WORLD, &stat.rank_);
  }
  io_pattern_log_->emplace(stat);
}


}  // namespace hermes
