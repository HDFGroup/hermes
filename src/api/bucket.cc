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

#include "bucket.h"
#include "data_placement_engine_factory.h"

namespace hermes::api {

using hermes::adapter::AdapterMode;

/**====================================
 * Bucket Operations
 * ===================================*/

/**
 * Either initialize or fetch the bucket.
 * */
Bucket::Bucket(const std::string &bkt_name,
               Context &ctx,
               size_t backend_size)
: mdm_(HERMES->mdm_.get()), bpm_(HERMES->bpm_.get()), name_(bkt_name) {
  std::vector<TraitId> traits;
  auto ret = mdm_->GlobalGetOrCreateTag(bkt_name, true, traits, backend_size);
  id_ = ret.first;
  did_create_ = ret.second;
}

/**
 * Either initialize or fetch the bucket.
 * */
Bucket::Bucket(TagId tag_id)
    : mdm_(HERMES->mdm_.get()), bpm_(HERMES->bpm_.get()) {
  // TODO(llogan): Get tag name
  id_ = tag_id;
  did_create_ = false;
}

/**
 * Get the current size of the bucket
 * */
size_t Bucket::GetSize(bool backend) {
  return mdm_->GlobalGetBucketSize(id_, backend);
}

/**
   * Update the size of the bucket
   * Needed for the adapters for now.
   * */
void Bucket::UpdateSize(ssize_t delta, BucketUpdate mode) {
  mdm_->GlobalUpdateBucketSize(id_,
                               delta,
                               mode);
}

/**
 * Rename this bucket
 * */
void Bucket::Rename(const std::string &new_bkt_name) {
  mdm_->GlobalRenameTag(id_, new_bkt_name);
}

/**
   * Clears the buckets contents, but doesn't destroy its metadata
   * */
void Bucket::Clear(bool backend) {
  mdm_->GlobalClearBucket(id_, backend);
}

/**
 * Destroys this bucket along with all its contents.
 * */
void Bucket::Destroy() {
  mdm_->GlobalDestroyTag(id_);
}

/**====================================
 * Blob Operations
 * ===================================*/

/**
 * Get the id of a blob from the blob name
 * */
Status Bucket::GetBlobId(const std::string &blob_name,
                         BlobId &blob_id) {
  blob_id = mdm_->GlobalGetBlobId(GetId(), blob_name);
  return Status();
}

/**
 * Get the name of a blob from the blob id
 *
 * @param blob_id the blob_id
 * @return The Status of the operation
 * */
std::string Bucket::GetBlobName(const BlobId &blob_id) {
  return mdm_->GlobalGetBlobName(blob_id);
}

/**
 * Get the score of a blob from the blob id
 *
 * @param blob_id the blob_id
 * @return The Status of the operation
 * */
float Bucket::GetBlobScore(const BlobId &blob_id) {
  return mdm_->GlobalGetBlobScore(blob_id);
}


/**
 * Lock the bucket
 * */
bool Bucket::LockBlob(BlobId blob_id, MdLockType lock_type) {
  return mdm_->GlobalLockBlob(blob_id, lock_type);
}

/**
 * Unlock the bucket
 * */
bool Bucket::UnlockBlob(BlobId blob_id, MdLockType lock_type) {
  return mdm_->GlobalUnlockBlob(blob_id, lock_type);
}

/**
 * Put \a blob_id Blob into the bucket
 * */
Status Bucket::TryCreateBlob(const std::string &blob_name,
                             BlobId &blob_id,
                             Context &ctx) {
  std::pair<BlobId, bool> ret = mdm_->GlobalTryCreateBlob(id_, blob_name);
  blob_id = ret.first;
  if (ret.second) {
    mdm_->GlobalTagAddBlob(id_,
                           blob_id);
  }
  return Status();
}

/**
 * Label \a blob_id blob with \a tag_name TAG
 * */
Status Bucket::TagBlob(BlobId &blob_id,
                       TagId &tag_id) {
  mdm_->GlobalTagAddBlob(tag_id, blob_id);
  return mdm_->GlobalTagBlob(blob_id, tag_id);
}

/**
 * Put \a blob_id Blob into the bucket
 * */
Status Bucket::Put(std::string blob_name,
                   const Blob &blob,
                   BlobId &blob_id,
                   Context &ctx) {
  // Calculate placement
  auto dpe = DPEFactory::Get(ctx.policy);
  std::vector<size_t> blob_sizes(1, blob.size());
  std::vector<PlacementSchema> schemas;
  dpe->CalculatePlacement(blob_sizes, schemas, ctx);

  // Allocate buffers for the blob & enqueue placement
  for (auto &schema : schemas) {
    auto buffers = bpm_->GlobalAllocateAndSetBuffers(schema, blob);
    auto put_ret = mdm_->GlobalPutBlobMetadata(id_, blob_name,
                                               blob.size(), buffers,
                                               ctx.blob_score_);
    blob_id = std::get<0>(put_ret);
    bool did_create = std::get<1>(put_ret);
    ssize_t orig_blob_size = (ssize_t)std::get<2>(put_ret);
    ssize_t new_blob_size = blob.size();
    if (did_create) {
      mdm_->GlobalTagAddBlob(id_, blob_id);
    }
    UpdateSize(new_blob_size - orig_blob_size, BucketUpdate::kInternal);
  }

  // Update the local MDM I/O log
  mdm_->AddIoStat(id_, blob_id, blob.size(), IoType::kWrite);

  return Status();
}

/**
 * Get \a blob_id Blob from the bucket
 * */
Status Bucket::Get(BlobId blob_id, Blob &blob, Context &ctx) {
  std::vector<BufferInfo> buffers = mdm_->GlobalGetBlobBuffers(blob_id);
  blob = HERMES->borg_->GlobalReadBlobFromBuffers(buffers);
  // Update the local MDM I/O log
  mdm_->AddIoStat(id_, blob_id, blob.size(), IoType::kRead);
  return Status();
}

/**
 * Determine if the bucket contains \a blob_id BLOB
 * */
bool Bucket::ContainsBlob(const std::string &blob_name,
                          BlobId &blob_id) {
  GetBlobId(blob_name, blob_id);
  return !blob_id.IsNull();
}

/**
 * Determine if the bucket contains \a blob_id BLOB
 * */
bool Bucket::ContainsBlob(BlobId blob_id) {
  return mdm_->GlobalBlobHasTag(blob_id, id_);
}

/**
 * Rename \a blob_id blob to \a new_blob_name new name
 * */
void Bucket::RenameBlob(BlobId blob_id,
                        std::string new_blob_name,
                        Context &ctx) {
  mdm_->GlobalRenameBlob(id_, blob_id, new_blob_name);
}

/**
 * Delete \a blob_id blob
 * */
void Bucket::DestroyBlob(BlobId blob_id,
                         Context &ctx) {
  mdm_->GlobalTagRemoveBlob(id_, blob_id);
  mdm_->GlobalDestroyBlob(id_, blob_id);
}

/**
 * Get the set of blob IDs owned by the bucket
 * */
std::vector<BlobId> Bucket::GetContainedBlobIds() {
  return mdm_->GlobalGroupByTag(id_);
}

}  // namespace hermes::api
