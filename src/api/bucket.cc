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
: mdm_(&HERMES->mdm_), bpm_(HERMES->bpm_.get()), name_(bkt_name) {
  std::vector<TraitId> traits;
  auto ret = mdm_->GlobalGetOrCreateTag(bkt_name, true, traits, backend_size);
  id_ = ret.first;
  did_create_ = ret.second;
}

/**
 * Get the current size of the bucket
 * */
size_t Bucket::GetSize(bool backend) {
  return mdm_->GlobalGetBucketSize(id_, backend);
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
void Bucket::Clear() {
  mdm_->GlobalClearBucket(id_);
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
  hipc::string lblob_name(blob_name);
  blob_id = mdm_->GlobalGetBlobId(GetId(), lblob_name);
  return Status();
}

/**
 * Get the name of a blob from the blob id
 *
 * @param blob_id the blob_id
 * @param blob_name the name of the blob
 * @return The Status of the operation
 * */
Status Bucket::GetBlobName(const BlobId &blob_id, std::string &blob_name) {
  hipc::string lblob_name(blob_name);
  blob_name = mdm_->GlobalGetBlobName(blob_id);
  return Status();
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
  std::pair<BlobId, bool> ret = mdm_->GlobalTryCreateBlob(
      id_, hipc::charbuf(blob_name));
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
    auto put_ret = mdm_->GlobalPutBlobMetadata(id_, hipc::string(blob_name),
                                               blob.size(), buffers);
    blob_id = std::get<0>(put_ret);
    bool did_create = std::get<1>(put_ret);
    ssize_t orig_blob_size = (ssize_t)std::get<2>(put_ret);
    ssize_t new_blob_size = blob.size();
    mdm_->GlobalTagAddBlob(id_, blob_id);
    mdm_->GlobalUpdateBucketSize(id_,
                                 new_blob_size - orig_blob_size,
                                 BucketUpdate::kBoth);
  }

  return Status();
}

/**
 * Put \a blob_name Blob into the bucket. Load the blob from the
 * I/O backend if it does not exist.
 *
 * @param blob_name the semantic name of the blob
 * @param blob the buffer to put final data in
 * @param blob_off the offset within the blob to begin the Put
 * @param io_ctx which adapter to route I/O request if blob DNE
 * @param opts which adapter to route I/O request if blob DNE
 * @param ctx any additional information
 * */
Status Bucket::PartialPutOrCreate(const std::string &blob_name,
                                  const Blob &blob,
                                  size_t blob_off,
                                  BlobId &blob_id,
                                  IoStatus &status,
                                  const IoClientContext &opts,
                                  Context &ctx) {
  Blob full_blob;
  if (ContainsBlob(blob_name, blob_id)) {
    // Case 1: The blob already exists (read from hermes)
    // Read blob from Hermes
    LOG(INFO) << "Blob existed. Reading from Hermes." << std::endl;
    Get(blob_id, full_blob, ctx);
  }
  if (blob_off == 0 &&
      blob.size() >= opts.backend_size_ &&
      blob.size() >= full_blob.size()) {
    // Case 2: We're overriding the entire blob
    // Put the entire blob, no need to load from storage
    LOG(INFO) << "Putting the entire blob." << std::endl;
    return Put(blob_name, blob, blob_id, ctx);
  }
  if (full_blob.size() < opts.backend_size_) {
    // Case 3: The blob did not fully exist (need to read from backend)
    // Read blob using adapter
    LOG(INFO) << "Blob didn't fully exist. Reading from backend." << std::endl;
    auto io_client = IoClientFactory::Get(opts.type_);
    full_blob.resize(opts.backend_size_);
    if (io_client) {
      io_client->ReadBlob(hipc::charbuf(name_),
                          full_blob, opts, status);
      if (!status.success_) {
        LOG(INFO) << "Failed to read blob of size "
                  << opts.backend_size_
                  << "from backend (PartialPut)";
        return PARTIAL_PUT_OR_CREATE_OVERFLOW;
      }
    }
  }
  LOG(INFO) << "Modifying full_blob at offset: " << blob_off
            << " for total size: " << blob.size() << std::endl;
  // Ensure the blob can hold the update
  full_blob.resize(std::max(full_blob.size(), blob_off + blob.size()));
  // Modify the blob
  memcpy(full_blob.data() + blob_off, blob.data(), blob.size());
  // Re-put the blob
  if (opts.adapter_mode_ != AdapterMode::kBypass) {
    Put(blob_name, full_blob, blob_id, ctx);
  }
  LOG(INFO) << "Partially put to blob: (" << blob_id.unique_
            << ", " << blob_id.node_id_ << ")" << std::endl;
  return Status();
}

/**
 * Get \a blob_id Blob from the bucket
 * */
Status Bucket::Get(BlobId blob_id, Blob &blob, Context &ctx) {
  std::vector<BufferInfo> buffers = mdm_->GlobalGetBlobBuffers(blob_id);
  blob = HERMES->borg_.GlobalReadBlobFromBuffers(buffers);
  return Status();
}

/**
 * Load \a blob_name Blob from the bucket. Load the blob from the
 * I/O backend if it does not exist.
 *
 * @param blob_name the semantic name of the blob
 * @param blob the buffer to put final data in
 * @param blob_off the offset within the blob to begin the Put
 * @param blob_size the total amount of data to read
 * @param blob_id [out] the blob id corresponding to blob_name
 * @param io_ctx information required to perform I/O to the backend
 * @param opts specific configuration of the I/O to perform
 * @param ctx any additional information
 * */
Status Bucket::PartialGetOrCreate(const std::string &blob_name,
                                  Blob &blob,
                                  size_t blob_off,
                                  size_t blob_size,
                                  BlobId &blob_id,
                                  IoStatus &status,
                                  const IoClientContext &opts,
                                  Context &ctx) {
  Blob full_blob;
  if (ContainsBlob(blob_name, blob_id)) {
    // Case 1: The blob already exists (read from hermes)
    // Read blob from Hermes
    LOG(INFO) << "Blob existed. Reading blob from Hermes." << std::endl;
    Get(blob_id, full_blob, ctx);
  }
  if (full_blob.size() < opts.backend_size_) {
    // Case 2: The blob did not exist (or at least not fully)
    // Read blob using adapter
    LOG(INFO) << "Blob did not exist. Reading blob from backend." << std::endl;
    auto io_client = IoClientFactory::Get(opts.type_);
    full_blob.resize(opts.backend_size_);
    if (io_client) {
      io_client->ReadBlob(hipc::charbuf(name_), full_blob, opts, status);
      if (!status.success_) {
        LOG(INFO) << "Failed to read blob of size "
                  << opts.backend_size_
                  << "from backend (PartialCreate)";
        return PARTIAL_GET_OR_CREATE_OVERFLOW;
      }
      if (opts.adapter_mode_ != AdapterMode::kBypass) {
        Put(blob_name, full_blob, blob_id, ctx);
      }
    }
  }
  // Ensure the blob can hold the update
  if (full_blob.size() < blob_off + blob_size) {
    return PARTIAL_GET_OR_CREATE_OVERFLOW;
  }
  // Modify the blob
  // TODO(llogan): we can avoid a copy here
  blob.resize(blob_size);
  memcpy(blob.data(), full_blob.data() + blob_off, blob.size());
  return Status();
}

/**
 * Flush a blob
 * */
void Bucket::FlushBlob(BlobId blob_id,
                       const IoClientContext &opts) {
  LOG(INFO) << "Flushing blob" << std::endl;
  if (opts.adapter_mode_ == AdapterMode::kScratch) {
    LOG(INFO) << "In scratch mode, ignoring flush" << std::endl;
    return;
  }
  Blob full_blob;
  IoStatus status;
  // Read blob from Hermes
  Get(blob_id, full_blob, ctx_);
  LOG(INFO) << "The blob being flushed as size: "
            << full_blob.size() << std::endl;
  std::string blob_name;
  GetBlobName(blob_id, blob_name);
  // Write blob to backend
  auto io_client = IoClientFactory::Get(opts.type_);
  if (io_client) {
    IoClientContext decode_opts = io_client->DecodeBlobName(opts, blob_name);
    io_client->WriteBlob(hipc::charbuf(name_),
                         full_blob,
                         decode_opts,
                         status);
  }
}

/**
 * Flush the entire bucket
 * */
void Bucket::Flush(const IoClientContext &opts) {
  std::vector<BlobId> blob_ids = GetContainedBlobIds();
  if (opts.adapter_mode_ == AdapterMode::kScratch) { return; }
  LOG(INFO) << "Flushing " << blob_ids.size() << " blobs" << std::endl;
  for (BlobId &blob_id : blob_ids) {
    FlushBlob(blob_id, opts);
  }
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
  hipc::string lnew_blob_name(new_blob_name);
  mdm_->GlobalRenameBlob(id_, blob_id, lnew_blob_name);
}

/**
 * Delete \a blob_id blob
 * */
void Bucket::DestroyBlob(BlobId blob_id,
                         Context &ctx,
                         IoClientContext opts) {
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
