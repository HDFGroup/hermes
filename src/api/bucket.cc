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

///////////////////////////
////// Bucket Operations
//////////////////////////

/**
 * Either initialize or fetch the bucket.
 * */
Bucket::Bucket(const std::string &bkt_name,
               Context &ctx,
               const IoClientContext &opts)
: mdm_(&HERMES->mdm_), bpm_(HERMES->bpm_.get()), name_(bkt_name) {
  hipc::string lname(bkt_name);
  id_ = mdm_->LocalGetOrCreateBucket(lname, opts);
}

/**
 * Get the current size of the bucket
 * */
size_t Bucket::GetSize(IoClientContext opts) {
  return mdm_->LocalGetBucketSize(id_, opts);
}

/**
 * Lock the bucket
 * */
void Bucket::LockBucket(MdLockType lock_type) {
  mdm_->LocalLockBucket(id_, lock_type);
}

/**
 * Unlock the bucket
 * */
void Bucket::UnlockBucket(MdLockType lock_type) {
  mdm_->LocalUnlockBucket(id_, lock_type);
}

/**
 * Rename this bucket
 * */
void Bucket::Rename(std::string new_bkt_name) {
  hipc::string lname(new_bkt_name);
  mdm_->LocalRenameBucket(id_, lname);
}

/**
 * Destroys this bucket along with all its contents.
 * */
void Bucket::Destroy() {
}

///////////////////////
////// Blob Operations
///////////////////////

/**
 * Get the id of a blob from the blob name
 * */
Status Bucket::GetBlobId(const std::string &blob_name,
                         BlobId &blob_id) {
  hipc::string lblob_name(blob_name);
  blob_id = mdm_->LocalGetBlobId(GetId(), lblob_name);
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
  blob_name = mdm_->LocalGetBlobName(blob_id);
  return Status();
}


/**
 * Lock the bucket
 * */
bool Bucket::LockBlob(BlobId blob_id, MdLockType lock_type) {
  return mdm_->LocalLockBlob(blob_id, lock_type);
}

/**
 * Unlock the bucket
 * */
bool Bucket::UnlockBlob(BlobId blob_id, MdLockType lock_type) {
  return mdm_->LocalUnlockBlob(blob_id, lock_type);
}

/**
 * Put \a blob_id Blob into the bucket
 * */
Status Bucket::TryCreateBlob(const std::string &blob_name,
                             BlobId &blob_id,
                             Context &ctx,
                             const IoClientContext &opts) {
  std::pair<BlobId, bool> ret = mdm_->LocalBucketTryCreateBlob(
      id_, hipc::charbuf(blob_name));
  blob_id = ret.first;
  if (ret.second) {
    mdm_->LocalBucketRegisterBlobId(id_,
                                    blob_id,
                                    0, 0, true,
                                    opts);
  }
  return Status();
}

/**
 * Put \a blob_id Blob into the bucket
 * */
Status Bucket::Put(std::string blob_name,
                   const Blob &blob,
                   BlobId &blob_id,
                   Context &ctx,
                   IoClientContext opts) {
  // Calculate placement
  auto dpe = DPEFactory::Get(ctx.policy);
  std::vector<size_t> blob_sizes(1, blob.size());
  std::vector<PlacementSchema> schemas;
  dpe->CalculatePlacement(blob_sizes, schemas, ctx);

  // Allocate buffers for the blob & enqueue placement
  for (auto &schema : schemas) {
    // TODO(llogan): rpcify
    auto buffers = bpm_->LocalAllocateAndSetBuffers(schema, blob);
    auto put_ret = mdm_->LocalBucketPutBlob(id_, hipc::string(blob_name),
                                            blob.size(), buffers);
    blob_id = std::get<0>(put_ret);
    bool did_create = std::get<1>(put_ret);
    size_t orig_blob_size = std::get<2>(put_ret);
    opts.backend_size_ = blob.size();
    mdm_->LocalBucketRegisterBlobId(id_,
                                    blob_id,
                                    orig_blob_size,
                                    blob.size(),
                                    did_create,
                                    opts);
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
    return Put(blob_name, blob, blob_id, ctx, opts);
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
    Put(blob_name, full_blob, blob_id, ctx, opts);
  }
  LOG(INFO) << "Partially put to blob: (" << blob_id.unique_
            << ", " << blob_id.node_id_ << ")" << std::endl;
  return Status();
}

/**
 * Get \a blob_id Blob from the bucket
 * */
Status Bucket::Get(BlobId blob_id, Blob &blob, Context &ctx) {
  Blob b = mdm_->LocalBucketGetBlob(blob_id);
  blob = std::move(b);
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
        Put(blob_name, full_blob, blob_id, ctx, opts);
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
  return mdm_->LocalBucketContainsBlob(id_, blob_id);
}

/**
 * Rename \a blob_id blob to \a new_blob_name new name
 * */
void Bucket::RenameBlob(BlobId blob_id,
                        std::string new_blob_name,
                        Context &ctx) {
  hipc::string lnew_blob_name(new_blob_name);
  mdm_->LocalRenameBlob(id_, blob_id, lnew_blob_name);
}

/**
 * Delete \a blob_id blob
 * */
void Bucket::DestroyBlob(BlobId blob_id, Context &ctx,
                         IoClientContext opts) {
  mdm_->LocalBucketUnregisterBlobId(id_, blob_id, opts);
  mdm_->LocalDestroyBlob(id_, blob_id);
}

/**
   * Get the set of blob IDs contained in the bucket
   * */
std::vector<BlobId> Bucket::GetContainedBlobIds() {
  return mdm_->LocalBucketGetContainedBlobIds(id_);
}

}  // namespace hermes::api
