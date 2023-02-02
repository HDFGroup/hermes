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

///////////////////////////
////// Bucket Operations
//////////////////////////

/**
 * Either initialize or fetch the bucket.
 * */
Bucket::Bucket(const std::string &bkt_name,
               Context &ctx,
               const IoClientOptions &opts)
: mdm_(&HERMES->mdm_), bpm_(&HERMES->bpm_) {
  lipc::string lname(bkt_name);
  id_ = mdm_->LocalGetOrCreateBucket(lname, opts);
}

/**
 * Get the current size of the bucket
 * */
size_t Bucket::GetSize(IoClientOptions opts) {
  return mdm_->LocalGetBucketSize(id_, opts);
}

/**
 * Rename this bucket
 * */
void Bucket::Rename(std::string new_bkt_name) {
  lipc::string lname(new_bkt_name);
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
Status Bucket::GetBlobId(std::string blob_name,
                         BlobId &blob_id) {
  lipc::string lblob_name(blob_name);
  blob_id = mdm_->LocalGetBlobId(GetId(), lblob_name);
  return Status();
}


/**
 * Put \a blob_id Blob into the bucket
 * */
Status Bucket::Put(std::string blob_name,
                   const Blob blob,
                   BlobId &blob_id,
                   Context &ctx,
                   IoClientOptions opts) {
  // Calculate placement
  auto dpe = DPEFactory::Get(ctx.policy);
  std::vector<size_t> blob_sizes(1, blob.size());
  std::vector<PlacementSchema> schemas;
  dpe->CalculatePlacement(blob_sizes, schemas, ctx);

  // Allocate buffers for the blob & enqueue placement
  for (auto &schema : schemas) {
    // TODO(llogan): rpcify
    auto buffers = bpm_->LocalAllocateAndSetBuffers(schema, blob);
    auto put_ret = mdm_->LocalBucketPutBlob(id_, lipc::string(blob_name),
                                            blob.size(), buffers);
    blob_id = std::get<0>(put_ret);
    bool did_create = std::get<1>(put_ret);
    size_t orig_blob_size = std::get<2>(put_ret);
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
                                  const IoClientContext &io_ctx,
                                  const IoClientOptions &opts,
                                  Context &ctx) {
  Blob full_blob;
  // Put the blob
  if (blob_off == 0 && blob.size() == opts.backend_size_) {
    // Case 1: We're overriding the entire blob
    // Put the entire blob, no need to load from storage
    return Put(blob_name, blob, blob_id, ctx);
  }
  if (ContainsBlob(blob_id)) {
    // Case 2: The blob already exists (read from hermes)
    // Read blob from Hermes
    Get(blob_id, full_blob, ctx);
  } else {
    // Case 3: The blob did not exist (need to read from backend)
    // Read blob using adapter
    IoStatus status;
    auto io_client = IoClientFactory::Get(io_ctx.type_);
    full_blob.resize(opts.backend_size_);
    io_client->ReadBlob(full_blob,
                        io_ctx,
                        opts,
                        status);
  }
  // Ensure the blob can hold the update
  full_blob.resize(std::max(full_blob.size(), blob_off + blob.size()));
  // Modify the blob
  memcpy(full_blob.data() + blob_off, blob.data(), blob.size());
  // Re-put the blob
  Put(blob_name, full_blob, blob_id, ctx);
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
                                  const IoClientContext &io_ctx,
                                  const IoClientOptions &opts,
                                  Context &ctx) {
  Blob full_blob;
  if (ContainsBlob(blob_name, blob_id)) {
    // Case 1: The blob already exists (read from hermes)
    // Read blob from Hermes
    Get(blob_id, full_blob, ctx);
  } else {
    // Case 2: The blob did not exist (need to read from backend)
    // Read blob using adapter
    IoStatus status;
    auto io_client = IoClientFactory::Get(io_ctx.type_);
    full_blob.resize(opts.backend_size_);
    io_client->ReadBlob(full_blob,
                        io_ctx,
                        opts,
                        status);
  }
  // Ensure the blob can hold the update
  if (full_blob.size() < blob_off + blob_size) {
    return PARTIAL_GET_OR_CREATE_OVERFLOW;
  }
  // Modify the blob
  // TODO(llogan): we can avoid a copy here
  blob.resize(blob_size);
  memcpy(blob.data() + blob_off, full_blob.data() + blob_off, blob.size());
  return Status();
}

/**
 * Determine if the bucket contains \a blob_id BLOB
 * */
bool Bucket::ContainsBlob(const std::string &blob_name,
                          BlobId &blob_id) {
  GetBlobId(blob_name, blob_id);
  return blob_id.IsNull();
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
  lipc::string lnew_blob_name(new_blob_name);
  mdm_->LocalRenameBlob(id_, blob_id, lnew_blob_name);
}

/**
 * Delete \a blob_id blob
 * */
void Bucket::DestroyBlob(BlobId blob_id, Context &ctx) {
  mdm_->LocalDestroyBlob(id_, blob_id);
}

}  // namespace hermes::api
