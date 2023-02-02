//
// Created by lukemartinlogan on 12/4/22.
//

#include "bucket.h"
#include "data_placement_engine_factory.h"

namespace hermes::api {

///////////////////////////
////// Bucket Operations
//////////////////////////

/**
 * Either initialize or fetch the bucket.
 * */
Bucket::Bucket(std::string name, Context &ctx)
    : mdm_(&HERMES->mdm_), bpm_(&HERMES->bpm_) {
  lipc::string lname(name);
  id_ = mdm_->LocalGetOrCreateBucket(lname);
}

/**
 * Get \a bkt_id bucket, which is known to exist.
 *
 * Used internally by Hermes.
 * */
Bucket::Bucket(BucketId bkt_id, Context &ctx)
: id_(bkt_id) {}

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
                         BlobId &blob_id, Context &ctx) {
  lipc::string lblob_name(blob_name);
  blob_id = mdm_->LocalGetBlobId(GetId(), lblob_name);
  return Status();
}


/**
 * Put \a blob_id Blob into the bucket
 * */
Status Bucket::Put(std::string blob_name, const Blob blob,
                   BlobId &blob_id, Context &ctx) {
  // Calculate placement
  auto dpe = DPEFactory::Get(ctx.policy);
  std::vector<size_t> blob_sizes(1, blob.size());
  std::vector<PlacementSchema> schemas;
  dpe->CalculatePlacement(blob_sizes, schemas, ctx);

  // Allocate buffers for the blob & enqueue placement
  for (auto &schema : schemas) {
    // TODO(llogan): rpcify
    auto buffers = bpm_->LocalAllocateAndSetBuffers(schema, blob);
    blob_id = mdm_->LocalBucketPutBlob(id_, lipc::string(blob_name),
                                       blob, buffers);
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
 * @param backend_off the offset to read from the backend if blob DNE
 * @param backend_size the size to read from the backend if blob DNE
 * @param backend_ctx which adapter to route I/O request if blob DNE
 * @param ctx any additional information
 * */
Status Bucket::PartialPutOrCreate(std::string blob_name,
                                  const Blob &blob,
                                  size_t blob_off,
                                  size_t backend_off,
                                  size_t backend_size,
                                  BlobId &blob_id,
                                  const IoClientContext &io_ctx,
                                  const IoClientOptions &opts,
                                  Context &ctx) {
  mdm_->LocalBucketPartialPutOrCreateBlob(
      id_,
      lipc::string(blob_name),
      blob,
      blob_off,
      backend_off,
      backend_size,
      io_ctx,
      opts,
      ctx);
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