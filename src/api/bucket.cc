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
Status Bucket::Put(std::string blob_name, ConstBlobData blob,
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
 * Get \a blob_id Blob from the bucket
 * */
Blob Bucket::Get(BlobId blob_id, Context &ctx) {
  return mdm_->LocalBucketGetBlob(blob_id);;
}

/**
 * Rename \a blob_id blob to \a new_blob_name new name
 * */
void Bucket::RenameBlob(BlobId blob_id,
                        std::string new_blob_name, Context &ctx) {
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