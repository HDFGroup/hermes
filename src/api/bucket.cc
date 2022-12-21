//
// Created by lukemartinlogan on 12/4/22.
//

#include "bucket.h"
// #include "data_placement_engine_factory.h"

namespace hermes::api {

/**
 * Either initialize or fetch the bucket.
 * */
Bucket::Bucket(std::string name, Context &ctx) : mdm_(&HERMES->mdm_) {
  lipc::string lname(name);
  id_ = mdm_->GetOrCreateBucket(lname);
}

/**
 * Rename this bucket
 * */
void Bucket::Rename(std::string new_bkt_name) {
}

/**
 * Destroys this bucket along with all its contents.
 * */
void Bucket::Destroy(std::string blob_name) {
}

/**
 * Put \a blob_id Blob into the bucket
 * */
Status Bucket::Put(std::string blob_name, Blob blob,
                   BlobId &blob_id, Context &ctx) {
  // Calculate placement
  /*auto dpe = DPEFactory::Get(ctx.policy);
  std::vector<size_t> blob_sizes(1, blob.size());
  std::vector<PlacementSchema> schema;
  dpe->CalculatePlacement(blob_sizes, schema, ctx);*/

  // Allocate buffers for the blob

}

/**
 * Get \a blob_id Blob from the bucket
 * :WRAP-param: ctx -> ctx_
 * */
Status Bucket::Get(BlobId blob_id, Blob &blob, Context &ctx) {
}

}  // namespace hermes::api