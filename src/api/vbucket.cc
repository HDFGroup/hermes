//
// Created by lukemartinlogan on 12/4/22.
//

#include "vbucket.h"
#include "hermes.h"

namespace hermes::api {

/**
 * Either gets or creates a VBucket.
 *
 * @param name the name of the vbucket
 * @param ctx any additional information
 * */
VBucket::VBucket(std::string name, Context &ctx) : mdm_(&HERMES->mdm_) {
  lipc::string lname(name);
  // TODO(llogan): rpcify
  id_ = mdm_->LocalGetOrCreateVBucket(lname);
}

/**
 * Rename a VBucket.
 *
 * @param name the name of the new vbucket
 * */
void VBucket::Rename(std::string name) {
  lipc::string lname(name);
  mdm_->LocalRenameVBucket(id_, lname);
}

/**
 * Associate a blob in a VBucket.
 *
 * @param blob_id the ID of the blob to associate
 * @param ctx any additional information
 * @return status
 * */
Status VBucket::Link(BlobId blob_id, Context &ctx) {
  mdm_->LocalVBucketLinkBlob(id_, blob_id);
  return Status();
}

/**
 * Unlink a blob from a VBucket.
 *
 * @param blob_id the ID of a blob
 * @param ctx any additional information
 * */
Status VBucket::Unlink(BlobId blob_id, Context &ctx) {
  mdm_->LocalVBucketUnlinkBlob(id_, blob_id);
  return Status();
}

/**
 * Destroys a VBucket and its contents
 * */
Status VBucket::Destroy() {
  mdm_->LocalDestroyVBucket(id_);
  return Status();
}

/**
 * Check if \a blob_id blob is linked in the VBucket
 * */
bool VBucket::ContainsBlob(BlobId blob_id) {
  return mdm_->LocalVBucketContainsBlob(id_, blob_id);
}

}  // namespace hermes::api