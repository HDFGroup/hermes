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

#include "vbucket.h"
#include "hermes.h"

namespace hermes::api {

/**
 * Either gets or creates a VBucket.
 *
 * @param name the name of the vbucket
 * @param ctx any additional information
 * */
VBucket::VBucket(const std::string &name,
                 Context &ctx,
                 const IoClientContext &opts)
: mdm_(&HERMES->mdm_), ctx_(ctx) {
  hipc::string lname(name);
  // TODO(llogan): rpcify
  id_ = mdm_->LocalGetOrCreateVBucket(lname, opts);
}

/**
 * Rename a VBucket.
 *
 * @param name the name of the new vbucket
 * */
void VBucket::Rename(std::string name) {
  hipc::string lname(name);
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
