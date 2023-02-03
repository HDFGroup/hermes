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

#ifndef HERMES_SRC_API_VBUCKET_H_
#define HERMES_SRC_API_VBUCKET_H_

#include "hermes_types.h"
#include "status.h"
#include "metadata_manager.h"

namespace hermes::api {

class VBucket {
 private:
  MetadataManager *mdm_;
  VBucketId id_;
  Context ctx_;

 public:
  /**
   * Either gets or creates a VBucket.
   *
   * @param name the name of the vbucket
   * @param ctx any additional information
   * */
  VBucket(const std::string &name,
          Context &ctx,
          const IoClientContext &opts);

  /**
   * Rename a VBucket.
   *
   * @param name the name of the new vbucket
   * */
  void Rename(std::string name);

  /**
   * Associate a blob in a VBucket.
   *
   * @param blob_id the ID of the blob to associate
   * @param ctx any additional information
   * @return status
   * */
  Status Link(BlobId blob_id, Context &ctx);

  /**
   * Unlink a blob from a VBucket.
   *
   * @param blob_id the ID of a blob
   * @param ctx any additional information
   * */
  Status Unlink(BlobId blob_id, Context &ctx);

  /**
   * Destroys a VBucket and its contents
   * */
  Status Destroy();

  /**
   * Check if \a blob_id blob is linked in the VBucket
   * */
  bool ContainsBlob(BlobId blob_id);

  /**
   * Create + attach a trait to the VBucket.
   * */
  template<typename T, typename ...Args>
  void Attach(Args ...args) {
  }
};

}  // namespace hermes::api

#endif  // HERMES_SRC_API_VBUCKET_H_
