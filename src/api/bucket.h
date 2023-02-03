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

#ifndef HERMES_SRC_API_BUCKET_H_
#define HERMES_SRC_API_BUCKET_H_

#include "hermes_types.h"
#include "status.h"
#include "buffer_pool.h"
#include "adapter/io_client/io_client_factory.h"

namespace hermes::api {

using hermes::adapter::IoClientObject;

class Bucket {
 private:
  MetadataManager *mdm_;
  BufferPool *bpm_;
  BucketId id_;
  std::string name_;
  Context ctx_;

  ///////////////////////////
  /// Bucket Operations
  //////////////////////////
 public:
  /**
   * Get or create \a bkt_name bucket.
   *
   * Called from hermes.h in GetBucket(). Should not
   * be used directly.
   * */
  Bucket(const std::string &bkt_name,
         Context &ctx,
         const IoClientContext &opts);

 public:
  /**
   * Get the name of this bucket. Name is cached instead of
   * making an RPC. Not coherent if Rename is called.
   * */
  std::string GetName() const {
    return name_;
  }

  /**
   * Get the identifier of this bucket
   * */
  BucketId GetId() const {
    return id_;
  }

  /**
   * Get the current size of the bucket
   * */
  size_t GetSize(IoClientContext opts = IoClientContext());

  /**
   * Rename this bucket
   * */
  void Rename(std::string new_bkt_name);

  /**
   * Destroys this bucket along with all its contents.
   * */
  void Destroy();

  /**
   * Check if this bucket is valid
   * */
  bool IsNull() {
    return id_.IsNull();
  }


  ///////////////////////
  /// Blob Operations
  ///////////////////////
 public:
  /**
   * Get the id of a blob from the blob name
   *
   * @param blob_name the name of the blob
   * @param blob_id (output) the returned blob_id
   * @return The Status of the operation
   * */
  Status GetBlobId(std::string blob_name, BlobId &blob_id);

  /**
   * Put \a blob_name Blob into the bucket
   * */
  Status Put(std::string blob_name,
             const Blob &blob,
             BlobId &blob_id,
             Context &ctx,
             IoClientContext opts = IoClientContext());

  /**
   * Put \a blob_name Blob into the bucket. Load the blob from the
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
  Status PartialPutOrCreate(const std::string &blob_name,
                            const Blob &blob,
                            size_t blob_off,
                            BlobId &blob_id,
                            const IoClientContext &opts,
                            Context &ctx);

  /**
   * Get \a blob_id Blob from the bucket
   * */
  Status Get(BlobId blob_id,
             Blob &blob,
             Context &ctx);

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
  Status PartialGetOrCreate(const std::string &blob_name,
                            Blob &blob,
                            size_t blob_off,
                            size_t blob_size,
                            BlobId &blob_id,
                            const IoClientContext &opts,
                            Context &ctx);

  /**
   * Determine if the bucket contains \a blob_id BLOB
   * */
  bool ContainsBlob(const std::string &blob_name,
                    BlobId &blob_id);

  /**
   * Determine if the bucket contains \a blob_id BLOB
   * */
  bool ContainsBlob(BlobId blob_id);

  /**
   * Rename \a blob_id blob to \a new_blob_name new name
   * */
  void RenameBlob(BlobId blob_id, std::string new_blob_name, Context &ctx);

  /**
   * Delete \a blob_id blob
   * */
  void DestroyBlob(BlobId blob_id, Context &ctx);
};

}  // namespace hermes::api

#endif  // HERMES_SRC_API_BUCKET_H_
