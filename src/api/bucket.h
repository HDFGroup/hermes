//
// Created by lukemartinlogan on 12/4/22.
//

#ifndef HERMES_SRC_API_BUCKET_H_
#define HERMES_SRC_API_BUCKET_H_

#include "hermes_types.h"
#include "status.h"
#include "buffer_pool.h"
#include "adapter/io_client/io_client_factory.h"

namespace hermes::api {

using hermes::adapter::IoClientContext;

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
  Bucket(std::string bkt_name,
         Context &ctx);

  /**
   * Get \a bkt_id bucket, which is known to exist.
   *
   * Used internally by Hermes.
   * */
  Bucket(BucketId bkt_id, Context &ctx);

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
   * Set the size of bucket on storage
   * */
  void SetBackendSize();

  /**
   * Get the size of the data on storage
   * */
  void GetBackendSize();

  /**
   * Get the current size of the bucket
   * */
  size_t GetSize();

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
   * @param ctx any additional information
   * @return The Status of the operation
   * */
  Status GetBlobId(std::string blob_name, BlobId &blob_id, Context &ctx);

  /**
   * Put \a blob_name Blob into the bucket
   * */
  Status Put(std::string blob_name,
             const Blob blob,
             BlobId &blob_id,
             Context &ctx);

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
  Status PartialPutOrCreate(std::string blob_name,
                            const Blob &blob,
                            size_t blob_off,
                            size_t backend_off,
                            size_t backend_size,
                            BlobId &blob_id,
                            const IoClientContext &io_ctx,
                            const IoClientOptions &opts,
                            Context &ctx);

  /**
   * Get \a blob_id Blob from the bucket
   * */
  Status Get(BlobId blob_id,
             Blob &blob,
             Context &ctx);

  /**
   * Rename \a blob_id blob to \a new_blob_name new name
   * */
  void RenameBlob(BlobId blob_id, std::string new_blob_name, Context &ctx);

  /**
   * Delete \a blob_id blob
   * */
  void DestroyBlob(BlobId blob_id, Context &ctx);
};

}

#endif  // HERMES_SRC_API_BUCKET_H_
