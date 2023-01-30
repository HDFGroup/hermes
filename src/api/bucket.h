//
// Created by lukemartinlogan on 12/4/22.
//

#ifndef HERMES_SRC_API_BUCKET_H_
#define HERMES_SRC_API_BUCKET_H_

#include "hermes_types.h"
#include "status.h"
#include "hermes.h"

namespace hermes::api {

class Bucket {
 private:
  MetadataManager *mdm_;
  BufferPool *bpm_;
  BucketId id_;
  std::string name_;
  Context ctx_;

  ///////////////////////////
  ////// Bucket Operations
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
  ////// Blob Operations
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
   * Put \a blob_id Blob into the bucket
   * */
  Status Put(std::string blob_name, const Blob &blob,
             BlobId &blob_id, Context &ctx);

  /**
   * Get \a blob_id Blob from the bucket
   * @WRAP_DEFAULT: ctx -> ctx_
   * @WRAP_PROTO: blob_id -> std::string blob_name
   * @WRAP_DEFAULT: blob_id -> GetBlobId(blob_name)
   * */
  Status Get(BlobId blob_id, Blob &blob, Context &ctx);

  /**
   * Rename \a blob_id blob to \a new_blob_name new name
   * */
  void RenameBlob(BlobId blob_id, std::string new_blob_name, Context &ctx);

  /**
   * Delete \a blob_id blob
   * */
  void DestroyBlob(BlobId blob_id, Context &ctx);

 public:
  RPC_AUTOGEN_START
  RPC_AUTOGEN_END
};

}

#endif  // HERMES_SRC_API_BUCKET_H_
