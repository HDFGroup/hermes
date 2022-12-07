//
// Created by lukemartinlogan on 12/4/22.
//

#ifndef HERMES_SRC_API_BUCKET_H_
#define HERMES_SRC_API_BUCKET_H_

#include "hermes_types.h"
#include "hermes_status.h"
#include "hermes.h"

namespace hermes::api {

class Bucket {
 private:
  MetadataManager *mdm_;
  BucketID id_;
  std::string name_;
  Context ctx_;

  /* Bucket operations */
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
  BucketID GetId() const {
    return id_;
  }

  /**
   * Rename this bucket
   * */
  void Rename(std::string new_bkt_name);

  /**
   * Destroys this bucket along with all its contents.
   * */
  void Destroy(std::string blob_name);


  /* Blob operations */
 public:

  /**
   * Put \a blob_name Blob into the bucket
   * */
  Status PutBlob(std::string blob_name, Blob &blob,
                 Context &ctx);

  /**
   * Put \a blob_id Blob into the bucket
   * */
  Status PutBlob(BlobID blob_id, Blob &blob,
                 Context &ctx);

  /**
   * Get \a blob_name Blob from the bucket
   * */
  Status GetBlob(std::string blob_name, Blob &blob,
                 Context &ctx);

  /**
   * Get \a blob_id Blob from the bucket
   * :WRAP-param: ctx -> ctx_
   * */
  Status GetBlob(BlobID blob_id, Blob &blob,
                  Context &ctx);

 public:
  RPC_AUTOGEN_START
  RPC_AUTOGEN_END
};

}

#endif  // HERMES_SRC_API_BUCKET_H_
