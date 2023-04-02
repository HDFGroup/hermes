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

namespace hermes::api {

class Bucket {
 private:
  MetadataManager *mdm_;
  BufferPool *bpm_;
  TagId id_;
  std::string name_;
  Context ctx_;
  bool did_create_;

 public:
  /**====================================
   * Bucket Operations
   * ===================================*/

  /**
   * Get or create \a bkt_name bucket.
   *
   * Called from hermes.h in GetBucket(). Should not
   * be used directly.
   * */
  explicit Bucket(const std::string &bkt_name,
                  Context &ctx,
                  size_t backend_size = 0);

  /**
   * Check if the bucket was created in the constructor
   * */
  bool DidCreate() {
    return did_create_;
  }

  /**
   * Get an existing bucket.
   * */
  explicit Bucket(TagId tag_id);

 public:
  /**
   * Get the name of this bucket. Name is cached instead of
   * making an RPC. Not coherent if Rename is called.
   * */
  const std::string& GetName() const {
    return name_;
  }

  /**
   * Get the identifier of this bucket
   * */
  TagId GetId() const {
    return id_;
  }

  /**
   * Get the context object of this bucket
   * */
  Context& GetContext() {
    return ctx_;
  }

  /**
   * Attach a trait to the bucket
   * */
  void AttachTrait(TraitId trait_id);

  /**
   * Get the current size of the bucket
   * */
  size_t GetSize(bool backend = false);

  /**
   * Update the size of the bucket
   * Needed for the adapters for now.
   * */
  void UpdateSize(ssize_t delta, BucketUpdate mode);

  /**
   * Rename this bucket
   * */
  void Rename(const std::string &new_bkt_name);

  /**
   * Clears the buckets contents, but doesn't destroy its metadata
   * */
  void Clear(bool backend = false);

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

 public:
  /**====================================
   * Blob Operations
   * ===================================*/

  /**
   * Get the id of a blob from the blob name
   *
   * @param blob_name the name of the blob
   * @param blob_id (output) the returned blob_id
   * @return The Status of the operation
   * */
  Status GetBlobId(const std::string &blob_name, BlobId &blob_id);

  /**
   * Get the name of a blob from the blob id
   *
   * @param blob_id the blob_id
   * @param blob_name the name of the blob
   * @return The Status of the operation
   * */
  std::string GetBlobName(const BlobId &blob_id);


  /**
   * Get the score of a blob from the blob id
   *
   * @param blob_id the blob_id
   * @return The Status of the operation
   * */
  float GetBlobScore(const BlobId &blob_id);

  /**
   * Lock the blob
   * */
  bool LockBlob(BlobId blob_id, MdLockType lock_type);

  /**
   * Unlock the blob
   * */
  bool UnlockBlob(BlobId blob_id, MdLockType lock_type);

  /**
   * Create \a blob_name EMPTY BLOB if it does not already exist.
   * */
  Status TryCreateBlob(const std::string &blob_name,
                       BlobId &blob_id,
                       Context &ctx);

  /**
   * Label \a blob_id blob with \a tag_name TAG
   * */
  Status TagBlob(BlobId &blob_id,
                 TagId &tag_id);

  /**
   * Put \a blob_name Blob into the bucket
   * */
  Status Put(std::string blob_name,
             const Blob &blob,
             BlobId &blob_id,
             Context &ctx);

  /**
   * Get \a blob_id Blob from the bucket
   * */
  Status Get(BlobId blob_id,
             Blob &blob,
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

  /**
   * Get the set of blob IDs contained in the bucket
   * */
  std::vector<BlobId> GetContainedBlobIds();
};

}  // namespace hermes::api

#endif  // HERMES_SRC_API_BUCKET_H_
