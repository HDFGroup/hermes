/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Distributed under BSD 3-Clause license.                                   *
* Copyright by The HDF Group.                                               *
* Copyright by the Illinois Institute of Technology.                        *
* All rights reserved.                                                      *
*                                                                           *
* This file is part of Hermes. The full Hermes copyright notice, including  *
* terms governing use, modification, and redistribution, is contained in    *
* the COPYFILE, which can be found at the top directory. If you do not have *
* access to either file, you may request a copy from help@hdfgroup.org.     *
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#ifndef BUCKET_H_
#define BUCKET_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "glog/logging.h"

#include "hermes.h"
#include "data_placement_engine.h"
#include "metadata_management.h"
#include "utils.h"

namespace hermes {

namespace api {

class Bucket {
 private:
  std::string name_;
  hermes::BucketID id_;

 public:
  /** internal Hermes object owned by Bucket */
  std::shared_ptr<Hermes> hermes_;

  // TODO(chogan): Think about the Big Three
  Bucket() : name_(""), id_{0, 0}, hermes_(nullptr) {
    LOG(INFO) << "Create NULL Bucket " << std::endl;
  }

  Bucket(const std::string &initial_name, std::shared_ptr<Hermes> const &h,
         Context ctx);

  ~Bucket() {
    // TODO(chogan): Should we close implicitly by default?
    // Context ctx;
    // Close(ctx);

    name_.clear();
    id_.as_int = 0;
  }

  /** get the name of bucket */
  std::string GetName() const {
    return this->name_;
  }

  /** get the internal ID of the bucket */
  u64 GetId() const {
    return id_.as_int;
  }

  /** returns true if this Bucket has been created but not yet destroyed */
  bool IsValid() const;

  /** put a blob on this bucket */
  template<typename T>
  Status Put(const std::string &name, const std::vector<T> &data, Context &ctx);

  /**
   * \brief Puts a blob to a bucket
   *
   * \param name A blob name
   * \param data A blob buffer
   * \param size The number of blob bytes in buffer
   * \param ctx A Hermes context
   *
   * \return The return code/status
   *
   * \pre The bucket must be valid.
   * \pre The blob name \p name length (as byte array) must not exceed #kMaxBlobName.
   * \pre The blob buffer \p data must not be \c nullptr unless \p size is 0.
   * \pre If \p size is positive \p data must not be \c nullptr.
   *
   */
  Status Put(const std::string &name, const u8 *data, size_t size,
             Context &ctx);

  /**
   *
   */
  template<typename T>
  Status PlaceBlobs(std::vector<PlacementSchema> &schemas,
                    const std::vector<std::vector<T>> &blobs,
                    const std::vector<std::string> &names, int retries);

  /**
   *
   */
  template<typename T>
  Status Put(std::vector<std::string> &names,
             std::vector<std::vector<T>> &blobs, Context &ctx);

  /** Get the size in bytes of the Blob referred to by `name` */
  size_t GetBlobSize(Arena *arena, const std::string &name, Context &ctx);

  /** get a blob on this bucket */
  /** - if user_blob.size() == 0 => return the minimum buffer size needed */
  /** - if user_blob.size() > 0 => copy user_blob.size() bytes */
  /** to user_blob and return user_blob.size() */
  /** use provides buffer */
  size_t Get(const std::string &name, Blob& user_blob, Context &ctx);

  /** get blob(s) on this bucket according to predicate */
  /** use provides buffer */
  template<class Predicate>
  Status GetV(void *user_blob, Predicate pred, Context &ctx);

  /** delete a blob from this bucket */
  Status DeleteBlob(const std::string &name, Context &ctx);

  /** rename a blob on this bucket */
  Status RenameBlob(const std::string &old_name, const std::string &new_name,
                    Context &ctx);

  /** Returns true if the Bucket contains a Blob called `name` */
  bool ContainsBlob(const std::string &name);

  /** Returns true if the Blob called `name` in this bucket is in swap space */
  bool BlobIsInSwap(const std::string &name);

  /** get a list of blob names filtered by pred */
  template<class Predicate>
  std::vector<std::string> GetBlobNames(Predicate pred, Context &ctx);

  /** get information from the bucket at level-of-detail  */
  struct bkt_info * GetInfo(Context &ctx);

  /** rename this bucket */
  Status Rename(const std::string& new_name, Context &ctx);

  /** Save this bucket's blobs to persistent storage.
   *
   * The blobs are written in the same order in which they are `Put`. */
  Status Persist(const std::string &file_name, Context &ctx);

  /** close this bucket and free its associated resources (?) */
  /** Invalidates handle */
  Status Close(Context &ctx);

  /** destroy this bucket */
  /** ctx controls "aggressiveness */
  Status Destroy(Context &ctx);
};

template<typename T>
Status Bucket::Put(const std::string &name, const std::vector<T> &data,
                   Context &ctx) {
  Status result = Put(name, (u8 *)data.data(), data.size() * sizeof(T), ctx);

  return result;
}

template<typename T>
Status Bucket::PlaceBlobs(std::vector<PlacementSchema> &schemas,
                          const std::vector<std::vector<T>> &blobs,
                          const std::vector<std::string> &names, int retries) {
  Status result;

  for (size_t i = 0; i < schemas.size(); ++i) {
    PlacementSchema &schema = schemas[i];
    hermes::Blob blob = {};
    blob.data = (u8 *)blobs[i].data();
    blob.size = blobs[i].size() * sizeof(T);
    LOG(INFO) << "Attaching blob '" << names[i] << "' to Bucket '" << name_
              << "'" << std::endl;
    result = PlaceBlob(&hermes_->context_, &hermes_->rpc_, schema, blob,
                       names[i], id_, retries);
  }

  return result;
}

template<typename T>
Status Bucket::Put(std::vector<std::string> &names,
                   std::vector<std::vector<T>> &blobs, Context &ctx) {
  Status ret;

  for (auto &name : names) {
    if (IsBlobNameTooLong(name)) {
      // TODO(chogan): @errorhandling
      ret = BLOB_NAME_TOO_LONG;
      LOG(ERROR) << ret.Msg();
      return ret;
    }
  }

  if (blobs.size() == 0) {
    ret = INVALID_BLOB;
    LOG(ERROR) << ret.Msg();
    return ret;
  }

  if (IsValid()) {
    size_t num_blobs = blobs.size();
    std::vector<size_t> sizes_in_bytes(num_blobs);
    for (size_t i = 0; i < num_blobs; ++i) {
      sizes_in_bytes[i] = blobs[i].size() * sizeof(T);
    }
    std::vector<PlacementSchema> schemas;
    HERMES_BEGIN_TIMED_BLOCK("CalculatePlacement");
    ret = CalculatePlacement(&hermes_->context_, &hermes_->rpc_, sizes_in_bytes,
                             schemas, ctx);
    HERMES_END_TIMED_BLOCK();

    if (ret.Succeeded()) {
      ret = PlaceBlobs(schemas, blobs, names, ctx.buffer_organizer_retries);
    } else {
      // TODO(chogan): @errorhandling No space left or contraints unsatisfiable.
      LOG(ERROR) << ret.Msg();
      return ret;
    }
  } else {
    // TODO(chogan): @errorhandling
    ret = INVALID_BUCKET;
    LOG(ERROR) << ret.Msg();
    return ret;
  }

  return ret;
}

}  // namespace api
}  // namespace hermes

#endif  // BUCKET_H_
