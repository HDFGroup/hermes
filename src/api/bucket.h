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
  /** This Bucket's Context. */
  Context ctx_;

  // TODO(chogan): Think about the Big Three
  Bucket() : name_(""), id_{0, 0}, hermes_(nullptr) {
    LOG(INFO) << "Create NULL Bucket " << std::endl;
  }

  Bucket(const std::string &initial_name, std::shared_ptr<Hermes> const &h,
         Context ctx = Context());

  /**
   * \brief Releases the Bucket, decrementing its reference count
   *
   * This does not free any resources. To remove the Bucket from the
   * MetadataManager and free its stored Blobs, see Bucket::Destroy.
   */
  ~Bucket();

  /** get the name of bucket */
  std::string GetName() const;

  /** get the internal ID of the bucket */
  u64 GetId() const;

  /** returns true if this Bucket has been created but not yet destroyed */
  bool IsValid() const;

  /** put a blob on this bucket */
  template<typename T>
  Status Put(const std::string &name, const std::vector<T> &data, Context &ctx);

  /**
   *
   */
  template<typename T>
  Status Put(const std::string &name, const std::vector<T> &data);

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
             const Context &ctx);

  /**
   *
   */
  Status Put(const std::string &name, const u8 *data, size_t size);

  /**
   *
   */
  template<typename T>
  Status Put(std::vector<std::string> &names,
             std::vector<std::vector<T>> &blobs, const Context &ctx);

  /**
   *
   */
  template<typename T>
  Status Put(std::vector<std::string> &names,
             std::vector<std::vector<T>> &blobs);

  /**
   *
   */
  template<typename T>
  Status PlaceBlobs(std::vector<PlacementSchema> &schemas,
                    const std::vector<std::vector<T>> &blobs,
                    const std::vector<std::string> &names, const Context &ctx);

  /** Get the size in bytes of the Blob referred to by `name` */
  size_t GetBlobSize(Arena *arena, const std::string &name, const Context &ctx);

  /** get a blob on this bucket */
  /** - if user_blob.size() == 0 => return the minimum buffer size needed */
  /** - if user_blob.size() > 0 => copy user_blob.size() bytes */
  /** to user_blob and return user_blob.size() */
  /** use provides buffer */
  size_t Get(const std::string &name, Blob& user_blob, const Context &ctx);
  size_t Get(const std::string &name, Blob& user_blob);

  /**
   * \brief Retrieve multiple Blobs in one call.
   */
  std::vector<size_t> Get(const std::vector<std::string> &names,
                          std::vector<Blob> &blobs, const Context &ctx);

  /**
   * \brief Retrieve a Blob into a user buffer.
   */
  size_t Get(const std::string &name, void *user_blob, size_t blob_size,
             const Context &ctx);

  /** get blob(s) on this bucket according to predicate */
  /** use provides buffer */
  template<class Predicate>
  Status GetV(void *user_blob, Predicate pred, Context &ctx);

  /** delete a blob from this bucket */
  Status DeleteBlob(const std::string &name, const Context &ctx);
  Status DeleteBlob(const std::string &name);

  /** rename a blob on this bucket */
  Status RenameBlob(const std::string &old_name, const std::string &new_name,
                    const Context &ctx);
  Status RenameBlob(const std::string &old_name, const std::string &new_name);

  /** Returns true if the Bucket contains a Blob called `name` */
  bool ContainsBlob(const std::string &name);

  /** Returns true if the Blob called `name` in this bucket is in swap space */
  bool BlobIsInSwap(const std::string &name);

  /** get a list of blob names filtered by pred */
  template<class Predicate>
  std::vector<std::string> GetBlobNames(Predicate pred, Context &ctx);

  /** rename this bucket */
  Status Rename(const std::string& new_name, const Context &ctx);
  Status Rename(const std::string& new_name);

  /** Save this bucket's blobs to persistent storage.
   *
   * The blobs are written in the same order in which they are `Put`. */
  Status Persist(const std::string &file_name, const Context &ctx);
  Status Persist(const std::string &file_name);

  /**
   * \brief Release this Bucket
   *
   * This simpley decrements the refcount to this Bucket in the Hermes metadata.
   * To free resources associated with this Bucket, call Bucket::Destroy.
   */
  Status Release(const Context &ctx);
  Status Release();

  /** destroy this bucket */
  /** ctx controls "aggressiveness */
  Status Destroy(const Context &ctx);
  Status Destroy();
};

template<typename T>
Status Bucket::Put(const std::string &name, const std::vector<T> &data,
                   Context &ctx) {
  Status result = Put(name, (u8 *)data.data(), data.size() * sizeof(T), ctx);

  return result;
}

template<typename T>
Status Bucket::Put(const std::string &name, const std::vector<T> &data) {
  Status result = Put(name, data, ctx_);

  return result;
}

template<typename T>
Status Bucket::PlaceBlobs(std::vector<PlacementSchema> &schemas,
                          const std::vector<std::vector<T>> &blobs,
                          const std::vector<std::string> &names,
                          const Context &ctx) {
  Status result;

  for (size_t i = 0; i < schemas.size(); ++i) {
    PlacementSchema &schema = schemas[i];
    hermes::Blob blob = {};
    blob.data = (u8 *)blobs[i].data();
    blob.size = blobs[i].size() * sizeof(T);
    LOG(INFO) << "Attaching blob '" << names[i] << "' to Bucket '" << name_
              << "'" << std::endl;
    result = PlaceBlob(&hermes_->context_, &hermes_->rpc_, schema, blob,
                       names[i], id_, ctx);
    if (result.Failed()) {
      break;
    }
  }

  return result;
}

template<typename T>
Status Bucket::Put(std::vector<std::string> &names,
                   std::vector<std::vector<T>> &blobs) {
  Status result = Put(names, blobs, ctx_);

  return result;
}

template<typename T>
Status Bucket::Put(std::vector<std::string> &names,
                   std::vector<std::vector<T>> &blobs, const Context &ctx) {
  Status ret;

  for (auto &name : names) {
    if (IsBlobNameTooLong(name)) {
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

    int num_devices = 2;
    int device = 0;
    while (ret.Failed() && device++ < num_devices) {
      std::vector<PlacementSchema> schemas;
      HERMES_BEGIN_TIMED_BLOCK("CalculatePlacement");
      ret = CalculatePlacement(&hermes_->context_, &hermes_->rpc_, sizes_in_bytes,
                               schemas, ctx);
      HERMES_END_TIMED_BLOCK();

      if (ret.Succeeded()) {
        ret = PlaceBlobs(schemas, blobs, names, ctx);
      } else {
        LOG(ERROR) << ret.Msg();
        return ret;
      }

      if (ret.Failed()) {
        DataPlacementEngine dpe;
        int current = dpe.GetCurrentDeviceIndex();
        dpe.SetCurrentDeviceIndex((current + 1) % num_devices);
      }
    }

  } else {
    ret = INVALID_BUCKET;
    LOG(ERROR) << ret.Msg();
    return ret;
  }

  return ret;
}

}  // namespace api
}  // namespace hermes

#endif  // BUCKET_H_
