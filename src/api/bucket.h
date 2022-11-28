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

#include <glog/logging.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "dpe/round_robin.h"
#include "hermes.h"
#include "metadata_management.h"
#include "utils.h"

/** \file bucket.h */

namespace hermes {

namespace api {

/** \brief A container for Blob%s
 *
 */
class Bucket {
 private:
  /** The user-facing descriptor of this Bucket. */
  std::string name_;
  /** The internal descriptor of this Bucket. */
  hermes::BucketID id_;

 public:
  /** The internal Hermes instance within which to create this Bucket. */
  std::shared_ptr<Hermes> hermes_;
  /** The api::Context that controls all operations on this Bucket. */
  Context ctx_;

  // TODO(chogan): Think about copy/move constructor/assignment operators

  /** \brief Default constructor.
   *
   * Creates the "NULL" Bucket.
   */
  Bucket() : name_(""), id_{0, 0}, hermes_(nullptr) {
    LOG(INFO) << "Create NULL Bucket " << std::endl;
  }

  /** \brief Constructor.
   *
   * Create a Bucket with name \p initial_name, backed by Hermes instance \p h,
   * with optional Context \p ctx.
   *
   * \param initial_name The name of this Bucket.
   * \param h An initialized Hermes instance.
   * \param ctx An optional Context that controls the behavior of this Bucket.
   */
  Bucket(const std::string &initial_name, std::shared_ptr<Hermes> const &h,
         Context ctx = Context());

  /** \brief Releases the Bucket, decrementing its reference count.
   *
   * This does not free any resources. To remove the Bucket%'s metadata and free
   * its stored Blob%s, see Bucket::Destroy.
   */
  ~Bucket();

  /** \brief Get the user-facing name of the Bucket.
   *
   * \return The name of this Bucket.
   */
  std::string GetName() const;

  /** \brief Get the internal ID of the bucket.
   *
   * The ID is the internal representation of the Bucket%'s name.
   *
   * \return The internal Bucket ID.
   */
  u64 GetId() const;

  /** \brief Return true if the Bucket is valid.
   *
   * A valid Bucket is one that was successfully created, contains metadata
   * entries, has a valid ID, and has not been destroyed. An invalid Bucket
   * cannot be used.
   *
   * \return \bool{the Bucket is valid}
   */
  bool IsValid() const;

  /** \brief Return the total size in bytes of all Blob%s in this Bucket.
   *
   * \return The total size in bytes of all Blob%s in this Bucket.
   */
  size_t GetTotalBlobSize();

  /** \brief Put a Blob in this Bucket.
   *
   * Uses the Bucket's saved Context.
   *
   * \param name The name of the Blob to put.
   * \param data The Blob data.
   *
   * \return \status
   */
  template <typename T>
  Status Put(const std::string &name, const std::vector<T> &data);

  /** \brief Put a Blob in this Bucket using context.
   *
   * \param name BLOB name
   * \param data BLOB data
   * \param ctx context
   */
  template <typename T>
  Status Put(const std::string &name, const std::vector<T> &data, Context &ctx);

  /**
   * \brief Put a Blob in this Bucket.
   *
   * \param name The name of the Blob to Put
   * \param data The Blob's data.
   * \param size The size of the Blob in bytes.
   *
   * \return \status
   *
   * \pre The Bucket must be valid.
   * \pre The length of \p name in bytes must not exceed
   *      hermes::api::kMaxBlobNameSize.
   * \pre The Blob buffer \p data must not be \c nullptr unless \p size is 0.
   *
   * \return \status
   */
  Status Put(const std::string &name, const u8 *data, size_t size);

  /**
   * \overload
   * \param name BLOB name
   * \param data BLOB data
   * \param size BLOB size
   * \param ctx context
   */
  Status Put(const std::string &name, const u8 *data, size_t size,
             const Context &ctx);

  /** \brief Put a vector of Blob%s.
   *
   * \param names 1 or more names, each of which is no longer than
   * kMaxBlobNameSize bytes.
   * \param blobs 1 or more Blob%s.
   *
   * \pre The length of \p names and \p blobs should be equal.
   *
   * \return \status
   */
  template <typename T>
  Status Put(const std::vector<std::string> &names,
             const std::vector<std::vector<T>> &blobs);

  /** \overload
   *
   * \param names BLOB names
   * \param blobs BLOB data
   * \param ctx context
   */
  template <typename T>
  Status Put(const std::vector<std::string> &names,
             const std::vector<std::vector<T>> &blobs, const Context &ctx);

  /** \brief Get the size in bytes of the Blob referred to by \p name.
   *
   * \param name The name of the Blob to query.
   * \param ctx context
   */
  size_t GetBlobSize(const std::string &name, const Context &ctx);

  /** \overload
   *
   * \param arena An Arena backed by allocated memory.
   * \param name The name of the Blob to query.
   * \param ctx context
   */
  size_t GetBlobSize(Arena *arena, const std::string &name, const Context &ctx);

  /** \brief Get a blob from this Bucket.
   *
   * If if the size of \p user_blob is 0, return the minimum buffer size needed
   * to contain the Blob \p name, otherwise copy \p user_blob.size() bytes to \p
   * user_blob and return the number of bytes copied.
   *
   * \param name The name of the Blob to get.
   * \param user_blob User-provided storage for the retrieved Blob.
   *
   * \return The size in bytes of the Blob.
   */
  size_t Get(const std::string &name, Blob &user_blob);

  /** \overload
   *
   * \param name The name of the Blob to get.
   * \param user_blob User-provided storage for the retrieved Blob.
   * \param ctx context
   */
  size_t Get(const std::string &name, Blob &user_blob, const Context &ctx);

  /** \brief Retrieve multiple Blob%s in one call.
   *
   * \param names A list of names of the Blob%s to get.
   * \param blobs User-provided storage for the retrieved Blob%s.
   * \param \ctx{Get}
   *
   * \return The sizes in bytes of the Blob%s.
   *
   */
  std::vector<size_t> Get(const std::vector<std::string> &names,
                          std::vector<Blob> &blobs, const Context &ctx);

  /** \overload
   *
   */
  size_t Get(const std::string &name, void *user_blob, size_t blob_size,
             const Context &ctx);

  /** \brief Given an ordering of Blob%s, retrieves the Blob at index \p
   * blob_index + 1.
   *
   * By default Blob%s are arranged in the order in which they were Put. If
   * user_blob.size() == 0, return the minimum buffer size needed. If
   * user_blob.size() > 0, copy user_blob.size() bytes to user_blob and return
   * user_blob.size()
   *
   * \param blob_index The starting index.
   * \param user_blob User-provided memory for the Blob.
   *
   * \return The size in bytes of the retrieved Blob.
   */
  size_t GetNext(u64 blob_index, Blob &user_blob);

  /** \overload
   *
   * \param blob_index The starting index.
   * \param user_blob User-provided memory for the Blob.
   * \param ctx context
   */
  size_t GetNext(u64 blob_index, Blob &user_blob, const Context &ctx);

  /** \overload
   *
   * \param blob_index The starting index.
   * \param user_blob User-provided memory for the Blob.
   * \param blob_size Blob size
   * \param ctx context
   */
  size_t GetNext(u64 blob_index, void *user_blob, size_t blob_size,
                 const Context &ctx);

  // TODO(chogan):
  /** \brief Retrieves multiple blobs from the Bucket.
   *
   * The Blobs retrieved are the next ones from the passed blob_index
   */
  std::vector<size_t> GetNext(u64 blob_index, u64 count,
                              std::vector<Blob> &blobs, const Context &ctx);

  /** \brief Get Blob%(s) from this Bucket according to a predicate.
   *
   * \todo Not implemented yet.
   *
   * \return \status
   */
  template <class Predicate>
  Status GetV(void *user_blob, Predicate pred, Context &ctx);

  /** \brief Delete a Blob from this Bucket.
   *
   * \param name The name of the Blob to delete.
   *
   * \return \status
   */
  Status DeleteBlob(const std::string &name);

  /** \overload
   *
   * \param name The name of the Blob to delete.
   * \param ctx context
   */
  Status DeleteBlob(const std::string &name, const Context &ctx);

  /** \brief Rename a Blob in this Bucket.
   *
   * \param old_name The Blob to rename.
   * \param new_name The desired new name of the Blob.
   *
   * \pre The size in bytes of \p new_name must be <= to kMaxBlobNameSize.
   *
   * \return \status
   */
  Status RenameBlob(const std::string &old_name, const std::string &new_name);

  /** \overload
   *
   * \param old_name The Blob to rename.
   * \param new_name The desired new name of the Blob.
   * \param ctx context
   */
  Status RenameBlob(const std::string &old_name, const std::string &new_name,
                    const Context &ctx);

  /** \brief Returns true if the Bucket contains a Blob called \p name.
   *
   * \param name The name of the Blob to check.
   *
   * \return \bool{the Blob \p name is in this Bucket}
   */
  bool ContainsBlob(const std::string &name);

  /** \brief Return true if the Blob \p name is in swap space.
   *
   * \param name The name of the Blob to check.
   *
   * \return \bool{the Blob called \p name in this Bucket is in swap space}
   */
  bool BlobIsInSwap(const std::string &name);

  /** \brief Get a list of blob names filtered by \p pred.
   *
   * \todo Not implemented yet.
   */
  template <class Predicate>
  std::vector<std::string> GetBlobNames(Predicate pred, Context &ctx);

  /** \brief Rename this Bucket.
   *
   * \param new_name A new name for the Bucket.
   *
   * \pre The length of \p new_name in bytes should be less than
   * #kMaxBucketNameSize.
   *
   * \return \status
   */
  Status Rename(const std::string &new_name);

  /** \overload
   *
   * \param new_name A new name for the Bucket.
   * \param ctx context
   */
  Status Rename(const std::string &new_name, const Context &ctx);

  /** \brief Save this Bucket%'s Blob%s to persistent storage.
   *
   * The blobs are written in the same order in which they were \p Put.
   *
   * \param file_name The name of the file to persist the Blob's to.
   *
   * \return \status
   */
  Status Persist(const std::string &file_name);

  /** \overload
   * \param file_name The name of the file to persist the Blob's to.
   * \param ctx context
   */
  Status Persist(const std::string &file_name, const Context &ctx);

  /**
   * \brief Allign <tt>blob_name</tt>'s access speed to its importance.
   *
   * \param blob_name The name of the Blob to organize.
   *
   * \param epsilon
   * The threshold within which the Blob's access time should match its
   * importance. Constraint: 0 < epsilon <= 1.
   *
   * \param custom_importance
   * A measure of importance that overrides the internal importance of a Blob.
   * Constraint: 0 <= custom_importance <= 1, where 1 signifies the most
   * important Blob. By default the internal, statistics-based measure of
   * importance is used.
   */
  void OrganizeBlob(const std::string &blob_name, f32 epsilon,
                    f32 custom_importance = -1.f);

  /** \brief Release this Bucket.
   *
   * This function simply decrements the refcount to this Bucket in the Hermes
   * metadata. To free resources associated with this Bucket, call
   * Bucket::Destroy.
   *
   * \return \status
   */
  Status Release();

  /** \overload
   *
   * \param \ctx{call}
   */
  Status Release(const Context &ctx);

  /** \brief Destroy this Bucket.
   *
   * Deletes all metadata and Blob's associated with this Bucket.
   *
   * \pre The Bucket must have a reference count of 1. Other ranks must first
   * Bucket::Close the Bucket.
   *
   * \return \status
   */
  Status Destroy();

  /** \overload
   *
   * \param \ctx{call}.
   */
  Status Destroy(const Context &ctx);

 private:
  /** \brief Internal version of Put, called by all overloads.
   *
   * \return \status
   */
  template <typename T>
  Status PutInternal(const std::vector<std::string> &names,
                     const std::vector<size_t> &sizes,
                     const std::vector<std::vector<T>> &blobs,
                     const Context &ctx);
  /** \brief Low-level version of Put.
   *
   * \return \status
   */
  template <typename T>
  Status PlaceBlobs(std::vector<PlacementSchema> &schemas,
                    const std::vector<std::vector<T>> &blobs,
                    const std::vector<std::string> &names, const Context &ctx);
};

template <typename T>
Status Bucket::Put(const std::string &name, const std::vector<T> &data,
                   Context &ctx) {
  Status result = Put(name, (u8 *)data.data(), data.size() * sizeof(T), ctx);

  return result;
}

template <typename T>
Status Bucket::Put(const std::string &name, const std::vector<T> &data) {
  Status result = Put(name, data, ctx_);

  return result;
}

template <typename T>
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
      // TODO(chogan): Need to return a std::vector<Status>
      break;
    }
  }

  return result;
}

template <typename T>
Status Bucket::Put(const std::vector<std::string> &names,
                   const std::vector<std::vector<T>> &blobs) {
  Status result = Put(names, blobs, ctx_);

  return result;
}

template <typename T>
Status Bucket::PutInternal(const std::vector<std::string> &names,
                           const std::vector<size_t> &sizes,
                           const std::vector<std::vector<T>> &blobs,
                           const Context &ctx) {
  std::vector<PlacementSchema> schemas;
  HERMES_BEGIN_TIMED_BLOCK("CalculatePlacement");
  Status result = CalculatePlacement(&hermes_->context_, &hermes_->rpc_, sizes,
                                     schemas, ctx);
  HERMES_END_TIMED_BLOCK();

  if (result.Succeeded()) {
    result = PlaceBlobs(schemas, blobs, names, ctx);
  } else {
    LOG(ERROR) << result.Msg();
  }

  return result;
}

template <typename T>
Status Bucket::Put(const std::vector<std::string> &names,
                   const std::vector<std::vector<T>> &blobs,
                   const Context &ctx) {
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

    if (ctx.rr_retry) {
      int num_devices =
          GetLocalSystemViewState(&hermes_->context_)->num_devices;

      for (int i = 0; i < num_devices; ++i) {
        ret = PutInternal(names, sizes_in_bytes, blobs, ctx);

        if (ret.Failed()) {
          RoundRobin rr_state;
          int current = rr_state.GetCurrentDeviceIndex();
          rr_state.SetCurrentDeviceIndex((current + 1) % num_devices);
        } else {
          break;
        }
      }
    } else {
      ret = PutInternal(names, sizes_in_bytes, blobs, ctx);
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
