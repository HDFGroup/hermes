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
                    const std::vector<std::string> &names);

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
                          const std::vector<std::string> &names) {
  Status result = 0;

  for (size_t i = 0; i < schemas.size(); ++i) {
    PlacementSchema &schema = schemas[i];
    if (schema.size()) {
      hermes::Blob blob = {};
      blob.data = (u8 *)blobs[i].data();
      blob.size = blobs[i].size() * sizeof(T);
      // TODO(chogan): @errorhandling What about partial failure?
      result = PlaceBlob(&hermes_->context_, &hermes_->rpc_, schema, blob,
                         names[i].c_str(), id_);
    } else {
      // TODO(chogan): @errorhandling
      result = 1;
    }
  }

  return result;
}

template<typename T>
Status Bucket::Put(std::vector<std::string> &names,
                   std::vector<std::vector<T>> &blobs, Context &ctx) {
  Status ret = 0;

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

    if (ret == 0) {
      ret = PlaceBlobs(schemas, blobs, names);
    } else {
      std::vector<SwapBlob> swapped_blobs =
        PutToSwap(&hermes_->context_, &hermes_->rpc_, id_, blobs, names);

      for (size_t i = 0; i < swapped_blobs.size(); ++i) {
        TriggerBufferOrganizer(&hermes_->rpc_, kPlaceInHierarchy, names[i],
                               swapped_blobs[i], ctx.buffer_organizer_retries);
      }
      ret = 0;
      // TODO(chogan): @errorhandling Signify in Status that the Blobs went to
      // swap space
    }
  } else {
    // TODO(chogan): @errorhandling
    ret = 1;
  }

  return ret;
}

}  // namespace api
}  // namespace hermes

#endif  // BUCKET_H_
