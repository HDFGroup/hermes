#include "bucket.h"

#include <cstdio>
#include <iostream>
#include <vector>

#include "utils.h"
#include "buffer_pool.h"
#include "metadata_management.h"

namespace hermes {

namespace api {

Bucket::Bucket(const std::string &initial_name,
               const std::shared_ptr<Hermes> &h, Context ctx)
    : name_(initial_name), hermes_(h) {
  (void)ctx;

  if (IsBucketNameTooLong(name_)) {
    id_.as_int = 0;
  } else {
    id_ = GetOrCreateBucketId(&hermes_->context_, &hermes_->rpc_, name_);
  }
}

bool Bucket::IsValid() const {
  bool result = id_.as_int != 0;

  return result;
}

Status Bucket::Put(const std::string &name, const u8 *data, size_t size,
                   Context &ctx) {
  Status ret = 0;

  if (IsBlobNameTooLong(name)) {
    // TODO(chogan): @errorhandling
    ret = 1;
  }

  if (IsValid() && ret == 0) {
    std::vector<size_t> sizes(1, size);
    std::vector<PlacementSchema> schemas;
    HERMES_BEGIN_TIMED_BLOCK("CalculatePlacement");
    ret = CalculatePlacement(&hermes_->context_, &hermes_->rpc_, sizes, schemas,
                             ctx);
    HERMES_END_TIMED_BLOCK();

    if (ret == 0) {
      std::vector<std::string> names(1, name);
      std::vector<std::vector<u8>> blobs(1);
      blobs[0].resize(size);
      // TODO(chogan): Create a PreallocatedMemory allocator for std::vector so
      // that a single-blob-Put doesn't perform a copy
      for (size_t i = 0; i < size; ++i) {
        blobs[0][i] = data[i];
      }
      ret = PlaceBlobs(schemas, blobs, names);
    } else {
      SwapBlob swap_blob = PutToSwap(&hermes_->context_, &hermes_->rpc_, name,
                                     id_, data, size);
      TriggerBufferOrganizer(&hermes_->rpc_, kPlaceInHierarchy, name, swap_blob,
                             ctx.buffer_organizer_retries);
      ret = 0;
      // TODO(chogan): @errorhandling Signify in Status that the Blob went to
      // swap space
    }
  }

  return ret;
}

size_t Bucket::GetBlobSize(Arena *arena, const std::string &name,
                           Context &ctx) {
  (void)ctx;
  size_t result = 0;

  if (IsValid()) {
    LOG(INFO) << "Getting Blob " << name << " size from bucket "
              << name_ << '\n';
    BlobID blob_id = GetBlobIdByName(&hermes_->context_, &hermes_->rpc_,
                                     name.c_str());
    result = GetBlobSizeById(&hermes_->context_, &hermes_->rpc_, arena,
                             blob_id);
  }

  return result;
}

size_t Bucket::Get(const std::string &name, Blob &user_blob, Context &ctx) {
  (void)ctx;

  size_t ret = 0;

  if (IsValid()) {
    // TODO(chogan): Assumes scratch is big enough to hold buffer_ids
    ScopedTemporaryMemory scratch(&hermes_->trans_arena_);

    if (user_blob.size() == 0) {
      ret = GetBlobSize(scratch, name, ctx);
    } else {
      LOG(INFO) << "Getting Blob " << name << " from bucket " << name_ << '\n';
      BlobID blob_id = GetBlobIdByName(&hermes_->context_, &hermes_->rpc_,
                                       name.c_str());
      ret = ReadBlobById(&hermes_->context_, &hermes_->rpc_,
                         &hermes_->trans_arena_, user_blob, blob_id);
    }
  }

  return ret;
}

template<class Predicate>
Status Bucket::GetV(void *user_blob, Predicate pred, Context &ctx) {
  (void)user_blob;
  (void)ctx;
  Status ret = 0;

  LOG(INFO) << "Getting blobs by predicate from bucket " << name_ << '\n';

  return ret;
}

Status Bucket::DeleteBlob(const std::string &name, Context &ctx) {
  (void)ctx;
  Status ret = 0;

  LOG(INFO) << "Deleting Blob " << name << " from bucket " << name_ << '\n';
  DestroyBlobByName(&hermes_->context_, &hermes_->rpc_, id_, name);

  return ret;
}

Status Bucket::RenameBlob(const std::string &old_name,
                          const std::string &new_name,
                          Context &ctx) {
  (void)ctx;
  Status ret = 0;

  if (IsBlobNameTooLong(new_name)) {
    ret = 1;
    // TODO(chogan): @errorhandling
  } else {
    LOG(INFO) << "Renaming Blob " << old_name << " to " << new_name << '\n';
    hermes::RenameBlob(&hermes_->context_, &hermes_->rpc_, old_name, new_name);
  }

  return ret;
}

bool Bucket::ContainsBlob(const std::string &name) {
  bool result = hermes::ContainsBlob(&hermes_->context_, &hermes_->rpc_, id_,
                                     name);

  return result;
}

bool Bucket::BlobIsInSwap(const std::string &name) {
  BlobID blob_id = GetBlobIdByName(&hermes_->context_, &hermes_->rpc_,
                                   name.c_str());
  bool result = hermes::BlobIsInSwap(blob_id);

  return result;
}

template<class Predicate>
std::vector<std::string> Bucket::GetBlobNames(Predicate pred,
                                              Context &ctx) {
  (void)ctx;

  LOG(INFO) << "Getting blob names by predicate from bucket " << name_ << '\n';

  return std::vector<std::string>();
}

struct bkt_info * Bucket::GetInfo(Context &ctx) {
  (void)ctx;
  struct bkt_info *ret = nullptr;

  LOG(INFO) << "Getting bucket information from bucket " << name_ << '\n';

  return ret;
}

Status Bucket::Rename(const std::string &new_name, Context &ctx) {
  (void)ctx;
  Status ret = 0;

  if (IsBucketNameTooLong(new_name)) {
    ret = 1;
    // TODO(chogan): @errorhandling
  } else {
    LOG(INFO) << "Renaming a bucket to" << new_name << '\n';
    RenameBucket(&hermes_->context_, &hermes_->rpc_, id_, name_, new_name);
  }

  return ret;
}

Status Bucket::Persist(const std::string &file_name, Context &ctx) {
  (void)ctx;
  // TODO(chogan): Once we have Traits, we need to let users control the mode
  // when we're, for example, updating an existing file. For now we just assume
  // we're always creating a new file.
  std::string open_mode = "w";

  // TODO(chogan): Support other storage backends
  Status result = StdIoPersistBucket(&hermes_->context_, &hermes_->rpc_,
                                     &hermes_->trans_arena_, id_, file_name,
                                     open_mode);

  return result;
}

Status Bucket::Close(Context &ctx) {
  (void)ctx;
  Status ret = 0;

  if (IsValid()) {
    LOG(INFO) << "Closing bucket '" << name_ << "'" << std::endl;
    DecrementRefcount(&hermes_->context_, &hermes_->rpc_, id_);
    id_.as_int = 0;
  }

  return ret;
}

Status Bucket::Destroy(Context &ctx) {
  (void)ctx;
  Status result = 0;

  if (IsValid()) {
    LOG(INFO) << "Destroying bucket '" << name_ << "'" << std::endl;
    bool destroyed = DestroyBucket(&hermes_->context_, &hermes_->rpc_,
                                   name_.c_str(), id_);
    if (destroyed) {
      id_.as_int = 0;
    } else {
      // TODO(chogan): @errorhandling
      result = 1;
    }
  }

  return result;
}

}  // namespace api
}  // namespace hermes
