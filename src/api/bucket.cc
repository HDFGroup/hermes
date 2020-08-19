#include "bucket.h"
#include "buffer_pool.h"
#include "metadata_management.h"
#include "data_placement_engine.h"

#include <iostream>

namespace hermes {

namespace api {

Bucket::Bucket(const std::string &initial_name,
               const std::shared_ptr<Hermes> &h, Context ctx)
    : name_(initial_name), hermes_(h) {
  (void)ctx;
  id_ = GetOrCreateBucketId(&hermes_->context_, &hermes_->rpc_, name_);
}

bool Bucket::IsValid() const {
  bool result = id_.as_int != 0;

  return result;
}

Status Bucket::Put(const std::string &name, const u8 *data, size_t size,
                   Context &ctx) {
  Status ret = 0;

  if (IsValid()) {
    std::vector<size_t> sizes(1, size);
    std::vector<TieredSchema> schemas; 
    ret = CalculatePlacement(&hermes_->context_, &hermes_->rpc_, sizes, schemas,
                             ctx);

    if (ret == 0) {
      std::vector<std::string> names(1, name);
      std::vector<std::vector<u8>> blobs(1);
      blobs[0].resize(size);
      // TODO(chogan): It would be nice to have a single-blob-Put that doesn't
      // perform a copy
      for (size_t i = 0; i < size; ++i) {
        blobs[0][i] = data[i];
      }
      ret = PlaceBlobs(schemas, blobs, names);
    }
  }

  return ret;
}

size_t Bucket::Get(const std::string &name, Blob& user_blob, Context &ctx) {
  (void)ctx;

  size_t ret = 0;

  if (IsValid()) {
    // TODO(chogan): Do this in iterations if scratch isn't big enough to hold
    // everything
    ScopedTemporaryMemory scratch(&hermes_->trans_arena_);

    if (user_blob.size() == 0) {
      LOG(INFO) << "Getting Blob " << name << " size from bucket "
                << name_ << '\n';
      BufferIdArray buffer_ids =
        GetBufferIdsFromBlobName(scratch, &hermes_->context_, &hermes_->rpc_,
                                 name.c_str(), NULL);
      ret = GetBlobSize(&hermes_->context_, &hermes_->rpc_, &buffer_ids);
    } else {
      LOG(INFO) << "Getting Blob " << name << " from bucket " << name_ << '\n';
      u32 *buffer_sizes = 0;
      BufferIdArray buffer_ids =
        GetBufferIdsFromBlobName(scratch, &hermes_->context_, &hermes_->rpc_,
                                 name.c_str(), &buffer_sizes);
      hermes::Blob blob = {};
      blob.data = user_blob.data();
      blob.size = user_blob.size();

      ret = ReadBlobFromBuffers(&hermes_->context_, &hermes_->rpc_, &blob,
                                &buffer_ids, buffer_sizes);
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

  LOG(INFO) << "Renaming Blob " << old_name << " to " << new_name << '\n';
  hermes::RenameBlob(&hermes_->context_, &hermes_->rpc_, old_name, new_name);

  return ret;
}

bool Bucket::ContainsBlob(const std::string &name) {
  bool result = hermes::ContainsBlob(&hermes_->context_, &hermes_->rpc_, id_,
                                     name);

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

  LOG(INFO) << "Renaming a bucket to" << new_name << '\n';
  RenameBucket(&hermes_->context_, &hermes_->rpc_, id_, name_, new_name);

  return ret;
}

Status Bucket::Close(Context &ctx) {
  (void)ctx;
  Status ret = 0;

  if (IsValid()) {
    LOG(INFO) << "Closing bucket '" << name_ << "'" << std::endl;
    DecrementRefcount(&hermes_->context_, &hermes_->rpc_, id_);
  }

  return ret;
}

Status Bucket::Destroy(Context &ctx) {
  (void)ctx;
  Status ret = 0;

  if (IsValid()) {
    LOG(INFO) << "Destroying bucket '" << name_ << "'" << std::endl;
    DestroyBucket(&hermes_->context_, &hermes_->rpc_, name_.c_str(), id_);
    id_.as_int = 0;
  }

  return ret;
}

} // api namespace
} // hermes namespace
