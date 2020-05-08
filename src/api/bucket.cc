#include "bucket.h"
#include "buffer_pool.h"
#include "metadata_management.h"
#include "data_placement_engine.h"

#include <iostream>

namespace hermes {

namespace api {

struct bkt_hdl * Open(const std::string &name, Context &ctx) {
  (void)ctx;
  struct bkt_hdl *ret = nullptr;

  LOG(INFO) << "Opening a bucket to" << name << '\n';

  return ret;
}

Bucket::Bucket(const std::string &initial_name,
               const std::shared_ptr<Hermes> &h, Context ctx)
    : name_(initial_name), hermes_(h) {
  (void)ctx;
  BucketID id = GetBucketIdByName(&hermes_->context_, &hermes_->rpc_,
                                  initial_name.c_str());

  if (id.as_int != 0) {
    LOG(INFO) << "Opening Bucket " << initial_name << std::endl;
    id_ = id;
    IncrementRefcount(&hermes_->context_, &hermes_->rpc_, id_);
  } else {
    LOG(INFO) << "Creating Bucket " << initial_name << std::endl;
    id_ = GetNextFreeBucketId(&hermes_->context_, &hermes_->rpc_, initial_name);
  }
}

bool Bucket::IsValid() const {
  bool result = id_.as_int != 0;

  return result;
}

Status Bucket::Put(const std::string &name, const Blob &data, Context &ctx) {
  (void)ctx;
  Status ret = 0;

  if (IsValid()) {
    LOG(INFO) << "Attaching blob " << name << " to Bucket " << '\n';

    TieredSchema schema = CalculatePlacement(data.size(), ctx);
    while (schema.size() == 0) {
      // NOTE(chogan): Keep running the DPE until we get a valid placement
      schema = CalculatePlacement(data.size(), ctx);
    }

    std::vector<BufferID> buffer_ids = GetBuffers(&hermes_->context_, schema);
    while (buffer_ids.size() == 0) {
      // NOTE(chogan): This loop represents waiting for the BufferOrganizer to
      // free some buffers if it needs to. It will probably be handled through the
      // messaging service.
      buffer_ids = GetBuffers(&hermes_->context_, schema);
    }

    hermes::Blob blob = {};
    blob.data = (u8 *)data.data();
    blob.size = data.size();
    WriteBlobToBuffers(&hermes_->context_, blob, buffer_ids);

    // NOTE(chogan): Update all metadata associated with this Put
    AttachBlobToBucket(&hermes_->context_, &hermes_->rpc_, name.c_str(), id_,
                       buffer_ids);
  }

  return ret;
}

size_t Bucket::Get(const std::string &name, Blob& user_blob, Context &ctx) {
  (void)ctx;

  size_t ret = 0;

  if (IsValid()) {
    ScopedTemporaryMemory scratch(&hermes_->trans_arena_);
    BufferIdArray buffer_ids =
      GetBufferIdsFromBlobName(scratch, &hermes_->context_, &hermes_->rpc_,
                               name.c_str());

    if (user_blob.size() == 0) {
      LOG(INFO) << "Getting Blob " << name << " size from bucket "
                << name_ << '\n';
      ret = GetBlobSize(&hermes_->context_, &hermes_->comm_, &buffer_ids);
    } else {
      LOG(INFO) << "Getting Blob " << name << " from bucket " << name_ << '\n';
      hermes::Blob blob = {};
      blob.data = user_blob.data();
      blob.size = user_blob.size();

      ret = ReadBlobFromBuffers(&hermes_->context_, &hermes_->comm_,
                                &hermes_->rpc_, &blob, &buffer_ids);
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
  DestroyBlob(&hermes_->context_, &hermes_->rpc_, id_, name);

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

  LOG(INFO) << "Closing a bucket to " << name_ << '\n';

  if (IsValid()) {
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
