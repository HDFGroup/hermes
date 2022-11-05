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

#include "bucket.h"
#include "prefetcher.h"

#include <iostream>
#include <vector>

#include "utils.h"
#include "buffer_pool.h"
#include "metadata_management.h"

namespace hermes {

namespace api {

Bucket::Bucket(const std::string &initial_name,
               const std::shared_ptr<Hermes> &h, Context ctx)
  : name_(initial_name), hermes_(h), ctx_(ctx) {
  if (IsBucketNameTooLong(name_)) {
    id_.as_int = 0;
    throw std::length_error("Bucket name is too long: " +
                            std::to_string(kMaxBucketNameSize));
  } else {
    id_ = GetOrCreateBucketId(&hermes_->context_, &hermes_->rpc_, name_);
    if (!IsValid()) {
      throw std::runtime_error("Bucket id is invalid.");
    }
  }
}

Bucket::~Bucket() {
  if (IsValid()) {
    Context ctx;
    Release(ctx);
  }
}

std::string Bucket::GetName() const {
  return name_;
}

u64 Bucket::GetId() const {
  return id_.as_int;
}

bool Bucket::IsValid() const {
  return !IsNullBucketId(id_);
}

Status Bucket::Put(const std::string &name, const u8 *data, size_t size,
                   const Context &ctx) {
  Status result;

  if (size > 0 && nullptr == data) {
    result = INVALID_BLOB;
    LOG(ERROR) << result.Msg();
  }

  if (result.Succeeded()) {
    std::vector<std::string> names{name};
    // TODO(chogan): Create a PreallocatedMemory allocator for std::vector so
    // that a single-blob-Put doesn't perform a copy
    std::vector<Blob> blobs{Blob{data, data + size}};
    result = Put(names, blobs, ctx);
  }

  return result;
}

Status Bucket::Put(const std::string &name, const u8 *data, size_t size) {
  Status result = Put(name, data, size, ctx_);

  return result;
}

size_t Bucket::GetTotalBlobSize() {
  std::vector<BlobID> blob_ids = hermes::GetBlobIds(&hermes_->context_,
                                                    &hermes_->rpc_, id_);
  api::Context ctx;
  size_t result = 0;
  for (size_t i = 0; i < blob_ids.size(); ++i) {
    ScopedTemporaryMemory scratch(&hermes_->trans_arena_);
    result += hermes::GetBlobSizeById(&hermes_->context_, &hermes_->rpc_,
                                      scratch, blob_ids[i]);
  }

  return result;
}

size_t Bucket::GetBlobSize(const std::string &name, const Context &ctx) {
  ScopedTemporaryMemory scratch(&hermes_->trans_arena_);
  size_t result = GetBlobSize(scratch, name, ctx);

  return result;
}

size_t Bucket::GetBlobSize(Arena *arena, const std::string &name,
                           const Context &ctx) {
  (void)ctx;
  size_t result = 0;

  if (IsValid()) {
    LOG(INFO) << "Getting Blob " << name << " size from bucket "
              << name_ << '\n';
    BlobID blob_id = GetBlobId(&hermes_->context_, &hermes_->rpc_, name,
                               id_, false);
    if (!IsNullBlobId(blob_id)) {
      result = GetBlobSizeById(&hermes_->context_, &hermes_->rpc_, arena,
                               blob_id);
    }
  }

  return result;
}

size_t Bucket::Get(const std::string &name, Blob &user_blob,
                   const Context &ctx) {
  size_t ret = Get(name, user_blob.data(), user_blob.size(), ctx);

  return ret;
}

size_t Bucket::Get(const std::string &name, Blob &user_blob) {
  size_t result = Get(name, user_blob, ctx_);

  return result;
}

size_t Bucket::Get(const std::string &name, void *user_blob, size_t blob_size,
                   const Context &ctx) {
  (void)ctx;

  size_t ret = 0;

  if (IsValid()) {
    // TODO(chogan): Assumes scratch is big enough to hold buffer_ids
    ScopedTemporaryMemory scratch(&hermes_->trans_arena_);

    if (user_blob && blob_size != 0) {
      hermes::Blob blob = {};
      blob.data = (u8 *)user_blob;
      blob.size = blob_size;
      LOG(INFO) << "Getting Blob " << name << " from bucket " << name_ << '\n';
      BlobID blob_id = GetBlobId(&hermes_->context_, &hermes_->rpc_,
                                 name, id_);

      // Upload statistic to prefetcher
      if (ctx.pctx_.hint_ != PrefetchHint::kNone) {
        IoLogEntry entry;
        entry.vbkt_id_ = ctx.vbkt_id_;
        entry.bkt_id_ = id_;
        entry.blob_id_ = blob_id;
        entry.type_ = IoType::kGet;
        entry.off_ = 0;
        entry.size_ = blob_size;
        entry.pctx_ = ctx.pctx_;
        entry.tid_ = GlobalThreadID(hermes_->comm_.world_proc_id);
        entry.historical_ = false;
        Prefetcher::LogIoStat(hermes_.get(), entry);
      }
      LockBlob(&hermes_->context_, &hermes_->rpc_, blob_id);
      ret = ReadBlobById(&hermes_->context_, &hermes_->rpc_,
                         &hermes_->trans_arena_, blob, blob_id);
      UnlockBlob(&hermes_->context_, &hermes_->rpc_, blob_id);
    } else {
      ret = GetBlobSize(scratch, name, ctx);
    }
  }

  return ret;
}

std::vector<size_t> Bucket::Get(const std::vector<std::string> &names,
                                std::vector<Blob> &blobs, const Context &ctx) {
  std::vector<size_t> result(names.size(), 0);
  if (names.size() == blobs.size()) {
    for (size_t i = 0; i < result.size(); ++i) {
      result[i] = Get(names[i], blobs[i], ctx);
    }
  } else {
    LOG(ERROR) << "names.size() != blobs.size() in Bucket::Get ("
               << names.size() << " != " << blobs.size() << ")"
               << std::endl;
  }

  return result;
}

size_t Bucket::GetNext(u64 blob_index, Blob &user_blob,
                       const Context &ctx) {
  size_t ret = GetNext(blob_index, user_blob.data(), user_blob.size(), ctx);

  return ret;
}

size_t Bucket::GetNext(u64 blob_index, Blob &user_blob) {
  size_t result = GetNext(blob_index, user_blob, ctx_);

  return result;
}

size_t Bucket::GetNext(u64 blob_index, void *user_blob, size_t blob_size,
                       const Context &ctx) {
  (void)ctx;
  size_t ret = 0;

  if (IsValid()) {
    std::vector<BlobID> blob_ids = GetBlobIds(&hermes_->context_,
                                              &hermes_->rpc_,
                                              this->id_);
    if (blob_index > blob_ids.size()) {
      LOG(INFO) << "Already on the tail for bucket " << name_ << '\n';
      return ret;
    }
    BlobID next_blob_id = blob_ids.at(blob_index);
    if (user_blob && blob_size != 0) {
      hermes::Blob blob = {};
      blob.data = (u8 *)user_blob;
      blob.size = blob_size;
      LOG(INFO) << "Getting Blob " << next_blob_id.as_int << " from bucket "
                << name_ << '\n';
      ret = ReadBlobById(&hermes_->context_, &hermes_->rpc_,
                         &hermes_->trans_arena_, blob, next_blob_id);
    } else {
      LOG(INFO) << "Getting Blob " << next_blob_id.as_int <<
          " size from bucket " << name_ << '\n';
      ScopedTemporaryMemory scratch(&hermes_->trans_arena_);
      if (!IsNullBlobId(next_blob_id)) {
        ret = GetBlobSizeById(&hermes_->context_, &hermes_->rpc_, scratch,
                              next_blob_id);
      }
    }
  }

  return ret;
}

std::vector<size_t> Bucket::GetNext(u64 blob_index, u64 count,
                                    std::vector<Blob> &blobs,
                                    const Context &ctx) {
  std::vector<size_t> result(count, 0);

  if (IsValid()) {
    if (count == blobs.size()) {
      for (size_t i = 0; i < result.size(); ++i) {
        result[i] = GetNext(blob_index + i, blobs[i], ctx);
      }
    } else {
      LOG(ERROR) << "names.size() != blobs.size() in Bucket::Get (" << count
                 << " != " << blobs.size() << ")" << std::endl;
    }
  }

  return result;
}

template<class Predicate>
Status Bucket::GetV(void *user_blob, Predicate pred, Context &ctx) {
  (void)user_blob;
  (void)ctx;
  Status ret;

  LOG(INFO) << "Getting blobs by predicate from bucket " << name_ << '\n';

  HERMES_NOT_IMPLEMENTED_YET;

  return ret;
}

Status Bucket::DeleteBlob(const std::string &name) {
  Status result = DeleteBlob(name, ctx_);

  return result;
}

Status Bucket::DeleteBlob(const std::string &name, const Context &ctx) {
  (void)ctx;
  Status ret;

  LOG(INFO) << "Deleting Blob " << name << " from bucket " << name_ << '\n';
  DestroyBlobByName(&hermes_->context_, &hermes_->rpc_, id_, name);

  return ret;
}

Status Bucket::RenameBlob(const std::string &old_name,
                          const std::string &new_name) {
  Status result = RenameBlob(old_name, new_name, ctx_);

  return result;
}

Status Bucket::RenameBlob(const std::string &old_name,
                          const std::string &new_name,
                          const Context &ctx) {
  (void)ctx;
  Status ret;

  if (IsBlobNameTooLong(new_name)) {
    ret = BLOB_NAME_TOO_LONG;
    LOG(ERROR) << ret.Msg();
    return ret;
  } else {
    LOG(INFO) << "Renaming Blob " << old_name << " to " << new_name << '\n';
    hermes::RenameBlob(&hermes_->context_, &hermes_->rpc_, old_name,
                       new_name, id_);
  }

  return ret;
}

bool Bucket::ContainsBlob(const std::string &name) {
  bool result = hermes::ContainsBlob(&hermes_->context_, &hermes_->rpc_, id_,
                                     name);

  return result;
}

bool Bucket::BlobIsInSwap(const std::string &name) {
  BlobID blob_id = GetBlobId(&hermes_->context_, &hermes_->rpc_, name,
                                   id_);
  bool result = hermes::BlobIsInSwap(blob_id);

  return result;
}

template<class Predicate>
std::vector<std::string> Bucket::GetBlobNames(Predicate pred,
                                              Context &ctx) {
  (void)ctx;

  LOG(INFO) << "Getting blob names by predicate from bucket " << name_ << '\n';

  HERMES_NOT_IMPLEMENTED_YET;

  return std::vector<std::string>();
}

Status Bucket::Rename(const std::string &new_name) {
  Status result = Rename(new_name, ctx_);

  return result;
}

Status Bucket::Rename(const std::string &new_name, const Context &ctx) {
  (void)ctx;
  Status ret;

  if (IsBucketNameTooLong(new_name)) {
    ret = BUCKET_NAME_TOO_LONG;
    LOG(ERROR) << ret.Msg();
    return ret;
  } else {
    LOG(INFO) << "Renaming a bucket to" << new_name << '\n';
    RenameBucket(&hermes_->context_, &hermes_->rpc_, id_, name_, new_name);
  }

  return ret;
}

Status Bucket::Persist(const std::string &file_name) {
  Status result = Persist(file_name, ctx_);

  return result;
}

Status Bucket::Persist(const std::string &file_name, const Context &ctx) {
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

void Bucket::OrganizeBlob(const std::string &blob_name, f32 epsilon,
                          f32 custom_importance) {
  hermes::OrganizeBlob(&hermes_->context_, &hermes_->rpc_, id_, blob_name,
                       epsilon, custom_importance);
}

Status Bucket::Release() {
  Status result = Release(ctx_);

  return result;
}

Status Bucket::Release(const Context &ctx) {
  (void)ctx;
  Status ret;

  if (IsValid() && hermes_->is_initialized) {
    LOG(INFO) << "Closing bucket '" << name_ << "'" << std::endl;
    DecrementRefcount(&hermes_->context_, &hermes_->rpc_, id_);
    id_.as_int = 0;
  }

  return ret;
}

Status Bucket::Destroy() {
  Status result = Destroy(ctx_);

  return result;
}

Status Bucket::Destroy(const Context &ctx) {
  (void)ctx;
  Status result;

  if (IsValid()) {
    LOG(INFO) << "Destroying bucket '" << name_ << "'" << std::endl;
    bool destroyed = DestroyBucket(&hermes_->context_, &hermes_->rpc_,
                                   name_.c_str(), id_);
    if (destroyed) {
      id_.as_int = 0;
    } else {
      result = BUCKET_IN_USE;
      LOG(ERROR) << result.Msg();
    }
  }

  return result;
}

}  // namespace api
}  // namespace hermes
