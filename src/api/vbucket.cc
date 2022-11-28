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

#include "vbucket.h"

#include <metadata_management_internal.h>

#include <utility>
#include <vector>

#include "bucket.h"

namespace hermes {

namespace api {

VBucket::VBucket(std::string initial_name, std::shared_ptr<Hermes> const &h,
                 Context ctx)
  : name_(initial_name),
    id_({{0, 0}}),
    attached_traits_(),
    hermes_(h),
    ctx_(ctx) {
  if (IsVBucketNameTooLong(name_)) {
    id_.as_int = 0;
    throw std::length_error("VBucket name exceeds maximum size of " +
                            std::to_string(kMaxVBucketNameSize));
  } else {
    id_ = GetOrCreateVBucketId(&hermes_->context_, &hermes_->rpc_, name_);
    if (!IsValid()) {
      throw std::runtime_error("Could not open or create VBucket");
    }
  }
}

VBucket::~VBucket() {
  if (IsValid()) {
    Release();
  }
}

bool VBucket::IsValid() const {
  return !IsNullVBucketId(id_);
}

std::string VBucket::GetName() const {
  return this->name_;
}

void VBucket::WaitForBackgroundFlush() {
  AwaitAsyncFlushingTasks(&hermes_->context_, &hermes_->rpc_, id_);
}

Status VBucket::Link(std::string blob_name, std::string bucket_name) {
  Status result = Link(blob_name, bucket_name, ctx_);

  return result;
}

Status VBucket::Link(std::string blob_name, std::string bucket_name,
                     Context& ctx) {
  (void)ctx;
  Status ret;

  LOG(INFO) << "Linking blob " << blob_name << " in bucket " << bucket_name
            << " to VBucket " << name_ << '\n';

  bool blob_exists = hermes_->BucketContainsBlob(bucket_name, blob_name);

  if (blob_exists) {
    TraitInput input;
    input.bucket_name = bucket_name;
    input.blob_name = blob_name;
    for (const auto& t : attached_traits_) {
      if (t->onLinkFn != nullptr) {
        t->onLinkFn(hermes_, input, t);
        // TODO(hari): @errorhandling Check if linking was successful
      }
    }

    bool already_linked = ContainsBlob(blob_name, bucket_name);
    if (!already_linked) {
      AttachBlobToVBucket(&hermes_->context_, &hermes_->rpc_, blob_name.data(),
                          bucket_name.data(), id_);
    }
  } else {
    ret = BLOB_NOT_IN_BUCKET;
    LOG(ERROR) << ret.Msg();
  }

  return ret;
}

Status VBucket::Unlink(std::string blob_name, std::string bucket_name) {
  Status result = Unlink(blob_name, bucket_name, ctx_);

  return result;
}

Status VBucket::Unlink(std::string blob_name, std::string bucket_name,
                       Context& ctx) {
  (void)ctx;
  Status ret;

  LOG(INFO) << "Unlinking blob " << blob_name << " in bucket " << bucket_name
            << " from VBucket " << name_ << '\n';
  bool found = false;
  auto blob_ids =
      GetBlobsFromVBucketInfo(&hermes_->context_, &hermes_->rpc_, id_);
  auto bucket_id =
      GetBucketId(&hermes_->context_, &hermes_->rpc_, bucket_name.c_str());
  auto selected_blob_id = GetBlobId(&hermes_->context_, &hermes_->rpc_,
                                    blob_name.c_str(), bucket_id);
  for (hermes::BlobID& blob_id : blob_ids) {
    if (selected_blob_id.as_int == blob_id.as_int) {
      TraitInput input;
      input.bucket_name = bucket_name;
      input.blob_name = blob_name;
      for (const auto& t : attached_traits_) {
        if (t->onUnlinkFn != nullptr) {
          t->onUnlinkFn(hermes_, input, t);
          // TODO(hari): @errorhandling Check if unlinking was successful
        }
      }
      RemoveBlobFromVBucketInfo(&hermes_->context_, &hermes_->rpc_, id_,
                                blob_name.c_str(), bucket_name.c_str());
      found = true;
      break;
    }
  }
  if (!found) {
    ret = BLOB_NOT_LINKED_TO_VBUCKET;
    LOG(ERROR) << ret.Msg();
  }

  return ret;
}

bool VBucket::ContainsBlob(std::string blob_name, std::string bucket_name) {
  bool ret = false;

  LOG(INFO) << "Checking if blob " << blob_name << " from bucket "
            << bucket_name << " is in this VBucket " << name_ << '\n';

  auto blob_ids =
      GetBlobsFromVBucketInfo(&hermes_->context_, &hermes_->rpc_, id_);
  auto bucket_id =
      GetBucketId(&hermes_->context_, &hermes_->rpc_, bucket_name.c_str());
  auto selected_blob_id = GetBlobId(&hermes_->context_, &hermes_->rpc_,
                                    blob_name.c_str(), bucket_id);
  for (const auto& blob_id : blob_ids) {
    if (selected_blob_id.as_int == blob_id.as_int) {
      ret = true;
    }
  }

  return ret;
}

size_t VBucket::Get(const std::string &name, Bucket &bkt, Blob &user_blob,
                   const Context &ctx) {
  size_t ret = Get(name, bkt, user_blob.data(), user_blob.size(), ctx);

  return ret;
}

size_t VBucket::Get(const std::string &name, Bucket &bkt, Blob &user_blob) {
  size_t result = Get(name, bkt, user_blob, ctx_);

  return result;
}

size_t VBucket::Get(const std::string &name, Bucket &bkt, void *user_blob,
                    size_t blob_size, const Context &ctx) {
  bool is_size_query = false;
  if (blob_size != 0) {
    is_size_query = true;
  }

  size_t result = bkt.Get(name, user_blob, blob_size, ctx);

  if (!is_size_query) {
    TraitInput input;
    input.blob_name = name;
    input.bucket_name = bkt.GetName();
    for (const auto& t : attached_traits_) {
      if (t->onGetFn != nullptr) {
        t->onGetFn(hermes_, input, t);
      }
    }
  }

  return result;
}

std::vector<std::string> VBucket::GetLinks() {
  Context ctx;
  return GetLinks(ctx);
}

std::vector<std::string> VBucket::GetLinks(Context& ctx) {
  (void)ctx;
  LOG(INFO) << "Getting subset of links satisfying pred in VBucket " << name_
            << '\n';
  auto blob_ids =
      GetBlobsFromVBucketInfo(&hermes_->context_, &hermes_->rpc_, id_);
  auto blob_names = std::vector<std::string>();
  for (const auto& blob_id : blob_ids) {
    blob_names.push_back(
        GetBlobNameFromId(&hermes_->context_, &hermes_->rpc_, blob_id));
  }
  // TODO(hari): add filtering
  return blob_names;
}

Status VBucket::Attach(Trait* trait) {
  Status result = Attach(trait, ctx_);

  return result;
}

Status VBucket::Attach(Trait* trait, Context& ctx) {
  (void)ctx;
  Status ret;

  LOG(INFO) << "Attaching trait to VBucket " << name_ << '\n';
  Trait* selected_trait = NULL;
  for (const auto& t : attached_traits_) {
    if (t->id == trait->id) {
      selected_trait = t;
      break;
    }
  }
  if (!selected_trait) {
    Trait* t = static_cast<Trait*>(trait);
    if (t->onAttachFn != nullptr) {
      t->onAttachFn(hermes_, id_, trait);
      // TODO(hari): @errorhandling Check if attach was successful
    }

    attached_traits_.push_back(trait);
  } else {
    ret = TRAIT_EXISTS_ALREADY;
    LOG(ERROR) << ret.Msg();
  }

  return ret;
}

Status VBucket::Detach(Trait* trait) {
  Status result = Detach(trait, ctx_);

  return result;
}

Status VBucket::Detach(Trait* trait, Context& ctx) {
  (void)ctx;
  (void)trait;
  Status ret;

  LOG(INFO) << "Detaching trait from VBucket " << name_ << '\n';

  auto trait_iter = attached_traits_.begin();
  Trait* selected_trait = NULL;
  auto selected_trait_iter = attached_traits_.begin();
  while (trait_iter != attached_traits_.end()) {
    selected_trait = reinterpret_cast<Trait*>(&*trait_iter);
    if (selected_trait->id == trait->id) {
      selected_trait_iter = trait_iter;
      break;
    } else {
      trait_iter++;
    }
  }
  if (selected_trait) {
    Trait* t = static_cast<Trait*>(trait);
    if (t->onDetachFn != nullptr) {
      t->onDetachFn(hermes_, id_, trait);
      // TODO(hari): @errorhandling Check if detach was successful
    }

    attached_traits_.erase(selected_trait_iter);
  } else {
    ret = TRAIT_NOT_VALID;
    LOG(ERROR) << ret.Msg();
  }

  return ret;
}

Trait *VBucket::GetTrait(TraitType type) {
  Trait *result = 0;
  for (Trait *trait : attached_traits_) {
    if (trait && trait->type == type) {
      result = trait;
      break;
    }
  }

  return result;
}

template <class Predicate>
std::vector<TraitID> VBucket::GetTraits(Predicate pred, Context& ctx) {
  (void)ctx;

  LOG(INFO) << "Getting the subset of attached traits satisfying pred in "
            << "VBucket " << name_ << '\n';
  auto attached_traits = std::vector<TraitID>();
  // TODO(hari): add filtering based on predicate.
  for (const auto& t : this->attached_traits_) {
    attached_traits.push_back(t->id);
  }
  return attached_traits;
}

Status VBucket::Release() {
  Status result = Release(ctx_);

  return result;
}

Status VBucket::Release(Context& ctx) {
  (void)ctx;
  Status ret;

  if (IsValid() && hermes_->is_initialized) {
    LOG(INFO) << "Closing vbucket '" << name_ << "'" << std::endl;
    DecrementRefcount(&hermes_->context_, &hermes_->rpc_, id_);
    id_.as_int = 0;
  }
  return ret;
}

Status VBucket::Destroy() {
  Status result = Destroy(ctx_);

  return result;
}

Status VBucket::Destroy(Context& ctx) {
  (void)ctx;
  Status result;

  if (IsValid()) {
    // NOTE(chogan): Let all flushing tasks complete before destroying the
    // VBucket.
    WaitForBackgroundFlush();

    SharedMemoryContext *context = &hermes_->context_;
    RpcContext *rpc = &hermes_->rpc_;

    LOG(INFO) << "Destroying VBucket " << name_ << '\n';

    auto blob_ids = GetBlobsFromVBucketInfo(context, rpc, id_);

    // NOTE(chogan): Call each Trait's unlink callback on each Blob
    for (const auto& t : attached_traits_) {
      for (const auto& blob_id : blob_ids) {
        TraitInput input = {};
        BucketID bucket_id = GetBucketIdFromBlobId(context, rpc, blob_id);
        if (!IsNullBucketId(bucket_id)) {
          input.bucket_name = GetBucketNameById(context, rpc, bucket_id);
          input.blob_name = GetBlobNameFromId(context, rpc, blob_id);
          if (t->onUnlinkFn != nullptr) {
            t->onUnlinkFn(hermes_, input, t);
            // TODO(hari): @errorhandling Check if unlinking was successful
          }
        }
      }
    }

    // NOTE(chogan): Call each trait's onDetach callback
    for (const auto& t : attached_traits_) {
      if (t->onDetachFn != nullptr) {
        TraitInput input = {};
        t->onDetachFn(hermes_, id_, t);
        // TODO(hari): @errorhandling Check if detach was successful
      }
    }

    attached_traits_.clear();
    bool destroyed = DestroyVBucket(context, rpc, name_.c_str(), id_);
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
