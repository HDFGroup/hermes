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

bool VBucket::IsValid() const { return !IsNullVBucketId(id_); }

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
    // inserting value by insert function
    TraitInput input;
    input.bucket_name = bucket_name;
    input.blob_name = blob_name;
    for (const auto& t : attached_traits_) {
      if (t->onLinkFn != nullptr) {
        t->onLinkFn(input, t);
        // TODO(hari): @errorhandling Check if linking was successful
      }
    }
    AttachBlobToVBucket(&hermes_->context_, &hermes_->rpc_, blob_name.data(),
                        bucket_name.data(), id_);
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
          t->onUnlinkFn(input, t);
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
  std::string bk_tmp, blob_tmp;

  LOG(INFO) << "Checking if blob " << blob_name << " from bucket "
            << bucket_name << " is in this VBucket " << name_ << '\n';

  auto blob_ids =
      GetBlobsFromVBucketInfo(&hermes_->context_, &hermes_->rpc_, id_);
  auto bucket_id =
      GetBucketId(&hermes_->context_, &hermes_->rpc_, bucket_name.c_str());
  auto selected_blob_id = GetBlobId(&hermes_->context_, &hermes_->rpc_,
                                    blob_name.c_str(), bucket_id);
  for (const auto& blob_id : blob_ids) {
    if (selected_blob_id.as_int == blob_id.as_int) ret = true;
  }

  return ret;
}

Blob& VBucket::GetBlob(std::string blob_name, std::string bucket_name) {
  LOG(INFO) << "Retrieving blob " << blob_name << " from bucket " << bucket_name
            << " in VBucket " << name_ << '\n';
  hermes::api::Context ctx;
  Bucket bkt(bucket_name, hermes_, ctx);
  local_blob = {};
  size_t blob_size = bkt.Get(blob_name, local_blob, ctx);
  local_blob.resize(blob_size);
  bkt.Get(blob_name, local_blob, ctx);
  return local_blob;
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
  (void)trait;
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
    auto blob_ids =
        GetBlobsFromVBucketInfo(&hermes_->context_, &hermes_->rpc_, id_);
    for (const auto& blob_id : blob_ids) {
      Trait* t = static_cast<Trait*>(trait);
      TraitInput input;
      auto bucket_id =
          GetBucketIdFromBlobId(&hermes_->context_, &hermes_->rpc_, blob_id);
      input.bucket_name =
          GetBucketNameById(&hermes_->context_, &hermes_->rpc_, bucket_id);
      input.blob_name =
          GetBlobNameFromId(&hermes_->context_, &hermes_->rpc_, blob_id);
      if (t->onAttachFn != nullptr) {
        t->onAttachFn(input, trait);
        // TODO(hari): @errorhandling Check if attach was successful
      }
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
    auto blob_ids =
        GetBlobsFromVBucketInfo(&hermes_->context_, &hermes_->rpc_, id_);
    for (const auto& blob_id : blob_ids) {
      Trait* t = static_cast<Trait*>(trait);
      TraitInput input;
      auto bucket_id =
          GetBucketIdFromBlobId(&hermes_->context_, &hermes_->rpc_, blob_id);
      input.bucket_name =
          GetBucketNameById(&hermes_->context_, &hermes_->rpc_, bucket_id);
      input.blob_name =
          GetBlobNameFromId(&hermes_->context_, &hermes_->rpc_, blob_id);
      if (t->onDetachFn != nullptr) {
        t->onDetachFn(input, trait);
        // TODO(hari): @errorhandling Check if detach was successful
      }
    }
    attached_traits_.erase(selected_trait_iter);
  } else {
    ret = TRAIT_NOT_VALID;
    LOG(ERROR) << ret.Msg();
  }

  return ret;
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
  Status ret;

  LOG(INFO) << "Deleting VBucket " << name_ << '\n';

  for (const auto& t : attached_traits_) {
    FILE* file = nullptr;
    if (this->persist) {
      if (t->type == TraitType::FILE_MAPPING) {
        FileMappingTrait* fileBackedTrait = (FileMappingTrait*)t;
        if (fileBackedTrait->fh != nullptr) {
          file = fileBackedTrait->fh;
        } else {
          std::string open_mode;
          if (access(fileBackedTrait->filename.c_str(), F_OK) == 0) {
            open_mode = "r+";
          } else {
            open_mode = "w+";
          }
          file = fopen(fileBackedTrait->filename.c_str(), open_mode.c_str());
        }
      }
    }
    auto blob_ids =
        GetBlobsFromVBucketInfo(&hermes_->context_, &hermes_->rpc_, id_);
    for (const auto& blob_id : blob_ids) {
      TraitInput input;
      auto bucket_id =
          GetBucketIdFromBlobId(&hermes_->context_, &hermes_->rpc_, blob_id);
      input.bucket_name =
          GetBucketNameById(&hermes_->context_, &hermes_->rpc_, bucket_id);
      input.blob_name =
          GetBlobNameFromId(&hermes_->context_, &hermes_->rpc_, blob_id);
      if (attached_traits_.size() > 0) {
        if (this->persist) {
          if (t->type == TraitType::FILE_MAPPING) {
            FileMappingTrait* fileBackedTrait = (FileMappingTrait*)t;
            // if callback defined by user
            if (fileBackedTrait->flush_cb) {
              fileBackedTrait->flush_cb(input, fileBackedTrait);
            } else {
              if (!fileBackedTrait->offset_map.empty()) {
                auto iter = fileBackedTrait->offset_map.find(input.blob_name);
                if (iter != fileBackedTrait->offset_map.end()) {
                  // TODO(hari): @errorhandling check return of StdIoPersistBlob
                  ret = StdIoPersistBlob(&hermes_->context_, &hermes_->rpc_,
                                         &hermes_->trans_arena_, blob_id, file,
                                         iter->second);
                  if (!ret.Succeeded()) LOG(ERROR) << ret.Msg();
                } else {
                  ret = BLOB_NOT_LINKED_IN_MAP;
                  LOG(ERROR) << ret.Msg();
                }

              } else {
                ret = OFFSET_MAP_EMPTY;
                LOG(ERROR) << ret.Msg();
              }
            }
          }
        }
        if (t->onDetachFn != nullptr) {
          t->onDetachFn(input, t);
          // TODO(hari): @errorhandling Check if detach was successful
        }
        if (t->onUnlinkFn != nullptr) {
          t->onUnlinkFn(input, t);
          // TODO(hari): @errorhandling Check if unlinking was successful
        }
      }
      RemoveBlobFromVBucketInfo(&hermes_->context_, &hermes_->rpc_, id_,
                                input.blob_name.c_str(),
                                input.bucket_name.c_str());
    }
    if (persist) {
      if (file != nullptr) {
        fflush(file);
        if (fclose(file) != 0) {
          ret = FCLOSE_FAILED;
          LOG(ERROR) << ret.Msg() << strerror(errno);
        }
      }
    }
  }
  attached_traits_.clear();
  DestroyVBucket(&hermes_->context_, &hermes_->rpc_, this->name_.c_str(), id_);
  return Status();
}

}  // namespace api
}  // namespace hermes
