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

Status VBucket::Link(std::string blob_name, std::string bucket_name,
                     Context& ctx) {
  (void)ctx;
  Status ret;

  LOG(INFO) << "Linking blob " << blob_name << " in bucket " << bucket_name
            << " to VBucket " << name_ << '\n';

  bool blob_exists = hermes_->BucketContainsBlob(bucket_name, blob_name);
  if (blob_exists) {
    // inserting value by insert function
    // linked_blobs_.push_back(make_pair(bucket_name, blob_name));
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
                        id_);
  } else {
    // TODO(hari): @errorhandling
    ret = BLOB_NOT_IN_BUCKET;
    LOG(ERROR) << ret.Msg();
  }

  return ret;
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
  auto selected_blob_id =
      GetBlobIdByName(&hermes_->context_, &hermes_->rpc_, blob_name.c_str());
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
      // linked_blobs_.erase(ci);
      RemoveBlobFromVBucketInfo(&hermes_->context_, &hermes_->rpc_, id_,
                                blob_name.data());
      found = true;
      break;
    }
  }
  if (!found) {
    // TODO(hari): @errorhandling
    ret = BLOB_NOT_LINKED_TO_VBUCKET;
    LOG(ERROR) << ret.Msg();
  }

  return ret;
}

bool VBucket::Contain_blob(std::string blob_name, std::string bucket_name) {
  bool ret = false;
  std::string bk_tmp, blob_tmp;

  LOG(INFO) << "Checking if blob " << blob_name << " from bucket "
            << bucket_name << " is in this VBucket " << name_ << '\n';
  auto blob_ids =
      GetBlobsFromVBucketInfo(&hermes_->context_, &hermes_->rpc_, id_);
  auto selected_blob_id =
      GetBlobIdByName(&hermes_->context_, &hermes_->rpc_, blob_name.c_str());
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
  bkt.Close(ctx);
  return local_blob;
}

template <class Predicate>
std::vector<std::string> VBucket::GetLinks(Predicate pred, Context& ctx) {
  LOG(INFO) << "Getting subset of links satisfying pred in VBucket " << name_
            << '\n';

  return std::vector<std::string>();
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
      // FIXME(hari): this needs to be read from blob_id;
      // input.bucket_name = ci->first;
      input.blob_name =
          GetBlobNameById(&hermes_->context_, &hermes_->rpc_, blob_id);
      if (t->onAttachFn != nullptr) {
        t->onAttachFn(input, trait);
        // TODO(hari): @errorhandling Check if attach was successful
      }
    }
    attached_traits_.push_back(trait);
  } else {
    // TODO(hari): @errorhandling throw trait already exists.
    ret = TRAIT_EXISTS_ALREADY;
    LOG(ERROR) << ret.Msg();
  }

  return ret;
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
      // FIXME(hari): this needs to be read from blob_id;
      // input.bucket_name = ci->first;
      input.blob_name =
          GetBlobNameById(&hermes_->context_, &hermes_->rpc_, blob_id);
      if (t->onDetachFn != nullptr) {
        t->onDetachFn(input, trait);
        // TODO(hari): @errorhandling Check if detach was successful
      }
    }
    attached_traits_.erase(selected_trait_iter);
  } else {
    // TODO(hari): @errorhandling throw trait not valid.
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

Status VBucket::Delete(Context& ctx) {
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
      if (attached_traits_.size() > 0) {
        TraitInput input;
        // FIXME(hari): this needs to be read from blob_id;
        // input.bucket_name = ci->first;
        input.blob_name =
            GetBlobNameById(&hermes_->context_, &hermes_->rpc_, blob_id);

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
                  StdIoPersistBlob(&hermes_->context_, &hermes_->rpc_,
                                   &hermes_->trans_arena_, blob_id, file,
                                   iter->second);
                } else {
                  // TODO(hari): @errorhandling map doesnt have the blob linked.
                  ret = BLOB_NOT_LINKED_IN_MAP;
                  LOG(ERROR) << ret.Msg();
                }

              } else {
                // TODO(hari): @errorhandling offset_map should not be empty
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
    }
    if (persist) {
      if (file != nullptr) {
        fflush(file);
        if (fclose(file) != 0) {
          // TODO(chogan): @errorhandling
          ret = FCLOSE_FAILED;
          LOG(ERROR) << ret.Msg() << strerror(errno);
        }
      }
    }
  }
  // linked_blobs_.clear();
  attached_traits_.clear();
  return Status();
}

}  // namespace api
}  // namespace hermes