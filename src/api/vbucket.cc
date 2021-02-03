/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Distributed under BSD 3-Clause license.                                   *
* Copyright by The HDF Group.                                               *
* Copyright by the Illinois Institute of Technology.                        *
* All rights reserved.                                                      *
*                                                                           *
* This file is part of Hermes. The full Hermes copyright notice, including  *
* terms governing use, modification, and redistribution, is contained in    *
* the COPYFILE, which can be found at the top directory. If you do not have *
* access to either file, you may request a copy from help@hdfgroup.org.     *
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
  Status ret = 0;

  LOG(INFO) << "Linking blob " << blob_name << " in bucket " << bucket_name
            << " to VBucket " << name_ << '\n';

  bool blob_exists = hermes_->BucketContainsBlob(bucket_name, blob_name);
  if (blob_exists) {
    // inserting value by insert function
    linked_blobs_.push_back(make_pair(bucket_name, blob_name));
    TraitInput input;
    input.bucket_name = bucket_name;
    input.blob_name = blob_name;
    for (const auto& t : attached_traits_) {
      if (t->onLinkFn != nullptr) {
        t->onLinkFn(input, t);
        // TODO(hari): @errorhandling Check if linking was successful
      }
    }
  } else {
    // TODO(hari): @errorhandling
  }
  return ret;
}

Status VBucket::Unlink(std::string blob_name, std::string bucket_name,
                       Context& ctx) {
  (void)ctx;
  Status ret = 0;

  LOG(INFO) << "Unlinking blob " << blob_name << " in bucket " << bucket_name
            << " from VBucket " << name_ << '\n';
  bool found = false;
  for (auto ci = linked_blobs_.begin(); ci != linked_blobs_.end(); ++ci) {
    if (ci->first == bucket_name && ci->second == blob_name) {
      TraitInput input;
      input.bucket_name = bucket_name;
      input.blob_name = blob_name;
      for (const auto& t : attached_traits_) {
        if (t->onUnlinkFn != nullptr) {
          t->onUnlinkFn(input, t);
          // TODO(hari): @errorhandling Check if unlinking was successful
        }
      }
      linked_blobs_.erase(ci);
      found = true;
      break;
    }
  }
  if (!found) {
    // TODO(hari): @errorhandling
  }
  return ret;
}

Status VBucket::Contain_blob(std::string blob_name, std::string bucket_name) {
  Status ret = 0;
  std::string bk_tmp, blob_tmp;

  LOG(INFO) << "Checking if blob " << blob_name << " from bucket "
            << bucket_name << " is in this VBucket " << name_ << '\n';

  for (auto ci = linked_blobs_.begin(); ci != linked_blobs_.end(); ++ci) {
    bk_tmp = ci->first;
    blob_tmp = ci->second;
    if (bk_tmp == bucket_name && blob_tmp == blob_name) ret = 1;
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
  Status ret = 0;

  LOG(INFO) << "Attaching trait to VBucket " << name_ << '\n';
  Trait* selected_trait = NULL;
  for (const auto& t : attached_traits_) {
    if (t->id == trait->id) {
      selected_trait = t;
      break;
    }
  }
  if (!selected_trait) {
    for (auto ci = linked_blobs_.begin(); ci != linked_blobs_.end(); ++ci) {
      Trait* t = static_cast<Trait*>(trait);
      TraitInput input;
      input.bucket_name = ci->first;
      input.blob_name = ci->second;
      if (t->onAttachFn != nullptr) {
        t->onAttachFn(input, trait);
        // TODO(hari): @errorhandling Check if attach was successful
      }
    }
    attached_traits_.push_back(trait);
  } else {
    // TODO(hari): @errorhandling throw trait already exists.
  }
  return ret;
}

Status VBucket::Detach(Trait* trait, Context& ctx) {
  (void)ctx;
  (void)trait;
  Status ret = 0;

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
    for (auto ci = linked_blobs_.begin(); ci != linked_blobs_.end(); ++ci) {
      Trait* t = static_cast<Trait*>(trait);
      TraitInput input;
      input.bucket_name = ci->first;
      input.blob_name = ci->second;
      if (t->onDetachFn != nullptr) {
        t->onDetachFn(input, trait);
        // TODO(hari): @errorhandling Check if detach was successful
      }
    }
    attached_traits_.erase(selected_trait_iter);
  } else {
    // TODO(hari): @errorhandling throw trait not valid.
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

  LOG(INFO) << "Deleting VBucket " << name_ << '\n';

  for (const auto& t : attached_traits_) {
    FILE* file;
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
    for (auto ci = linked_blobs_.begin(); ci != linked_blobs_.end(); ++ci) {
      if (attached_traits_.size() > 0) {
        TraitInput input;
        input.bucket_name = ci->first;
        input.blob_name = ci->second;

        if (this->persist) {
          if (t->type == TraitType::FILE_MAPPING) {
            FileMappingTrait* fileBackedTrait = (FileMappingTrait*)t;
            // if callback defined by user
            if (fileBackedTrait->flush_cb) {
              fileBackedTrait->flush_cb(input, fileBackedTrait);
            } else {
              if (!fileBackedTrait->offset_map.empty()) {
                auto iter = fileBackedTrait->offset_map.find(ci->second);
                if (iter != fileBackedTrait->offset_map.end()) {
                  auto blob_id = GetBlobIdByName(
                      &hermes_->context_, &hermes_->rpc_, ci->second.c_str());
                  StdIoPersistBlob(&hermes_->context_, &hermes_->rpc_,
                                   &hermes_->trans_arena_, blob_id, file,
                                   iter->second);
                } else {
                  // TODO(hari): @errorhandling map doesnt have the blob linked.
                }

              } else {
                // TODO(hari): @errorhandling offset_map should not be empty
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
        }
      }
    }
  }
  linked_blobs_.clear();
  attached_traits_.clear();
  return Status();
}

}  // namespace api
}  // namespace hermes
