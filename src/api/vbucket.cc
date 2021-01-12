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

  // FIXME(hari): to use both bucket name and blob name
  bool blob_exists = hermes_->BucketContainsBlob(bucket_name, blob_name);
  if (blob_exists) {
    // inserting value by insert function
    linked_blobs_.push_back(make_pair(bucket_name, blob_name));
    Blob& blob = GetBlob(blob_name, bucket_name);
    TraitInput input;
    input.bucket_name = bucket_name;
    input.blob_name = blob_name;
    input.blob = std::move(blob);
    for (const auto& t : attached_traits_) {
      if (t->onLinkFn != nullptr) {
        t->onLinkFn(input, t);
        // TODO(hari): @errorhandling Check if linking was successful
      }
    }
  } else {
    // TODO(hari): @errorhandling
  };
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
      Blob& blob = GetBlob(blob_name, bucket_name);
      TraitInput input;
      input.bucket_name = bucket_name;
      input.blob_name = blob_name;
      input.blob = std::move(blob);
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
      Blob& blob = GetBlob(ci->second, ci->first);
      TraitInput input;
      input.bucket_name = ci->first;
      input.blob_name = ci->second;
      input.blob = std::move(blob);
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
    selected_trait = reinterpret_cast<Trait*>(&*trait_iter);;
    if (selected_trait->id == trait->id) {
      selected_trait_iter = trait_iter;
      break;
    }else{
      trait_iter++;
    }
  }
  if (selected_trait) {
    for (auto ci = linked_blobs_.begin(); ci != linked_blobs_.end(); ++ci) {
      Trait* t = static_cast<Trait*>(trait);
      Blob& blob = GetBlob(ci->second, ci->first);
      TraitInput input;
      input.bucket_name = ci->first;
      input.blob_name = ci->second;
      input.blob = std::move(blob);
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
  for (const auto& t : this->attached_traits_) {
    attached_traits.push_back(t->id);
  }
  return attached_traits;
}

Status VBucket::Delete(Context& ctx) {
  (void)ctx;

  LOG(INFO) << "Deleting VBucket " << name_ << '\n';
  for (auto ci = linked_blobs_.begin(); ci != linked_blobs_.end(); ++ci) {
    if (attached_traits_.size() > 0) {
      Blob& blob = GetBlob(ci->second, ci->first);
      TraitInput input;
      input.bucket_name = ci->first;
      input.blob_name = ci->second;
      input.blob = std::move(blob);
      for (const auto& t : attached_traits_) {
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
  }
  linked_blobs_.clear();
  attached_traits_.clear();
  return Status();
}

}  // namespace api
}  // namespace hermes
