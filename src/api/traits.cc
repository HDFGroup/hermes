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

#include "traits.h"

#include <functional>

#include "buffer_organizer.h"

namespace hermes {
namespace api {
Trait::Trait(TraitID id, TraitIdArray conflict_traits, TraitType type)
    : id(id),
      conflict_traits(conflict_traits),
      type(type),
      onAttachFn(nullptr),
      onDetachFn(nullptr),
      onLinkFn(nullptr),
      onUnlinkFn(nullptr) {}

using OffsetMap = std::unordered_map<std::string, u64>;

PersistTrait::PersistTrait(const std::string &filename,
                           const OffsetMap &offset_map,
                           bool synchronous)
  : Trait(HERMES_PERSIST_TRAIT, TraitIdArray(), TraitType::PERSIST),
    filename(filename), offset_map(offset_map), synchronous(synchronous) {
  this->onAttachFn = std::bind(&PersistTrait::onAttach, this,
                               std::placeholders::_1, std::placeholders::_2,
                               std::placeholders::_3);
  this->onDetachFn = std::bind(&PersistTrait::onDetach, this,
                               std::placeholders::_1, std::placeholders::_2,
                               std::placeholders::_3);
  this->onLinkFn = std::bind(&PersistTrait::onLink, this,
                             std::placeholders::_1, std::placeholders::_2,
                             std::placeholders::_3);
  this->onUnlinkFn = std::bind(&PersistTrait::onUnlink, this,
                               std::placeholders::_1, std::placeholders::_2,
                               std::placeholders::_3);
}

void PersistTrait::onAttach(HermesPtr hermes, VBucketID id, Trait *trait) {
  SharedMemoryContext *context = &hermes->context_;
  RpcContext *rpc = &hermes->rpc_;

  auto blob_ids =
    GetBlobsFromVBucketInfo(context, rpc, id);
  for (const auto& blob_id : blob_ids) {
    TraitInput input;
    auto bucket_id = GetBucketIdFromBlobId(context, rpc, blob_id);
    input.bucket_name = GetBucketNameById(context, rpc, bucket_id);
    input.blob_name = GetBlobNameFromId(context, rpc, blob_id);
    onLink(hermes, input, trait);
  }
}

void PersistTrait::onDetach(HermesPtr hermes, VBucketID id, Trait *trait) {
  (void)hermes;
  (void)id;
  (void)trait;
}

void PersistTrait::onLink(HermesPtr hermes, TraitInput &input, Trait *trait) {
  PersistTrait *persist_trait = (PersistTrait *)trait;
  auto iter = persist_trait->offset_map.find(input.blob_name);

  if (iter != persist_trait->offset_map.end()) {
    SharedMemoryContext *context = &hermes->context_;
    RpcContext *rpc = &hermes->rpc_;
    BucketID bucket_id = GetBucketId(context, rpc, input.bucket_name.c_str());
    BlobID blob_id = GetBlobId(context, rpc, input.blob_name, bucket_id, true);
    std::string filename = persist_trait->filename;
    u64 offset = iter->second;

    if (synchronous) {
      FlushBlob(context, rpc, blob_id, filename, offset);
    } else {
      EnqueueFlushingTask(rpc, blob_id, filename, offset);
    }
  }
}

void PersistTrait::onUnlink(HermesPtr hermes, TraitInput &input, Trait *trait) {
  (void)hermes;
  (void)input;
  (void)trait;
}

}  // namespace api
}  // namespace hermes
