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

FileMappingTrait::FileMappingTrait(
    const std::string &filename,
    std::unordered_map<std::string, u64> &offset_map,
    FILE *fh,
    TraitCallback flush_cb, TraitCallback load_cb)
    : Trait(HERMES_FILE_TRAIT, TraitIdArray(), TraitType::FILE_MAPPING),
      flush_cb(flush_cb),
      load_cb(load_cb),
      filename(filename),
      offset_map(offset_map),
      fh(fh) {
  this->onAttachFn = std::bind(&FileMappingTrait::onAttach, this,
                               std::placeholders::_1, std::placeholders::_2,
                               std::placeholders::_3);
  this->onDetachFn = std::bind(&FileMappingTrait::onDetach, this,
                               std::placeholders::_1, std::placeholders::_2,
                               std::placeholders::_3);
  this->onLinkFn = std::bind(&FileMappingTrait::onLink, this,
                             std::placeholders::_1, std::placeholders::_2,
                             std::placeholders::_3);
  this->onUnlinkFn = std::bind(&FileMappingTrait::onUnlink, this,
                               std::placeholders::_1, std::placeholders::_2,
                               std::placeholders::_3);
}

void FileMappingTrait::onAttach(HermesPtr hermes, TraitInput &input,
                                Trait *trait) {
  if (load_cb) {
    load_cb(hermes, input, trait);
    // TODO(hari): @errorhandling Check if load was successful
  }
}

void FileMappingTrait::onDetach(HermesPtr hermes, TraitInput &input,
                                Trait *trait) {
  if (flush_cb) {
    flush_cb(hermes, input, trait);
    // TODO(hari): @errorhandling Check if flush was successful
  }
}

void FileMappingTrait::onLink(HermesPtr hermes, TraitInput &input,
                              Trait *trait) {
  onAttach(hermes, input, trait);
}

void FileMappingTrait::onUnlink(HermesPtr hermes, TraitInput &input,
                                Trait *trait) {
  onDetach(hermes, input, trait);
}

PersistTrait::PersistTrait(FileMappingTrait mapping, bool synchronous)
  : Trait(HERMES_PERSIST_TRAIT, TraitIdArray(), TraitType::PERSIST),
    file_mapping(mapping), synchronous(synchronous) {
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

void PersistTrait::onAttach(HermesPtr hermes, TraitInput &input, Trait *trait) {
  onLink(hermes, input, trait);
}

void PersistTrait::onDetach(HermesPtr hermes, TraitInput &input, Trait *trait) {
  (void)hermes;
  (void)input;
  (void)trait;
}

void PersistTrait::onLink(HermesPtr hermes, TraitInput &input, Trait *trait) {
  PersistTrait *persist_trait = (PersistTrait *)trait;
  auto iter = persist_trait->file_mapping.offset_map.find(input.blob_name);

  if (iter != persist_trait->file_mapping.offset_map.end()) {
    SharedMemoryContext *context = &hermes->context_;
    RpcContext *rpc = &hermes->rpc_;
    BucketID bucket_id = GetBucketId(context, rpc, input.bucket_name.c_str());
    BlobID blob_id = GetBlobId(context, rpc, input.blob_name, bucket_id, true);
    std::string filename = persist_trait->file_mapping.filename;
    u64 offset = iter->second;

    if (synchronous) {
      FlushBlob(context, rpc, blob_id, filename, offset);
    } else {
      EnqueueFlushingTask(context, rpc, blob_id, filename, offset);
      IncrementFlushCount(context, rpc, filename);
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
