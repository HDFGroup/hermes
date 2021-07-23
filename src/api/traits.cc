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
    std::string &filename, std::unordered_map<std::string, u64> &offset_map,
    FILE *fh, TraitCallback flush_cb, TraitCallback load_cb)
    : Trait(HERMES_FILE_TRAIT, TraitIdArray(), TraitType::FILE_MAPPING),
      flush_cb(flush_cb),
      load_cb(load_cb),
      filename(filename),
      offset_map(offset_map),
      fh(fh) {
  this->onAttachFn = std::bind(&FileMappingTrait::onAttach, this,
                               std::placeholders::_1, std::placeholders::_2,
                               std::placeholders::_3, std::placeholders::_4);
  this->onDetachFn = std::bind(&FileMappingTrait::onDetach, this,
                               std::placeholders::_1, std::placeholders::_2,
                               std::placeholders::_3, std::placeholders::_4);
  this->onLinkFn = std::bind(&FileMappingTrait::onLink, this,
                             std::placeholders::_1, std::placeholders::_2,
                             std::placeholders::_3, std::placeholders::_4);
  this->onUnlinkFn = std::bind(&FileMappingTrait::onUnlink, this,
                               std::placeholders::_1, std::placeholders::_2,
                               std::placeholders::_3, std::placeholders::_4);
}

void FileMappingTrait::onAttach(HermesPtr hermes, TraitInput &input,
                                Trait *trait, void *trait_args) {
  (void)trait_args;
  if (load_cb) {
    load_cb(hermes, input, trait, nullptr);
    // TODO(hari): @errorhandling Check if load was successful
  }
}

void FileMappingTrait::onDetach(HermesPtr hermes, TraitInput &input,
                                Trait *trait, void *trait_args) {
  (void)trait_args;
  if (flush_cb) {
    flush_cb(hermes, input, trait, nullptr);
    // TODO(hari): @errorhandling Check if flush was successful
  }
}

void FileMappingTrait::onLink(HermesPtr hermes, TraitInput &input,
                              Trait *trait, void *trait_args) {
  (void)trait_args;
  onAttach(hermes, input, trait, nullptr);
}

void FileMappingTrait::onUnlink(HermesPtr hermes, TraitInput &input,
                                Trait *trait, void *trait_args) {
  (void)trait_args;
  onDetach(hermes, input, trait, nullptr);
}

PersistTrait::PersistTrait(FileMappingTrait mapping, bool synchronous)
  : Trait(HERMES_PERSIST_TRAIT, TraitIdArray(), TraitType::PERSIST),
    file_mapping(mapping), synchronous(synchronous) {
  this->onAttachFn = std::bind(&PersistTrait::onAttach, this,
                               std::placeholders::_1, std::placeholders::_2,
                               std::placeholders::_3, std::placeholders::_4);
  this->onDetachFn = std::bind(&PersistTrait::onDetach, this,
                               std::placeholders::_1, std::placeholders::_2,
                               std::placeholders::_3, std::placeholders::_4);
  this->onLinkFn = std::bind(&PersistTrait::onLink, this,
                             std::placeholders::_1, std::placeholders::_2,
                             std::placeholders::_3, std::placeholders::_4);
  this->onUnlinkFn = std::bind(&PersistTrait::onUnlink, this,
                               std::placeholders::_1, std::placeholders::_2,
                               std::placeholders::_3, std::placeholders::_4);
}

void PersistTrait::onAttach(HermesPtr hermes, TraitInput &input, Trait *trait,
                            void *trait_args) {
  (void)hermes;
  (void)input;
  (void)trait;
  (void)trait_args;
  // PersistTrait *persist_trait = (PersistTrait *)trait;

  // TODO(chogan):
  // for each blob:
  //   onLink(blob)
}

void PersistTrait::onDetach(HermesPtr hermes, TraitInput &input, Trait *trait,
                            void *trait_args) {
  (void)trait_args;
  (void)hermes;
  (void)input;
  (void)trait;
}

void PersistTrait::onLink(HermesPtr hermes, TraitInput &input, Trait *trait,
                          void *trait_args) {
  size_t offset = *(size_t *)trait_args;
  PersistTrait *persist_trait = (PersistTrait *)trait;
  SharedMemoryContext *context = &hermes->context_;
  RpcContext *rpc = &hermes->rpc_;
  BucketID bucket_id = GetBucketId(context, rpc, input.bucket_name.c_str());
  BlobID blob_id = GetBlobId(context, rpc, input.blob_name, bucket_id, true);
  std::string filename = persist_trait->file_mapping.filename;

  if (synchronous) {
    FlushBlob(context, rpc, blob_id, filename, offset);
  } else {
    EnqueueFlushingTask(context, rpc, blob_id, filename, offset);
    IncrementFlushCount(context, rpc, filename);
  }
}

void PersistTrait::onUnlink(HermesPtr hermes, TraitInput &input, Trait *trait,
                            void *trait_args) {
  (void)trait_args;
  (void)hermes;
  (void)input;
  (void)trait;
}

}  // namespace api
}  // namespace hermes
