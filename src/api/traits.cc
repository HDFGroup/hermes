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
                               std::placeholders::_1, std::placeholders::_2);
  this->onDetachFn = std::bind(&FileMappingTrait::onDetach, this,
                               std::placeholders::_1, std::placeholders::_2);
  this->onLinkFn = std::bind(&FileMappingTrait::onLink, this,
                             std::placeholders::_1, std::placeholders::_2);
  this->onUnlinkFn = std::bind(&FileMappingTrait::onUnlink, this,
                               std::placeholders::_1, std::placeholders::_2);
}
void FileMappingTrait::onAttach(TraitInput &input, Trait *trait) {
  if (load_cb) {
    load_cb(input, trait);
    // TODO(hari): @errorhandling Check if load was successful
  }
}
void FileMappingTrait::onDetach(TraitInput &input, Trait *trait) {
  if (flush_cb) {
    flush_cb(input, trait);
    // TODO(hari): @errorhandling Check if flush was successful
  }
}
void FileMappingTrait::onLink(TraitInput &input, Trait *trait) {
  onAttach(input, trait);
}
void FileMappingTrait::onUnlink(TraitInput &input, Trait *trait) {
  onDetach(input, trait);
}
}  // namespace api
}  // namespace hermes
