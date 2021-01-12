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

FileBackedTrait::FileBackedTrait(
    std::string &filename, std::unordered_map<std::string, u64> &offset_map,
    bool flush, TraitCallback flush_cb, bool load, TraitCallback load_cb)
    : Trait(FILE_TRAIT, TraitIdArray(), TraitType::META),
      filename(filename),
      offset_map(offset_map),
      flush(flush),
      flush_cb(flush_cb),
      load(load),
      load_cb(load_cb) {
  this->onAttachFn = std::bind(&FileBackedTrait::onAttach, this,
                               std::placeholders::_1, std::placeholders::_2);
  this->onDetachFn = std::bind(&FileBackedTrait::onDetach, this,
                               std::placeholders::_1, std::placeholders::_2);
  this->onLinkFn = std::bind(&FileBackedTrait::onLink, this,
                             std::placeholders::_1, std::placeholders::_2);
  this->onUnlinkFn = std::bind(&FileBackedTrait::onUnlink, this,
                               std::placeholders::_1, std::placeholders::_2);
}
void FileBackedTrait::onAttach(TraitInput &input, Trait *trait) {
  if (load) {
    load_cb(input, trait);
    // TODO(hari): @errorhandling Check if load was successful
  }
}
void FileBackedTrait::onDetach(TraitInput &input, Trait *trait) {
  if (flush) {
    flush_cb(input, trait);
    // TODO(hari): @errorhandling Check if flush was successful
  }
}
void FileBackedTrait::onLink(TraitInput &input, Trait *trait) {
  onAttach(input, trait);
}
void FileBackedTrait::onUnlink(TraitInput &input, Trait *trait) {
  onDetach(input, trait);
}
}  // namespace api
}  // namespace hermes
