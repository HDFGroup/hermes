//
// Created by lukemartinlogan on 3/7/23.
//

#ifndef HERMES_SRC_API_TRAIT_H_
#define HERMES_SRC_API_TRAIT_H_

#include "hermes_types.h"

namespace hermes::api {

enum class TraitClass {
  kGroupBy,
  kBucket,
};

struct TraitHeader {
  char trait_uuid_[64];
  TraitClass trait_class_;

  explicit TraitHeader(const std::string &trait_uuid, TraitClass trait_class) {
    memcpy(trait_uuid_, trait_uuid.c_str(), trait_uuid.size());
    trait_class_ = trait_class;
  }
};

class Trait {
 public:
  hipc::charbuf trait_info_;
  TraitHeader *header_;

  /** Default constructor */
  Trait() = default;

  /** Deserialization constructor */
  explicit Trait(hipc::charbuf &trait_info)
      : trait_info_(trait_info),
        header_(reinterpret_cast<TraitHeader*>(trait_info_.data())) {}

  template<typename HeaderT, typename ...Args>
  HeaderT* CreateHeader(Args&& ...args) {
    trait_info_ = hipc::charbuf(sizeof(HeaderT));
    header_ = reinterpret_cast<TraitHeader*>(trait_info_.data());
    hipc::Allocator::ConstructObj<HeaderT>(
        *GetHeader<HeaderT>(),
        std::forward<Args>(args)...);
    return GetHeader<HeaderT>();
  }

  template<typename HeaderT>
  HeaderT* GetHeader() {
    return reinterpret_cast<HeaderT*>(header_);
  }
};

template<typename TraitT>
inline void ExecuteTrait(Trait *trait) {
  reinterpret_cast<TraitT>(trait)->Run();
}

}  // namespace hermes::api

#endif  // HERMES_SRC_API_TRAIT_H_
