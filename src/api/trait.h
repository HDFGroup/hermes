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

#ifndef HERMES_SRC_API_TRAIT_H_
#define HERMES_SRC_API_TRAIT_H_

#include "hermes_types.h"

namespace hapi = hermes::api;

namespace hermes::api {

enum class TraitClass {
  kGroupBy,
  kBucket,
};

struct TraitHeader {
  char trait_uuid_[64];
  char trait_name_[64];
  TraitClass trait_class_;

  explicit TraitHeader(const std::string &trait_uuid,
                       const std::string &trait_name,
                       TraitClass trait_class) {
    memcpy(trait_uuid_, trait_uuid.c_str(), trait_uuid.size());
    memcpy(trait_name_, trait_name.c_str(), trait_name.size());
    trait_uuid_[trait_uuid.size()] = 0;
    trait_name_[trait_name.size()] = 0;
    trait_class_ = trait_class;
  }
};

/**
 * Represents a custom operation to perform.
 * Traits are independent of Hermes.
 * */
class Trait {
 public:
  hshm::charbuf trait_info_;
  TraitHeader *header_;

  /** Default constructor */
  Trait() = default;

  /** Deserialization constructor */
  explicit Trait(hshm::charbuf &trait_info)
      : trait_info_(trait_info),
        header_(reinterpret_cast<TraitHeader*>(trait_info_.data())) {}

  /** Run a method of the trait */
  virtual void Run(int method, void *params) = 0;

  /** Create the header for the trait */
  template<typename HeaderT, typename ...Args>
  HeaderT* CreateHeader(Args&& ...args) {
    trait_info_ = hshm::charbuf(sizeof(HeaderT));
    header_ = reinterpret_cast<TraitHeader*>(trait_info_.data());
    hipc::Allocator::ConstructObj<HeaderT>(
        *GetHeader<HeaderT>(),
        std::forward<Args>(args)...);
    return GetHeader<HeaderT>();
  }

  /** Get the header of the trait */
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
