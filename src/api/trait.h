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
#include "trait_manager.h"

namespace hapi = hermes::api;

namespace hermes::api {

/** Determines where the trait is used within Hermes */
enum class TraitClass {
  kGroupBy,
  kBucket,
};

/** The basic state needed to be stored by every trait */
struct TraitHeader {
  char trait_uuid_[64];   /**< Unique name for this instance of trait */
  TraitClass trait_class_;  /**< Where the trait is useful */

  /** Constructor. */
  explicit TraitHeader(const std::string &trait_uuid,
                       TraitClass trait_class) {
    memcpy(trait_uuid_, trait_uuid.c_str(), trait_uuid.size());
    trait_uuid_[trait_uuid.size()] = 0;
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

}  // namespace hermes::api

#endif  // HERMES_SRC_API_TRAIT_H_
