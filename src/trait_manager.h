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

#ifndef HERMES_SRC_TRAIT_MANAGER_H_
#define HERMES_SRC_TRAIT_MANAGER_H_

#include <dlfcn.h>
#include "hermes_types.h"

/** Forward declaration of trait */
namespace hermes::api {
class Trait;
}

extern "C" {
/** The two methods provided by all traits */
typedef hermes::api::Trait* (*create_trait_t)(hshm::charbuf &trait_info);
typedef const char* (*get_trait_id_t)(void);
}

namespace hermes {

/**
 * Used internally by trait header file
 * Must be used after a "public:"
 * */
#define HERMES_TRAIT_H(TRAIT_ID) \
  static inline const char* trait_name_ = TRAIT_ID;

/** Used internally by trait source file */
#define HERMES_TRAIT_CC(TRAIT_CLASS) \
    extern "C" {                              \
        hermes::api::Trait* create_trait(hshm::charbuf &trait_info) { \
          return new TYPE_UNWRAP(TRAIT_CLASS)(trait_info); \
        } \
        const char* get_trait_id(void) { return TRAIT_CLASS::trait_name_; } \
    }

/** All information needed to create a trait */
struct TraitLibInfo {
  void *lib_;
  create_trait_t create_trait_;
  get_trait_id_t get_trait_id_;

  /** Default constructor */
  TraitLibInfo() = default;

  /** Destructor */
  ~TraitLibInfo() {
    if (lib_) {
      dlclose(lib_);
    }
  }

  /** Emplace constructor */
  explicit TraitLibInfo(void *lib,
                        create_trait_t create_trait,
                        get_trait_id_t get_trait_id)
      : lib_(lib), create_trait_(create_trait), get_trait_id_(get_trait_id) {}

  /** Copy constructor */
  TraitLibInfo(const TraitLibInfo &other)
      : lib_(other.lib_),
        create_trait_(other.create_trait_),
        get_trait_id_(other.get_trait_id_) {}

  /** Move constructor */
  TraitLibInfo(TraitLibInfo &&other) noexcept
      : lib_(other.lib_),
        create_trait_(other.create_trait_),
        get_trait_id_(other.get_trait_id_) {
    other.lib_ = nullptr;
    other.create_trait_ = nullptr;
    other.get_trait_id_ = nullptr;
  }
};

/** A module manager to keep track of trait binaries */
class TraitManager {
private:
  std::unordered_map<std::string, TraitLibInfo> traits_;

public:
  /** Identify the set of all traits and load them */
  void Init();

  /** Construct a trait */
  template<typename TraitT>
  api::Trait* ConstructTrait(hshm::charbuf &params) {
    auto iter = traits_.find(TraitT::trait_name_);
    if (iter == traits_.end()) {
      HELOG(kError, "Could not find the trait with name: {}",
            TraitT::trait_name_);
      return nullptr;
    }
    TraitLibInfo &info = iter->second;
    api::Trait *trait = info.create_trait_(params);
    return trait;
  }

private:
  /** Find the trait library on disk and load it */
  bool FindLoadTrait(const std::string &paths,
                     const std::string &trait_rel_path);
};

}

#endif // HERMES_SRC_TRAIT_MANAGER_H_
