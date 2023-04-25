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

namespace hermes {

/** This trait is useful to the GroupBy function */
#define HERMES_TRAIT_GROUP_BY BIT_OPT(uint32_t, 0)
/** This trait is useful to Put & Get */
#define HERMES_TRAIT_PUT_GET BIT_OPT(uint32_t, 1)
/** This trait is useful to BORG's Flush operation */
#define HERMES_TRAIT_FLUSH BIT_OPT(uint32_t, 2)

/** The basic state needed to be stored by every trait */
struct TraitHeader {
  char trait_uuid_[64];   /**< Unique name for this instance of trait */
  char trait_name_[64];   /**< The name of the trait shared object */
  bitfield32_t flags_;    /**< Where the trait is useful */

  /** Constructor. */
  explicit TraitHeader(const std::string &trait_uuid,
                       const std::string &trait_name,
                       uint32_t flags) {
    memcpy(trait_uuid_, trait_uuid.c_str(), trait_uuid.size());
    memcpy(trait_name_, trait_name.c_str(), trait_name.size());
    trait_uuid_[trait_uuid.size()] = 0;
    trait_name_[trait_name.size()] = 0;
    flags_.SetBits(flags);
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
  TraitId trait_id_;
  std::string trait_uuid_;

  /** Default constructor */
  Trait() : trait_id_(TraitId::GetNull()) {}

  /** Virtual destructor */
  virtual ~Trait() = default;

  /** Deserialization constructor */
  explicit Trait(hshm::charbuf &trait_info)
      : trait_info_(trait_info),
        header_(reinterpret_cast<TraitHeader*>(trait_info_.data())),
        trait_id_(TraitId::GetNull()) {}

  /** Get trait ID */
  TraitId GetTraitId() {
    return trait_id_;
  }

  /** Get trait uuid */
  const std::string& GetTraitUuid() {
    return trait_uuid_;
  }

  /** Get trait class */
  bitfield32_t& GetTraitFlags() {
    return header_->flags_;
  }

  /** Run a method of the trait */
  virtual void Run(int method, void *params) = 0;

  /** Create the header for the trait */
  template<typename HeaderT, typename ...Args>
  HeaderT* CreateHeader(const std::string &trait_uuid,
                        Args&& ...args) {
    trait_info_ = hshm::charbuf(sizeof(HeaderT));
    trait_uuid_ = trait_uuid;
    header_ = reinterpret_cast<TraitHeader*>(trait_info_.data());
    hipc::Allocator::ConstructObj<HeaderT>(
        *GetHeader<HeaderT>(),
        trait_uuid,
        std::forward<Args>(args)...);
    return GetHeader<HeaderT>();
  }

  /** Get the header of the trait */
  template<typename HeaderT>
  HeaderT* GetHeader() {
    return reinterpret_cast<HeaderT*>(header_);
  }

  /**
   * Method to register a trait with Hermes.
   * Defined automatically in HERMES_TRAIT_H
   * */
  virtual void Register() = 0;
};

extern "C" {
/** The two methods provided by all traits */
typedef Trait* (*create_trait_t)(hshm::charbuf &trait_info);
typedef const char* (*get_trait_id_t)(void);
}

/**
 * Used internally by trait header file
 * Must be used after a "public:"
 * */
#define HERMES_TRAIT_H(TYPED_CLASS, TRAIT_ID) \
  static inline const char* trait_name_ = TRAIT_ID; \
  void Register() override { \
    if (trait_id_.IsNull()) { \
      trait_id_ = HERMES->RegisterTrait<TYPE_UNWRAP(TYPED_CLASS)>(this); \
    } \
  }

/** Used internally by trait source file */
#define HERMES_TRAIT_CC(TRAIT_CLASS) \
    extern "C" {                              \
        hermes::Trait* create_trait(hshm::charbuf &trait_info) { \
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

  /** Construct a trait from a serialized parameter set */
  Trait* ConstructTrait(hshm::charbuf &params) {
    auto hdr = reinterpret_cast<TraitHeader*>(params.data());
    auto iter = traits_.find(hdr->trait_name_);
    if (iter == traits_.end()) {
      HELOG(kError, "Could not find the trait with name: {}",
            hdr->trait_name_);
      return nullptr;
    }
    TraitLibInfo &info = iter->second;
    Trait *trait = info.create_trait_(params);
    return trait;
  }

 private:
  /** Find the trait library on disk and load it */
  bool FindLoadTrait(const std::string &paths,
                     const std::string &trait_rel_path);
};

}  // namespace hermes

#endif  // HERMES_SRC_TRAIT_MANAGER_H_
