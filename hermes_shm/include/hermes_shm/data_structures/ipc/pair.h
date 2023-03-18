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

#ifndef HERMES_INCLUDE_HERMES_DATA_STRUCTURES_PAIR_H_
#define HERMES_INCLUDE_HERMES_DATA_STRUCTURES_PAIR_H_

#include "hermes_shm/data_structures/ipc/internal/shm_internal.h"
#include "hermes_shm/data_structures/smart_ptr/smart_ptr_base.h"

namespace hermes_shm::ipc {

/** forward declaration for string */
template<typename FirstT, typename SecondT>
class pair;

/**
* MACROS used to simplify the string namespace
* Used as inputs to the SHM_CONTAINER_TEMPLATE
* */
#define CLASS_NAME pair
#define TYPED_CLASS pair<FirstT, SecondT>
#define TYPED_HEADER ShmHeader<TYPED_CLASS>

/** pair shared-memory header */
template<typename FirstT, typename SecondT>
struct ShmHeader<TYPED_CLASS> {
  SHM_CONTAINER_HEADER_TEMPLATE(ShmHeader)
  ShmArchive<FirstT> first_;
  ShmArchive<SecondT> second_;
  void strong_copy() {}
};

/**
* A pair of two objects.
* */
template<typename FirstT, typename SecondT>
class pair : public ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE((CLASS_NAME), (TYPED_CLASS), (TYPED_HEADER))

 public:
  hipc::Ref<FirstT> first_;
  hipc::Ref<SecondT> second_;

 public:
  /**====================================
   * Default Constructor
   * ===================================*/

  /** SHM constructor. Default. */
  explicit pair(TYPED_HEADER *header, Allocator *alloc) {
    shm_init_header(header, alloc);
    first_ = make_ref<FirstT>(header_->first_, alloc_);
    second_ = make_ref<SecondT>(header_->second_, alloc_);
  }

  /**====================================
   * Emplace Constructors
   * ===================================*/

  /** SHM constructor. Move parameters. */
  explicit pair(TYPED_HEADER *header, Allocator *alloc,
                FirstT &&first, SecondT &&second) {
    shm_init_header(header, alloc);
    first_ = make_ref<FirstT>(header_->first_,
                              alloc_, std::forward<FirstT>(first));
    second_ = make_ref<SecondT>(header_->second_,
                                alloc_, std::forward<SecondT>(second));
  }

  /** SHM constructor. Copy parameters. */
  explicit pair(TYPED_HEADER *header, Allocator *alloc,
                const FirstT &first, const SecondT &second) {
    shm_init_header(header, alloc);
    first_ = make_ref<FirstT>(header_->first_, alloc_, first);
    second_ = make_ref<SecondT>(header_->second_, alloc_, second);
  }

  /** SHM constructor. Piecewise emplace. */
  template<typename FirstArgPackT, typename SecondArgPackT>
  explicit pair(TYPED_HEADER *header, Allocator *alloc,
                PiecewiseConstruct &&hint,
                FirstArgPackT &&first,
                SecondArgPackT &&second) {
    shm_init_header(header, alloc);
    first_ = make_ref_piecewise<FirstT>(
      make_argpack(header_->first_, alloc_),
      std::forward<FirstArgPackT>(first));
    second_ = make_ref_piecewise<SecondT>(
      make_argpack(header_->second_, alloc_),
      std::forward<SecondArgPackT>(second));
  }

  /**====================================
   * Copy Constructors
   * ===================================*/

  /** SHM copy constructor. From pair. */
  explicit pair(TYPED_HEADER *header, Allocator *alloc, const pair &other) {
    shm_init_header(header, alloc);
    shm_strong_copy_construct(other);
  }

  /** SHM copy constructor main */
  void shm_strong_copy_construct(const pair &other) {
    first_ = hipc::make_ref<FirstT>(header_->first_, alloc_,
                                    *other.first_);
    second_ = hipc::make_ref<SecondT>(header_->second_, alloc_,
                                      *other.second_);
  }

  /** SHM copy assignment operator. From pair. */
  pair& operator=(const pair &other) {
    if (this != &other) {
      shm_destroy();
      shm_strong_copy_assign_op(other);
    }
    return *this;
  }

  /** SHM copy assignment operator main */
  void shm_strong_copy_assign_op(const pair &other) {
    (*first_) = (*other.first_);
    (*second_) = (*other.second_);
  }

  /**====================================
   * Move Constructors
   * ===================================*/

  /** SHM move constructor. From pair. */
  explicit pair(TYPED_HEADER *header, Allocator *alloc, pair &&other) {
    shm_init_header(header, alloc);
    if (alloc_ == other.alloc_) {
      first_ = hipc::make_ref<FirstT>(header_->first_, alloc_,
                                      std::forward<FirstT>(*other.first_));
      second_ = hipc::make_ref<SecondT>(header_->second_, alloc_,
                                        std::forward<SecondT>(*other.second_));
    } else {
      shm_strong_copy_construct(other);
      other.shm_destroy();
    }
  }

  /** SHM move assignment operator. From pair. */
  pair& operator=(pair &&other) noexcept {
    if (this != &other) {
      shm_destroy();
      if (alloc_ == other.alloc_) {
        (*first_) = std::move(*other.first_);
        (*second_) = std::move(*other.second_);
        other.SetNull();
      } else {
        shm_strong_copy_assign_op(other);
        other.shm_destroy();
      }
    }
    return *this;
  }

  /**====================================
   * Destructor
   * ===================================*/

  /** Check if the pair is empty */
  bool IsNull() const {
    return false;
  }

  /** Sets this pair as empty */
  void SetNull() {}

  /** Destroy the shared-memory data */
  void shm_destroy_main() {
    first_.shm_destroy();
    second_.shm_destroy();
  }

  /**====================================
   * SHM Deserialization
   * ===================================*/

  /** Load from shared memory */
  void shm_deserialize_main() {
    first_ = hipc::Ref<FirstT>(header_->first_, alloc_);
    second_ = hipc::Ref<SecondT>(header_->second_, alloc_);
  }

  /**====================================
   * pair Methods
   * ===================================*/

  /** Get the first object */
  FirstT& GetFirst() { return *first_; }

  /** Get the first object (const) */
  FirstT& GetFirst() const { return *first_; }

  /** Get the second object */
  SecondT& GetSecond() { return *second_; }

  /** Get the second object (const) */
  SecondT& GetSecond() const { return *second_; }

  /** Get the first object (treated as key) */
  FirstT& GetKey() { return *first_; }

  /** Get the first object (treated as key) (const) */
  FirstT& GetKey() const { return *first_; }

  /** Get the second object (treated as value) */
  SecondT& GetVal() { return *second_; }

  /** Get the second object (treated as value) (const) */
  SecondT& GetVal() const { return *second_; }
};

#undef CLASS_NAME
#undef TYPED_CLASS
#undef TYPED_HEADER

}  // namespace hermes_shm::ipc

#endif  // HERMES_INCLUDE_HERMES_DATA_STRUCTURES_PAIR_H_
