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

namespace hshm::ipc {

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

/**
* A pair of two objects.
* */
template<typename FirstT, typename SecondT>
class pair : public ShmContainer {
 public:
  /**====================================
   * Variables
   * ===================================*/
  SHM_CONTAINER_TEMPLATE((CLASS_NAME), (TYPED_CLASS))
  ShmArchive<FirstT> first_;
  ShmArchive<SecondT> second_;

 public:
  /**====================================
   * Default Constructor
   * ===================================*/

  /** SHM constructor. Default. */
  explicit pair(Allocator *alloc) {
    shm_init_container(alloc);
    HSHM_MAKE_AR0(first_, GetAllocator())
    HSHM_MAKE_AR0(second_, GetAllocator())
  }

  /**====================================
   * Emplace Constructors
   * ===================================*/

  /** SHM constructor. Move parameters. */
  explicit pair(Allocator *alloc,
                FirstT &&first, SecondT &&second) {
    shm_init_container(alloc);
    HSHM_MAKE_AR(first_, GetAllocator(),
                 std::forward<FirstT>(first))
    HSHM_MAKE_AR(second_, GetAllocator(),
                 std::forward<SecondT>(second))
  }

  /** SHM constructor. Copy parameters. */
  explicit pair(Allocator *alloc,
                const FirstT &first, const SecondT &second) {
    shm_init_container(alloc);
    HSHM_MAKE_AR(first_, GetAllocator(), first)
    HSHM_MAKE_AR(second_, GetAllocator(), second)
  }

  /** SHM constructor. Piecewise emplace. */
  template<typename FirstArgPackT, typename SecondArgPackT>
  explicit pair(Allocator *alloc,
                PiecewiseConstruct &&hint,
                FirstArgPackT &&first,
                SecondArgPackT &&second) {
    shm_init_container(alloc);
    HSHM_MAKE_AR_PW(first_, GetAllocator(),
                    std::forward<FirstArgPackT>(first))
    HSHM_MAKE_AR_PW(second_, GetAllocator(),
                    std::forward<SecondArgPackT>(second))
  }

  /**====================================
   * Copy Constructors
   * ===================================*/

  /** SHM copy constructor. From pair. */
  explicit pair(Allocator *alloc, const pair &other) {
    shm_init_container(alloc);
    shm_strong_copy_construct(other);
  }

  /** SHM copy constructor main */
  void shm_strong_copy_construct(const pair &other) {
    HSHM_MAKE_AR(first_, GetAllocator(), *other.first_)
    HSHM_MAKE_AR(second_, GetAllocator(), *other.second_)
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
    (first_.get_ref()) = (*other.first_);
    (second_.get_ref()) = (*other.second_);
  }

  /**====================================
   * Move Constructors
   * ===================================*/

  /** SHM move constructor. From pair. */
  explicit pair(Allocator *alloc, pair &&other) {
    shm_init_container(alloc);
    if (GetAllocator() == other.GetAllocator()) {
      HSHM_MAKE_AR(first_, GetAllocator(),
                   std::forward<FirstT>(*other.first_))
      HSHM_MAKE_AR(second_, GetAllocator(),
                   std::forward<SecondT>(*other.second_))
    } else {
      shm_strong_copy_construct(other);
      other.shm_destroy();
    }
  }

  /** SHM move assignment operator. From pair. */
  pair& operator=(pair &&other) noexcept {
    if (this != &other) {
      shm_destroy();
      if (GetAllocator() == other.GetAllocator()) {
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
  HSHM_ALWAYS_INLINE bool IsNull() const {
    return false;
  }

  /** Sets this pair as empty */
  HSHM_ALWAYS_INLINE void SetNull() {}

  /** Destroy the shared-memory data */
  HSHM_ALWAYS_INLINE void shm_destroy_main() {
    HSHM_DESTROY_AR(first_)
    HSHM_DESTROY_AR(second_)
  }

  /**====================================
   * pair Methods
   * ===================================*/

  /** Get the first object */
  HSHM_ALWAYS_INLINE FirstT& GetFirst() { return first_.get_ref(); }

  /** Get the first object (const) */
  HSHM_ALWAYS_INLINE FirstT& GetFirst() const { return first_.get_ref(); }

  /** Get the second object */
  HSHM_ALWAYS_INLINE SecondT& GetSecond() { return second_.get_ref(); }

  /** Get the second object (const) */
  HSHM_ALWAYS_INLINE SecondT& GetSecond() const { return second_.get_ref(); }

  /** Get the first object (treated as key) */
  HSHM_ALWAYS_INLINE FirstT& GetKey() { return first_.get_ref(); }

  /** Get the first object (treated as key) (const) */
  HSHM_ALWAYS_INLINE FirstT& GetKey() const { return first_.get_ref(); }

  /** Get the second object (treated as value) */
  HSHM_ALWAYS_INLINE SecondT& GetVal() { return second_.get_ref(); }

  /** Get the second object (treated as value) (const) */
  HSHM_ALWAYS_INLINE SecondT& GetVal() const { return second_.get_ref(); }
};

#undef CLASS_NAME
#undef TYPED_CLASS
#undef TYPED_HEADER

}  // namespace hshm::ipc

#endif  // HERMES_INCLUDE_HERMES_DATA_STRUCTURES_PAIR_H_
