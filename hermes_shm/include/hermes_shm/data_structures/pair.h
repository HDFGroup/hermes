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

#include "internal/shm_internal.h"

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
struct ShmHeader<TYPED_CLASS> : public ShmBaseHeader {
  ShmArchiveOrT<FirstT> first_;
  ShmArchiveOrT<SecondT> second_;

  /** Default constructor */
  ShmHeader() {
    first_.shm_init();
    second_.shm_init();
  }

  /** Constructor. Default shm allocate. */
  explicit ShmHeader(Allocator *alloc) {
    first_.shm_init(alloc);
    second_.shm_init(alloc);
  }

  /** Piecewise constructor. */
  template<typename FirstArgPackT, typename SecondArgPackT>
  explicit ShmHeader(Allocator *alloc,
                     PiecewiseConstruct &&hint,
                     FirstArgPackT &&first,
                     SecondArgPackT &&second) {
    (void) hint;
    first_.shm_init_piecewise(alloc, std::forward<FirstArgPackT>(first));
    second_.shm_init_piecewise(alloc, std::forward<SecondArgPackT>(second));
  }

  /** Move constructor. */
  explicit ShmHeader(Allocator *alloc,
                     FirstT &&first,
                     SecondT &&second) {
    first_.shm_init(alloc, std::forward<FirstT>(first));
    second_.shm_init(alloc, std::forward<SecondT>(second));
  }

  /** Copy constructor. */
  explicit ShmHeader(Allocator *alloc,
                     const FirstT &first,
                     const SecondT &second) {
    first_.shm_init(alloc, first);
    second_.shm_init(alloc, second);
  }

  /** Shm destructor */
  void shm_destroy(Allocator *alloc) {
    first_.shm_destroy(alloc);
    second_.shm_destroy(alloc);
  }
};

/**
* A string of characters.
* */
template<typename FirstT, typename SecondT>
class pair : public ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE((CLASS_NAME), (TYPED_CLASS), (TYPED_HEADER))

 public:
  hipc::ShmRef<FirstT> first_;
  hipc::ShmRef<SecondT> second_;

 public:
  /** Default constructor */
  pair() = default;

  /** Default shm constructor */
  void shm_init_main(TYPED_HEADER *header, Allocator *alloc) {
    shm_init_allocator(alloc);
    shm_init_header(header, alloc);
  }

  /** Construct pair by moving parameters */
  void shm_init_main(TYPED_HEADER *header,
                     Allocator *alloc,
                     FirstT &&first, SecondT &&second) {
    shm_init_allocator(alloc);
    shm_init_header(header,
                    alloc_,
                    std::forward<FirstT>(first),
                    std::forward<SecondT>(second));
    first_ = hipc::ShmRef<FirstT>(header_->first_.internal_ref(alloc_));
    second_ = hipc::ShmRef<SecondT>(header_->second_.internal_ref(alloc_));
  }

  /** Construct pair by copying parameters */
  void shm_init_main(TYPED_HEADER *header,
                     Allocator *alloc,
                     const FirstT &first, const SecondT &second) {
    shm_init_allocator(alloc);
    shm_init_header(header,
                    alloc_, first, second);
    first_ = hipc::ShmRef<FirstT>(header_->first_.internal_ref(alloc_));
    second_ = hipc::ShmRef<SecondT>(header_->second_.internal_ref(alloc_));
  }

  /** Construct pair piecewise */
  template<typename FirstArgPackT, typename SecondArgPackT>
  void shm_init_main(TYPED_HEADER *header,
                     Allocator *alloc,
                     PiecewiseConstruct &&hint,
                     FirstArgPackT &&first,
                     SecondArgPackT &&second) {
    shm_init_allocator(alloc);
    shm_init_header(header,
                    alloc_,
                    std::forward<PiecewiseConstruct>(hint),
                    std::forward<FirstArgPackT>(first),
                    std::forward<SecondArgPackT>(second));
    first_ = hipc::ShmRef<FirstT>(header_->first_.internal_ref(alloc_));
    second_ = hipc::ShmRef<SecondT>(header_->second_.internal_ref(alloc_));
  }

  /** Move constructor */
  void shm_weak_move_main(TYPED_HEADER *header,
                          Allocator *alloc, pair &other) {
    shm_init_main(header,
                  alloc,
                  std::move(*other.first_),
                  std::move(*other.second_));
  }

  /** Copy constructor */
  void shm_strong_copy_main(TYPED_HEADER *header,
                            Allocator *alloc, const pair &other) {
    shm_init_main(header,
                  alloc, *other.first_, *other.second_);
  }

  /**
  * Destroy the shared-memory data.
  * */
  void shm_destroy_main() {
    header_->shm_destroy(alloc_);
  }

  /** Store into shared memory */
  void shm_serialize_main() const {}

  /** Load from shared memory */
  void shm_deserialize_main() {
    first_ = hipc::ShmRef<FirstT>(header_->first_.internal_ref(alloc_));
    second_ = hipc::ShmRef<SecondT>(header_->second_.internal_ref(alloc_));
  }

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
