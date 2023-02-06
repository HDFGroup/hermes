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

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_INTERNAL_SHM_NULL_CONTAINER_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_INTERNAL_SHM_NULL_CONTAINER_H_

#include "shm_container.h"
#include <string>

namespace hermes_shm::ipc {

/** forward declaration for string */
class NullContainer;

/**
 * MACROS used to simplify the string namespace
 * Used as inputs to the SHM_CONTAINER_TEMPLATE
 * */
#define CLASS_NAME NullContainer
#define TYPED_CLASS NullContainer
#define TYPED_HEADER ShmHeader<NullContainer>

/** string shared-memory header */
template<>
struct ShmHeader<NullContainer> : public ShmBaseHeader {
  /** Default constructor */
  ShmHeader() = default;

  /** Copy constructor */
  ShmHeader(const ShmHeader &other) {
    strong_copy(other);
  }

  /** Copy assignment operator */
  ShmHeader& operator=(const ShmHeader &other) {
    if (this != &other) {
      strong_copy(other);
    }
    return *this;
  }

  /** Strong copy operation */
  void strong_copy(const ShmHeader &other) {}

  /** Move constructor */
  ShmHeader(ShmHeader &&other) {
    weak_move(other);
  }

  /** Move operator */
  ShmHeader& operator=(ShmHeader &&other) {
    if (this != &other) {
      weak_move(other);
    }
    return *this;
  }

  /** Move operation */
  void weak_move(ShmHeader &other) {
    strong_copy(other);
  }
};

/**
 * A string of characters.
 * */
class NullContainer : public ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE((CLASS_NAME), (TYPED_CLASS), (TYPED_HEADER))

 public:
  ////////////////////////////
  /// SHM Overrides
  ////////////////////////////

  /** Default constructor */
  NullContainer() = default;

  /** Default shm constructor */
  void shm_init_main(TYPED_HEADER *header,
                     Allocator *alloc) {
    shm_init_allocator(alloc);
    shm_init_header(header);
  }

  /** Move constructor */
  void shm_weak_move_main(TYPED_HEADER *header,
                          Allocator *alloc,
                          NullContainer &other) {}

  /** Copy constructor */
  void shm_strong_copy_main(TYPED_HEADER *header,
                            Allocator *alloc,
                            const NullContainer &other) {}

  /**
   * Destroy the shared-memory data.
   * */
  void shm_destroy_main() {}

  /** Store into shared memory */
  void shm_serialize_main() const {}

  /** Load from shared memory */
  void shm_deserialize_main() {}
};

}  // namespace hermes_shm::ipc

namespace std {

}  // namespace std

#undef CLASS_NAME
#undef TYPED_CLASS
#undef TYPED_HEADER

#endif //HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_INTERNAL_SHM_NULL_CONTAINER_H_
