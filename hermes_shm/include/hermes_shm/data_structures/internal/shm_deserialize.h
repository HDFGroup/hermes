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

#ifndef HERMES_INCLUDE_HERMES_DATA_STRUCTURES_INTERNAL_DESERIALIZE_H_
#define HERMES_INCLUDE_HERMES_DATA_STRUCTURES_INTERNAL_DESERIALIZE_H_

#include "hermes_shm/memory/memory_registry.h"

namespace hermes_shm::ipc {

/**
 * The parameters used to deserialize an object.
 * */
template<typename ContainerT>
struct ShmDeserialize {
 public:
  typedef typename ContainerT::header_t header_t;
  Allocator *alloc_;
  header_t *header_;

 public:
  /** Default constructor */
  ShmDeserialize() = default;

  /** Construct from TypedPointer */
  ShmDeserialize(const TypedPointer<ContainerT> &ar) {
    alloc_ = HERMES_MEMORY_REGISTRY->GetAllocator(ar.allocator_id_);
    header_ = alloc_->Convert<
      TypedPointer<ContainerT>,
      OffsetPointer>(ar.ToOffsetPointer());
  }

  /** Construct from allocator + offset pointer */
  ShmDeserialize(TypedOffsetPointer<ContainerT> &ar, Allocator *alloc) {
    alloc_ = alloc;
    header_ = alloc_->Convert<
      TypedPointer<ContainerT>,
      OffsetPointer>(ar.ToOffsetPointer());
  }

  /** Construct from header (ptr) + allocator */
  ShmDeserialize(header_t *header, Allocator *alloc) {
    alloc_ = alloc;
    header_ = header;
  }

  /** Construct from header (ref) + allocator */
  ShmDeserialize(header_t &header, Allocator *alloc) {
    alloc_ = alloc;
    header_ = &header;
  }

  /** Copy constructor */
  ShmDeserialize(const ShmDeserialize &other) {
    shm_strong_copy(other);
  }

  /** Copy assign operator */
  ShmDeserialize& operator=(const ShmDeserialize &other) {
    shm_strong_copy(other);
    return *this;
  }

  /** Copy operation */
  void shm_strong_copy(const ShmDeserialize &other) {
    alloc_ = other.alloc_;
    header_ = other.header_;
  }

  /** Move constructor */
  ShmDeserialize(ShmDeserialize &&other) {
    shm_weak_move(other);
  }

  /** Move assign operator */
  ShmDeserialize& operator=(ShmDeserialize &&other) {
    shm_weak_move();
    return *this;
  }

  /** Move operation */
  void shm_weak_move(ShmDeserialize &other) {
    shm_strong_copy(other);
  }
};

}  // namespace hermes_shm::ipc

#endif //HERMES_INCLUDE_HERMES_DATA_STRUCTURES_INTERNAL_DESERIALIZE_H_
