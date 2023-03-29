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
#include "shm_archive.h"

namespace hshm::ipc {

/**
 * Indicates that a data structure can be archived in shared memory
 * and has a corresponding TypedPointer override.
 * */
class ShmArchiveable {};

template<typename ContainerT>
struct _ShmDeserializeShm {
  typedef typename ContainerT::header_t header_t;
};

template<typename ContainerT>
struct _ShmDeserializeNoShm {
  typedef ContainerT header_t;
};

#define MAKE_SHM_DESERIALIZE(T) \
  SHM_X_OR_Y(T, _ShmDeserializeShm<T>, _ShmDeserializeNoShm<T>)::header_t

/**
 * Indicates that a Deserialize should also be initialized
 * */
struct ShmInit {};

/**
 * The parameters used to deserialize an object.
 * */
template<typename ContainerT>
struct ShmDeserialize {
 public:
  typedef MAKE_SHM_DESERIALIZE(ContainerT) header_t;
  Allocator *alloc_;
  header_t *header_;

 public:
  /** Default constructor */
  ShmDeserialize() = default;

  /** Construct from TypedPointer */
  explicit ShmDeserialize(const TypedPointer<ContainerT> &ar) {
    alloc_ = HERMES_MEMORY_REGISTRY->GetAllocator(ar.allocator_id_);
    header_ = alloc_->Convert<
      header_t, OffsetPointer>(ar.ToOffsetPointer());
  }

  /** Construct from allocator + offset pointer */
  explicit ShmDeserialize(TypedOffsetPointer<ContainerT> &ar,
                          Allocator *alloc) {
    alloc_ = alloc;
    header_ = alloc_->Convert<
      header_t, OffsetPointer>(ar.ToOffsetPointer());
  }

  /** Construct from header (ptr) + allocator */
  explicit ShmDeserialize(header_t *header, Allocator *alloc) {
    alloc_ = alloc;
    header_ = header;
  }

  /** Construct from header (ref) + allocator */
  explicit ShmDeserialize(header_t &header, Allocator *alloc) {
    alloc_ = alloc;
    header_ = &header;
  }

  /** Copy constructor */
  ShmDeserialize(const ShmDeserialize &other) {
    shm_strong_copy(other);
  }

  /** Copy assign operator */
  ShmDeserialize& operator=(const ShmDeserialize &other) {
    if (this != &other) {
      shm_strong_copy(other);
    }
    return *this;
  }

  /** Move constructor */
  ShmDeserialize(ShmDeserialize &&other) noexcept {
    shm_strong_copy(other);
  }

  /** Move assign operator */
  ShmDeserialize& operator=(ShmDeserialize &&other) {
    if (this != &other) {
      shm_strong_copy();
    }
    return *this;
  }

  /** Internal copy operation */
  void shm_strong_copy(const ShmDeserialize &other) {
    alloc_ = other.alloc_;
    header_ = other.header_;
  }
};

}  // namespace hshm::ipc

#endif  // HERMES_INCLUDE_HERMES_DATA_STRUCTURES_INTERNAL_DESERIALIZE_H_
