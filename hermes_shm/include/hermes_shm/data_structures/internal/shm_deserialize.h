//
// Created by lukemartinlogan on 1/24/23.
//

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_INTERNAL_DESERIALIZE_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_INTERNAL_DESERIALIZE_H_

#include "hermes_shm/memory/memory_manager.h"

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
    alloc_ = HERMES_SHM_MEMORY_MANAGER->GetAllocator(ar.allocator_id_);
    header_ = alloc_->Convert<
      TypedPointer<ContainerT>,
      OffsetPointer>(ar.ToOffsetPointer());
  }

  /** Construct from allocator + offset pointer */
  ShmDeserialize(Allocator *alloc, TypedOffsetPointer<ContainerT> &ar) {
    alloc_ = alloc;
    header_ = alloc_->Convert<
      TypedPointer<ContainerT>,
      OffsetPointer>(ar.ToOffsetPointer());
  }

  /** Construct from allocator + offset pointer */
  ShmDeserialize(Allocator *alloc, header_t *header) {
    alloc_ = alloc;
    header_ = header;
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

#endif //HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_INTERNAL_DESERIALIZE_H_
