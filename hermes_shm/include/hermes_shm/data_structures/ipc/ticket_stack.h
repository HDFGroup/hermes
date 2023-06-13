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

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_IPC_TICKET_STACK_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_IPC_TICKET_STACK_H_

#include "hermes_shm/data_structures/ipc/internal/shm_internal.h"
#include "hermes_shm/thread/lock.h"
#include "vector.h"
#include "_queue.h"
#include "spsc_queue.h"

namespace hshm::ipc {

/** Forward declaration of ticket_stack */
template<typename T>
class ticket_stack;

/**
 * MACROS used to simplify the ticket_stack namespace
 * Used as inputs to the SHM_CONTAINER_TEMPLATE
 * */
#define CLASS_NAME ticket_stack
#define TYPED_CLASS ticket_stack<T>
#define TYPED_HEADER ShmHeader<ticket_stack<T>>

/**
 * A MPMC queue for allocating tickets. Handles concurrency
 * without blocking.
 * */
template<typename T>
class ticket_stack : public ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE((CLASS_NAME), (TYPED_CLASS))
  ShmArchive<spsc_queue<T>> queue_;
  hshm::Mutex lock_;

 public:
  /**====================================
   * Default Constructor
   * ===================================*/

  /** SHM constructor. Default. */
  explicit ticket_stack(Allocator *alloc,
                        size_t depth = 1024) {
    shm_init_container(alloc);
    HSHM_MAKE_AR(queue_, GetAllocator(), depth);
    lock_.Init();
    SetNull();
  }

  /**====================================
   * Copy Constructors
   * ===================================*/

  /** SHM copy constructor */
  explicit ticket_stack(Allocator *alloc,
                        const ticket_stack &other) {
    shm_init_container(alloc);
    SetNull();
    shm_strong_copy_construct_and_op(other);
  }

  /** SHM copy assignment operator */
  ticket_stack& operator=(const ticket_stack &other) {
    if (this != &other) {
      shm_destroy();
      shm_strong_copy_construct_and_op(other);
    }
    return *this;
  }

  /** SHM copy constructor + operator main */
  void shm_strong_copy_construct_and_op(const ticket_stack &other) {
    (*queue_) = (*other.queue_);
  }

  /**====================================
   * Move Constructors
   * ===================================*/

  /** SHM move constructor. */
  ticket_stack(Allocator *alloc,
               ticket_stack &&other) noexcept {
    shm_init_container(alloc);
    if (GetAllocator() == other.GetAllocator()) {
      (*queue_) = std::move(*other.queue_);
      other.SetNull();
    } else {
      shm_strong_copy_construct_and_op(other);
      other.shm_destroy();
    }
  }

  /** SHM move assignment operator. */
  ticket_stack& operator=(ticket_stack &&other) noexcept {
    if (this != &other) {
      shm_destroy();
      if (GetAllocator() == other.GetAllocator()) {
        (*queue_) = std::move(*other.queue_);
        other.SetNull();
      } else {
        shm_strong_copy_construct_and_op(other);
        other.shm_destroy();
      }
    }
    return *this;
  }

  /**====================================
   * Destructor
   * ===================================*/

  /** SHM destructor.  */
  void shm_destroy_main() {
    (*queue_).shm_destroy();
  }

  /** Check if the list is empty */
  bool IsNull() const {
    return (*queue_).IsNull();
  }

  /** Sets this list as empty */
  void SetNull() {
  }

  /**====================================
   * ticket Queue Methods
   * ===================================*/

  /** Construct an element at \a pos position in the queue */
  template<typename ...Args>
  HSHM_ALWAYS_INLINE qtok_t emplace(T &tkt) {
    lock_.Lock(0);
    auto qtok = queue_->emplace(tkt);
    lock_.Unlock();
    return qtok;
  }

 public:
  /** Pop an element from the queue */
  HSHM_ALWAYS_INLINE qtok_t pop(T &tkt) {
    lock_.Lock(0);
    auto qtok = queue_->pop(tkt);
    lock_.Unlock();
    return qtok;
  }
};

}  // namespace hshm::ipc

#undef TYPED_HEADER
#undef TYPED_CLASS
#undef CLASS_NAME

#endif  // HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_IPC_TICKET_STACK_H_
