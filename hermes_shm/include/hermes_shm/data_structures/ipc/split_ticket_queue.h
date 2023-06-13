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

#ifndef HERMES_SHM__DATA_STRUCTURES_IPC_SPLIT_TICKET_QUEUE_H_
#define HERMES_SHM__DATA_STRUCTURES_IPC_SPLIT_TICKET_QUEUE_H_

#include "hermes_shm/data_structures/ipc/internal/shm_internal.h"
#include "hermes_shm/thread/lock.h"
#include "vector.h"
#include "ticket_queue.h"

namespace hshm::ipc {

/** Forward declaration of split_ticket_queue */
template<typename T>
class split_ticket_queue;

/**
 * MACROS used to simplify the split_ticket_queue namespace
 * Used as inputs to the SHM_CONTAINER_TEMPLATE
 * */
#define CLASS_NAME split_ticket_queue
#define TYPED_CLASS split_ticket_queue<T>
#define TYPED_HEADER ShmHeader<split_ticket_queue<T>>

/**
 * A MPMC queue for allocating tickets. Handles concurrency
 * without blocking.
 * */
template<typename T>
class split_ticket_queue : public ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE((CLASS_NAME), (TYPED_CLASS))
  ShmArchive<vector<ticket_queue<T>>> splits_;
  std::atomic<uint16_t> rr_tail_, rr_head_;

 public:
  /**====================================
   * Default Constructor
   * ===================================*/

  /** SHM constructor. Default. */
  explicit split_ticket_queue(Allocator *alloc,
                              size_t depth_per_split = 1024,
                              size_t split = 0) {
    shm_init_container(alloc);
    if (split == 0) {
      split = HERMES_SYSTEM_INFO->ncpu_;
    }
    HSHM_MAKE_AR(splits_, GetAllocator(), split, depth_per_split);
    SetNull();
  }

  /**====================================
   * Copy Constructors
   * ===================================*/

  /** SHM copy constructor */
  explicit split_ticket_queue(Allocator *alloc,
                              const split_ticket_queue &other) {
    shm_init_container(alloc);
    SetNull();
    shm_strong_copy_construct_and_op(other);
  }

  /** SHM copy assignment operator */
  split_ticket_queue& operator=(const split_ticket_queue &other) {
    if (this != &other) {
      shm_destroy();
      shm_strong_copy_construct_and_op(other);
    }
    return *this;
  }

  /** SHM copy constructor + operator main */
  void shm_strong_copy_construct_and_op(const split_ticket_queue &other) {
    (*splits_) = (*other.splits_);
  }

  /**====================================
   * Move Constructors
   * ===================================*/

  /** SHM move constructor. */
  split_ticket_queue(Allocator *alloc,
                     split_ticket_queue &&other) noexcept {
    shm_init_container(alloc);
    if (GetAllocator() == other.GetAllocator()) {
      (*splits_) = std::move(*other.splits_);
      other.SetNull();
    } else {
      shm_strong_copy_construct_and_op(other);
      other.shm_destroy();
    }
  }

  /** SHM move assignment operator. */
  split_ticket_queue& operator=(split_ticket_queue &&other) noexcept {
    if (this != &other) {
      shm_destroy();
      if (GetAllocator() == other.GetAllocator()) {
        (*splits_) = std::move(*other.splits_);
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
    (*splits_).shm_destroy();
  }

  /** Check if the list is empty */
  bool IsNull() const {
    return (*splits_).IsNull();
  }

  /** Sets this list as empty */
  void SetNull() {
    rr_tail_ = 0;
    rr_head_ = 0;
  }

  /**====================================
   * ticket Queue Methods
   * ===================================*/

  /** Construct an element at \a pos position in the queue */
  template<typename ...Args>
  qtok_t emplace(T &tkt) {
    uint16_t rr = rr_tail_.fetch_add(1);
    auto &splits = (*splits_);
    size_t num_splits = splits.size();
    uint16_t qid_start = rr % num_splits;
    for (size_t i = 0; i < num_splits; ++i) {
      uint32_t qid = (qid_start + i) % num_splits;
      ticket_queue<T> &queue = (*splits_)[qid];
      qtok_t qtok = queue.emplace(tkt);
      if (!qtok.IsNull()) {
        return qtok;
      }
    }
    return qtok_t::GetNull();
  }

 public:
  /** Pop an element from the queue */
  qtok_t pop(T &tkt) {
    uint16_t rr = rr_head_.fetch_add(1);
    auto &splits = (*splits_);
    size_t num_splits = splits.size();
    uint16_t qid_start = rr % num_splits;
    for (size_t i = 0; i < num_splits; ++i) {
      uint32_t qid = (qid_start + i) % num_splits;
      ticket_queue<T> &queue = (*splits_)[qid];
      qtok_t qtok = queue.pop(tkt);
      if (!qtok.IsNull()) {
        return qtok;
      }
    }
    return qtok_t::GetNull();
  }
};

}  // namespace hshm::ipc

#undef TYPED_HEADER
#undef TYPED_CLASS
#undef CLASS_NAME

#endif  // HERMES_SHM__DATA_STRUCTURES_IPC_SPLIT_TICKET_QUEUE_H_
