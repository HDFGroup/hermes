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

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_THREAD_LOCK_RWQUEUE_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_THREAD_LOCK_RWQUEUE_H_

#include "hermes_shm/data_structures/ipc/internal/shm_internal.h"
#include "hermes_shm/thread/lock.h"
#include "mpsc_queue.h"

namespace hshm::ipc {

/** Forward declaration of rwqueue */
template<typename T>
class rwqueue;

/**
 * MACROS used to simplify the rwqueue namespace
 * Used as inputs to the SHM_CONTAINER_TEMPLATE
 * */
#define CLASS_NAME rwqueue
#define TYPED_CLASS rwqueue<T>
#define TYPED_HEADER ShmHeader<rwqueue<T>>

/**
 * The rwqueue shared-memory header
 * */
template<typename T>
struct ShmHeader<rwqueue<T>> {
  SHM_CONTAINER_HEADER_TEMPLATE(ShmHeader)
  ShmArchive<mpsc_queue<uint64_t>> reads_;
  ShmArchive<mpsc_queue<uint64_t>> writes_;
  std::atomic<uint64_t> ticket_;

  /** Strong copy operation */
  void strong_copy(const ShmHeader &other) {
    ticket_ = other.ticket.load();
  }
};

/**
 * A queue designed for synchronizing reads and writes to a data structure.
 * */
template<typename T>
class rwqueue : public ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE((CLASS_NAME), (TYPED_CLASS), (TYPED_HEADER))
  Ref<vector<pair<bitfield32_t, T>>> queue_;

 public:
  /**====================================
   * Default Constructor
   * ===================================*/

  /** SHM constructor. Default. */
  explicit rwqueue(TYPED_HEADER *header, Allocator *alloc,
                      size_t depth = 1024) {
    shm_init_header(header, alloc);
    queue_ = make_ref<vector<pair<bitfield32_t, T>>>(header_->queue_,
                                                     alloc_,
                                                     depth);
    SetNull();
  }

  /**====================================
   * Copy Constructors
   * ===================================*/

  /** SHM copy constructor */
  explicit rwqueue(TYPED_HEADER *header, Allocator *alloc,
                      const rwqueue &other) {
    shm_init_header(header, alloc);
    SetNull();
    shm_strong_copy_construct_and_op(other);
  }

  /** SHM copy assignment operator */
  rwqueue& operator=(const rwqueue &other) {
    if (this != &other) {
      shm_destroy();
      shm_strong_copy_construct_and_op(other);
    }
    return *this;
  }

  /** SHM copy constructor + operator main */
  void shm_strong_copy_construct_and_op(const rwqueue &other) {
    (*header_) = *(other.header_);
    (*queue_) = (*other.queue_);
  }

  /**====================================
   * Move Constructors
   * ===================================*/

  /** SHM move constructor. */
  rwqueue(TYPED_HEADER *header, Allocator *alloc,
             rwqueue &&other) noexcept {
    shm_init_header(header, alloc);
    if (alloc_ == other.alloc_) {
      (*header_) = std::move(*other.header_);
      (*queue_) = std::move(*other.queue_);
      other.SetNull();
    } else {
      shm_strong_copy_construct_and_op(other);
      other.shm_destroy();
    }
  }

  /** SHM move assignment operator. */
  rwqueue& operator=(rwqueue &&other) noexcept {
    if (this != &other) {
      shm_destroy();
      if (alloc_ == other.alloc_) {
        (*header_) = std::move(*other.header_);
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
    queue_->shm_destroy();
  }

  /** Check if the list is empty */
  bool IsNull() const {
    return queue_->IsNull();
  }

  /** Sets this list as empty */
  void SetNull() {
    header_->head_ = 0;
    header_->tail_ = 0;
  }

  /**====================================
   * SHM Deserialization
   * ===================================*/

  /** Load from shared memory */
  void shm_deserialize_main() {
    queue_ = Ref<vector<pair<bitfield32_t, T>>>(header_->queue_,
                                                alloc_);
  }

  /**====================================
   * MPSC Queue Methods
   * ===================================*/

  /** Construct an element at \a pos position in the list */
  template<typename ...Args>
  void emplace(Args&&... args) {
    // Allocate a slot in the queue
    // The slot is marked NULL, so pop won't do anything if context switch
    uint64_t head = header_->head_.load();
    uint64_t tail = header_->tail_.fetch_add(1);
    uint64_t size = tail - head + 1;

    // Check if there's space in the queue. Resize if necessary.
    if (size > queue_->size()) {
      ScopedRwWriteLock resize_lock(header_->lock_, 0);
      if (size > queue_->size()) {
        uint64_t old_size = queue_->size();
        uint64_t new_size = (RealNumber(5, 4) * (size + 64)).as_int();
        auto new_queue =
          hipc::make_uptr<vector<pair<bitfield32_t, T>>>(new_size);
        for (uint64_t i = 0; i < old_size; ++i) {
          uint64_t i_old = (head + i) % old_size;
          uint64_t i_new = (head + i) % new_size;
          (*(*new_queue)[i_new]) = std::move(*(*queue_)[i_old]);
        }
        (*queue_) = std::move(*new_queue);
      }
    }

    // Emplace into queue at our slot
    ScopedRwReadLock resize_lock(header_->lock_, 0);
    uint32_t idx = tail % queue_->size();
    auto iter = queue_->begin() + idx;
    queue_->replace(iter,
                    hshm::PiecewiseConstruct(),
                    make_argpack(),
                    make_argpack(std::forward<Args>(args)...));

    // Let pop know that the data is fully prepared
    Ref<pair<bitfield32_t, T>> entry = (*iter);
    entry->first_->SetBits(1);
  }

  /** Consumer pops the head object */
  bool pop(Ref<T> &val) {
    ScopedRwReadLock resize_lock(header_->lock_, 0);

    // Don't pop if there's no entries
    uint64_t head = header_->head_.load();
    uint64_t tail = header_->tail_.load();
    uint64_t size = tail - head;
    if (size == 0) {
      return false;
    }

    // Pop the element, but only if it's marked valid
    uint64_t idx = head % queue_->size();
    hipc::Ref<hipc::pair<bitfield32_t, T>> entry = (*queue_)[idx];
    if (entry->first_->Any(1)) {
      (*val) = std::move(*entry->second_);
      entry->first_->Clear();
      header_->head_.fetch_add(1);
      return true;
    } else {
      return false;
    }
  }
};

}  // namespace hshm::ipc

#undef CLASS_NAME
#undef TYPED_CLASS
#undef TYPED_HEADER

#endif  // HERMES_SHM_INCLUDE_HERMES_SHM_THREAD_LOCK_RWQUEUE_H_
