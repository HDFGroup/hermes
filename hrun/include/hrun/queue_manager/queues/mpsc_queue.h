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

#ifndef HRUN_INCLUDE_HRUN_DATA_STRUCTURES_IPC_mpsc_queue_H_
#define HRUN_INCLUDE_HRUN_DATA_STRUCTURES_IPC_mpsc_queue_H_

#include "hermes_shm/data_structures/ipc/internal/shm_internal.h"
#include "hermes_shm/thread/lock.h"
#include "hermes_shm/data_structures/ipc/vector.h"
#include "hermes_shm/data_structures/ipc/pair.h"
#include "hermes_shm/types/qtok.h"
#include "hrun/hrun_types.h"

namespace hrun {

/** Forward declaration of mpsc_queue */
template<typename T>
class mpsc_queue;

/**
 * MACROS used to simplify the mpsc_queue namespace
 * Used as inputs to the SHM_CONTAINER_TEMPLATE
 * */
#define CLASS_NAME mpsc_queue
#define TYPED_CLASS mpsc_queue<T>
#define TYPED_HEADER ShmHeader<mpsc_queue<T>>

using hipc::ShmContainer;
using hipc::pair;
using hshm::_qtok_t;
using hshm::qtok_t;
using hipc::vector;
using hipc::ShmArchive;
using hipc::Allocator;
using hshm::bitfield32_t;
using hshm::make_argpack;

/**
 * A queue optimized for multiple producers (emplace) with a single
 * consumer (pop).
 * */
template<typename T>
class mpsc_queue : public ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE((CLASS_NAME), (TYPED_CLASS))
  ShmArchive<vector<pair<bitfield32_t, T>>> queue_;
  std::atomic<_qtok_t> tail_;
  std::atomic<_qtok_t> head_;
  bitfield32_t flags_;
  QueueId id_;

 public:
  /**====================================
   * Default Constructor
   * ===================================*/

  /** SHM constructor. Default. */
  explicit mpsc_queue(Allocator *alloc,
                      size_t depth = 1024,
                      QueueId id = QueueId::GetNull()) {
    shm_init_container(alloc);
    HSHM_MAKE_AR(queue_, GetAllocator(), depth);
    flags_.Clear();
    id_ = id;
    SetNull();
  }

  /**====================================
   * Copy Constructors
   * ===================================*/

  /** SHM copy constructor */
  explicit mpsc_queue(Allocator *alloc,
                      const mpsc_queue &other) {
    shm_init_container(alloc);
    SetNull();
    shm_strong_copy_construct_and_op(other);
  }

  /** SHM copy assignment operator */
  mpsc_queue& operator=(const mpsc_queue &other) {
    if (this != &other) {
      shm_destroy();
      shm_strong_copy_construct_and_op(other);
    }
    return *this;
  }

  /** SHM copy constructor + operator main */
  void shm_strong_copy_construct_and_op(const mpsc_queue &other) {
    head_ = other.head_.load();
    tail_ = other.tail_.load();
    (*queue_) = (*other.queue_);
  }

  /**====================================
   * Move Constructors
   * ===================================*/

  /** SHM move constructor. */
  mpsc_queue(Allocator *alloc,
             mpsc_queue &&other) noexcept {
    shm_init_container(alloc);
    if (GetAllocator() == other.GetAllocator()) {
      head_ = other.head_.load();
      tail_ = other.tail_.load();
      (*queue_) = std::move(*other.queue_);
      other.SetNull();
    } else {
      shm_strong_copy_construct_and_op(other);
      other.shm_destroy();
    }
  }

  /** SHM move assignment operator. */
  mpsc_queue& operator=(mpsc_queue &&other) noexcept {
    if (this != &other) {
      shm_destroy();
      if (GetAllocator() == other.GetAllocator()) {
        head_ = other.head_.load();
        tail_ = other.tail_.load();
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
    head_ = 0;
    tail_ = 0;
  }

  /**====================================
   * MPSC Queue Methods
   * ===================================*/

  /** Construct an element at \a pos position in the list */
  template<typename ...Args>
  qtok_t emplace(Args&&... args) {
    // Allocate a slot in the queue
    // The slot is marked NULL, so pop won't do anything if context switch
    _qtok_t head = head_.load();
    _qtok_t tail = tail_.fetch_add(1);
    size_t size = tail - head + 1;
    vector<pair<bitfield32_t, T>> &queue = (*queue_);

    // Check if there's space in the queue.
    if (size > queue.size()) {
      HILOG(kInfo, "Queue {}/{} is full, waiting for space", id_, queue_->size());
      while (true) {
        head = head_.load();
        size = tail - head + 1;
        if (size <= (*queue_).size()) {
          break;
        }
        HERMES_THREAD_MODEL->Yield();
      }
    }

    // Emplace into queue at our slot
    uint32_t idx = tail % queue.size();
    auto iter = queue.begin() + idx;
    queue.replace(iter,
                      hshm::PiecewiseConstruct(),
                      make_argpack(),
                      make_argpack(std::forward<Args>(args)...));

    // Let pop know that the data is fully prepared
    pair<bitfield32_t, T> &entry = (*iter);
    entry.GetFirst().SetBits(1);
    return qtok_t(tail);
  }

 public:
  /** Consumer pops the head object */
  qtok_t pop(T &val) {
    // Don't pop if there's no entries
    _qtok_t head = head_.load();
    _qtok_t tail = tail_.load();
    if (head >= tail) {
      return qtok_t::GetNull();
    }

    // Pop the element, but only if it's marked valid
    _qtok_t idx = head % (*queue_).size();
    hipc::pair<bitfield32_t, T> &entry = (*queue_)[idx];
    if (entry.GetFirst().Any(1)) {
      val = std::move(entry.GetSecond());
      entry.GetFirst().Clear();
      head_.fetch_add(1);
      return qtok_t(head);
    } else {
      return qtok_t::GetNull();
    }
  }

  /** Consumer pops the head object */
  qtok_t pop() {
    // Don't pop if there's no entries
    _qtok_t head = head_.load();
    _qtok_t tail = tail_.load();
    if (head >= tail) {
      return qtok_t::GetNull();
    }

    // Pop the element, but only if it's marked valid
    _qtok_t idx = head % (*queue_).size();
    hipc::pair<bitfield32_t, T> &entry = (*queue_)[idx];
    if (entry.GetFirst().Any(1)) {
      entry.GetFirst().Clear();
      head_.fetch_add(1);
      return qtok_t(head);
    } else {
      return qtok_t::GetNull();
    }
  }

  /** Consumer peeks an object */
  qtok_t peek(T *&val, int off = 0) {
    // Don't pop if there's no entries
    _qtok_t head = head_.load() + off;
    _qtok_t tail = tail_.load();
    if (head >= tail) {
      return qtok_t::GetNull();
    }

    // Pop the element, but only if it's marked valid
    _qtok_t idx = (head) % (*queue_).size();
    hipc::pair<bitfield32_t, T> &entry = (*queue_)[idx];
    if (entry.GetFirst().Any(1)) {
      val = &entry.GetSecond();
      return qtok_t(head);
    } else {
      return qtok_t::GetNull();
    }
  }

  /** Consumer peeks an object */
  qtok_t peek(pair<bitfield32_t, T> *&val, int off = 0) {
    // Don't pop if there's no entries
    _qtok_t head = head_.load() + off;
    _qtok_t tail = tail_.load();
    if (head >= tail) {
      return qtok_t::GetNull();
    }

    // Pop the element, but only if it's marked valid
    _qtok_t idx = (head) % (*queue_).size();
    hipc::pair<bitfield32_t, T> &entry = (*queue_)[idx];
    if (entry.GetFirst().Any(1)) {
      val = &entry;
      return qtok_t(head);
    } else {
      return qtok_t::GetNull();
    }
  }
};

}  // namespace hshm::ipc

#undef CLASS_NAME
#undef TYPED_CLASS
#undef TYPED_HEADER

#endif  // HRUN_INCLUDE_HRUN_DATA_STRUCTURES_IPC_mpsc_queue_H_
