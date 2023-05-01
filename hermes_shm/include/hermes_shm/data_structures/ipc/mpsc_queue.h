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

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_IPC_mpsc_queue_templ_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_IPC_mpsc_queue_templ_H_

#include "hermes_shm/data_structures/ipc/internal/shm_internal.h"
#include "hermes_shm/thread/lock.h"
#include "vector.h"
#include "pair.h"
#include "_queue.h"

namespace hshm::ipc {

/** Forward declaration of mpsc_queue_templ */
template<typename T, bool EXTENSIBLE>
class mpsc_queue_templ;

/**
 * MACROS used to simplify the mpsc_queue_templ namespace
 * Used as inputs to the SHM_CONTAINER_TEMPLATE
 * */
#define CLASS_NAME mpsc_queue_templ
#define TYPED_CLASS mpsc_queue_templ<T, EXTENSIBLE>
#define TYPED_HEADER ShmHeader<mpsc_queue_templ<T, EXTENSIBLE>>

/**
 * A queue optimized for multiple producers (emplace) with a single
 * consumer (pop).
 * */
template<typename T, bool EXTENSIBLE>
class mpsc_queue_templ : public ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE((CLASS_NAME), (TYPED_CLASS))
  ShmArchive<vector<pair<bitfield32_t, T>>> queue_;
  std::atomic<_qtok_t> tail_;
  std::atomic<_qtok_t> head_;
  RwLock lock_;

 public:
  /**====================================
   * Default Constructor
   * ===================================*/

  /** SHM constructor. Default. */
  explicit mpsc_queue_templ(Allocator *alloc,
                            size_t depth = 1024) {
    shm_init_container(alloc);
    HSHM_MAKE_AR(queue_, GetAllocator(), depth);
    SetNull();
  }

  /**====================================
   * Copy Constructors
   * ===================================*/

  /** SHM copy constructor */
  explicit mpsc_queue_templ(Allocator *alloc,
                            const mpsc_queue_templ &other) {
    shm_init_container(alloc);
    SetNull();
    shm_strong_copy_construct_and_op(other);
  }

  /** SHM copy assignment operator */
  mpsc_queue_templ& operator=(const mpsc_queue_templ &other) {
    if (this != &other) {
      shm_destroy();
      shm_strong_copy_construct_and_op(other);
    }
    return *this;
  }

  /** SHM copy constructor + operator main */
  void shm_strong_copy_construct_and_op(const mpsc_queue_templ &other) {
    head_ = other.head_.load();
    tail_ = other.tail_.load();
    (*queue_) = (*other.queue_);
  }

  /**====================================
   * Move Constructors
   * ===================================*/

  /** SHM move constructor. */
  mpsc_queue_templ(Allocator *alloc,
                   mpsc_queue_templ &&other) noexcept {
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
  mpsc_queue_templ& operator=(mpsc_queue_templ &&other) noexcept {
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

    // Check if there's space in the queue. Resize if necessary.
    if (size > (*queue_).size()) {
      if constexpr(EXTENSIBLE) {
        ScopedRwWriteLock resize_lock(lock_, 0);
        if (size > (*queue_).size()) {
          size_t old_size = (*queue_).size();
          size_t new_size = (RealNumber(5, 4) * (size + 64)).as_int();
          auto new_queue =
            hipc::make_uptr<vector<pair<bitfield32_t, T>>>(new_size);
          for (uint64_t i = 0; i < old_size; ++i) {
            _qtok_t i_old = (head + i) % old_size;
            _qtok_t i_new = (head + i) % new_size;
            (*new_queue)[i_new] = std::move((*queue_)[i_old]);
          }
          (*queue_) = std::move(*new_queue);
        }
      } else {
        while (1) {
          head = head_.load();
          size = tail - head + 1;
          if (size <= (*queue_).size()) {
            break;
          }
          HERMES_THREAD_MODEL->Yield();
        }
      }
    }

    // Emplace into queue at our slot
    if constexpr(EXTENSIBLE) {
      ScopedRwReadLock resize_lock(lock_, 0);
      _emplace(tail, std::forward<Args>(args)...);
    } else {
      _emplace(tail, std::forward<Args>(args)...);
    }
    return qtok_t(tail);
  }

 private:
  /** Emplace operation */
  template<typename ...Args>
  HSHM_ALWAYS_INLINE void _emplace(_qtok_t tail, Args&& ...args) {
    uint32_t idx = tail % (*queue_).size();
    auto iter = (*queue_).begin() + idx;
    (*queue_).replace(iter,
                    hshm::PiecewiseConstruct(),
                    make_argpack(),
                    make_argpack(std::forward<Args>(args)...));

    // Let pop know that the data is fully prepared
    pair<bitfield32_t, T> &entry = (*iter);
    entry.GetFirst().SetBits(1);
  }

 public:
  /** Consumer pops the head object */
  HSHM_ALWAYS_INLINE qtok_t pop(T &val) {
    if constexpr(EXTENSIBLE) {
      ScopedRwReadLock resize_lock(lock_, 0);
      return _pop(val);
    } else {
      return _pop(val);
    }
  }

  /** Pop operation */
  HSHM_ALWAYS_INLINE qtok_t _pop(T &val) {
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
};

template<typename T>
using mpsc_queue_ext = mpsc_queue_templ<T, true>;

template<typename T>
using mpsc_queue = mpsc_queue_templ<T, false>;

}  // namespace hshm::ipc

#undef CLASS_NAME
#undef TYPED_CLASS
#undef TYPED_HEADER

#endif  // HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_IPC_mpsc_queue_templ_H_
