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


#ifndef HERMES_THREAD_MUTEX_H_
#define HERMES_THREAD_MUTEX_H_

#include <atomic>
#include "hermes_shm/thread/lock.h"
#include "hermes_shm/thread/thread_model_manager.h"

namespace hshm {

struct Mutex {
  std::atomic<uint32_t> lock_;
#ifdef HERMES_DEBUG_LOCK
  uint32_t owner_;
#endif

  /** Default constructor */
  HSHM_ALWAYS_INLINE Mutex() : lock_(0) {}

  /** Explicit initialization */
  HSHM_ALWAYS_INLINE void Init() {
    lock_ = 0;
  }

  /** Acquire lock */
  HSHM_ALWAYS_INLINE void Lock(uint32_t owner) {
    do {
      for (int i = 0; i < 1; ++i) {
        if (TryLock(owner)) { return; }
      }
      HERMES_THREAD_MODEL->Yield();
    } while (true);
  }

  /** Try to acquire the lock */
  HSHM_ALWAYS_INLINE bool TryLock(uint32_t owner) {
    if (lock_.load() != 0) {
      return false;
    }
    uint32_t tkt = lock_.fetch_add(1);
    if (tkt != 0) {
      lock_.fetch_sub(1);
      return false;
    }
#ifdef HERMES_DEBUG_LOCK
    owner_ = owner;
#endif
    return true;
  }

  /** Unlock */
  HSHM_ALWAYS_INLINE void Unlock() {
#ifdef HERMES_DEBUG_LOCK
    owner_ = 0;
#endif
    lock_.fetch_sub(1);
  }
};

struct ScopedMutex {
  Mutex &lock_;
  bool is_locked_;

  /** Acquire the mutex */
  HSHM_ALWAYS_INLINE explicit ScopedMutex(Mutex &lock, uint32_t owner)
  : lock_(lock), is_locked_(false) {
    Lock(owner);
  }

  /** Release the mutex */
  HSHM_ALWAYS_INLINE ~ScopedMutex() {
    Unlock();
  }

  /** Explicitly acquire the mutex */
  HSHM_ALWAYS_INLINE void Lock(uint32_t owner) {
    if (!is_locked_) {
      lock_.Lock(owner);
      is_locked_ = true;
    }
  }

  /** Explicitly try to lock the mutex */
  HSHM_ALWAYS_INLINE bool TryLock(uint32_t owner) {
    if (!is_locked_) {
      is_locked_ = lock_.TryLock(owner);
    }
    return is_locked_;
  }

  /** Explicitly unlock the mutex */
  HSHM_ALWAYS_INLINE void Unlock() {
    if (is_locked_) {
      lock_.Unlock();
      is_locked_ = false;
    }
  }
};

}  // namespace hshm

#endif  // HERMES_THREAD_MUTEX_H_
