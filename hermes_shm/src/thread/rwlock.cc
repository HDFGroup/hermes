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


#include "hermes_shm/thread/lock/rwlock.h"
#include "hermes_shm/thread/thread_model_manager.h"
#include "hermes_shm/util/logging.h"

namespace hshm {

/**====================================
 * Rw Lock
 * ===================================*/

/**
 * Acquire the read lock
 * */
void RwLock::ReadLock(uint32_t owner) {
  bool ret = false;
  RwLockPayload expected, desired;
  size_t count = 0;
  do {
    if (count > US_TO_CLOCKS(1000000)) {
      HILOG(kDebug, "Taking a while");
      count = 5;
    }
    for (int i = 0; i < 1; ++i) {
      expected.as_int_ = payload_.load();
      if (expected.IsWriteLocked()) {
        continue;
      }
      desired = expected;
      desired.bits_.r_ += 1;
      ret = payload_.compare_exchange_weak(
        expected.as_int_,
        desired.as_int_);
      if (ret) {
#ifdef HERMES_DEBUG_LOCK
        owner_ = owner;
        if (owner == 10) {
          HILOG(kDebug, "Locking kBORG_LocalProcessFlushes");
        }
#endif
        return;
      }
    }
    HERMES_THREAD_MODEL->Yield();
    ++count;
  } while (true);
}

/**
 * Release the read lock
 * */
void RwLock::ReadUnlock() {
  bool ret;
  RwLockPayload expected, desired;
#ifdef HERMES_DEBUG_LOCK
  if (owner_ == 10) {
    HILOG(kDebug, "Unlocking kBORG_LocalProcessFlushes");
  }
  owner_ = 0;
#endif
  do {
    expected.as_int_ = payload_.load();
    desired = expected;
    desired.bits_.r_ -= 1;
    ret = payload_.compare_exchange_weak(
      expected.as_int_,
      desired.as_int_);
  } while (!ret);
}

/**
 * Acquire the write lock
 * */
void RwLock::WriteLock(uint32_t owner) {
  bool ret = false;
  RwLockPayload expected, desired;
  size_t count = 0;
  do {
    if (count > US_TO_CLOCKS(1000000)) {
      HILOG(kDebug, "Taking a while");
      count = 5;
    }
    for (int i = 0; i < 1; ++i) {
      expected.as_int_ = payload_.load();
      if (expected.IsReadLocked() || expected.IsWriteLocked()) {
        continue;
      }
      desired = expected;
      desired.bits_.w_ += 1;
      ret = payload_.compare_exchange_weak(
        expected.as_int_,
        desired.as_int_);
      if (ret) {
#ifdef HERMES_DEBUG_LOCK
        owner_ = owner;
        if (owner == 10) {
          HILOG(kDebug, "Locking kBORG_LocalProcessFlushes");
        }
#endif
        return;
      }
    }

    if (count < 5) {
      HERMES_THREAD_MODEL->Yield();
    } else {
      usleep(100);
    }
    ++count;
  } while (true);
}

/**
 * Release the write lock
 * */
void RwLock::WriteUnlock() {
  bool ret;
  RwLockPayload expected, desired;
#ifdef HERMES_DEBUG_LOCK
  if (owner_ == 10) {
    HILOG(kDebug, "Unlocking kBORG_LocalProcessFlushes");
  }
  owner_ = 0;
#endif
  do {
    expected.as_int_ = payload_.load();
    desired = expected;
    desired.bits_.w_ -= 1;
    ret = payload_.compare_exchange_weak(
      expected.as_int_,
      desired.as_int_);
  } while (!ret);
}

/**====================================
 * ScopedRwReadLock
 * ===================================*/

/**
 * Constructor
 * */
ScopedRwReadLock::ScopedRwReadLock(RwLock &lock, uint32_t owner)
: lock_(lock), is_locked_(false) {
  Lock(owner);
}

/**
 * Release the read lock
 * */
ScopedRwReadLock::~ScopedRwReadLock() {
  Unlock();
}

/**
 * Acquire the read lock
 * */
void ScopedRwReadLock::Lock(uint32_t owner) {
  if (!is_locked_) {
    lock_.ReadLock(owner);
    is_locked_ = true;
  }
}

/**
 * Release the read lock
 * */
void ScopedRwReadLock::Unlock() {
  if (is_locked_) {
    lock_.ReadUnlock();
    is_locked_ = false;
  }
}

/**====================================
 * ScopedRwWriteLock
 * ===================================*/

/**
 * Constructor
 * */
ScopedRwWriteLock::ScopedRwWriteLock(RwLock &lock, uint32_t owner)
: lock_(lock), is_locked_(false) {
  Lock(owner);
}

/**
 * Release the write lock
 * */
ScopedRwWriteLock::~ScopedRwWriteLock() {
  Unlock();
}

/**
 * Acquire the write lock
 * */
void ScopedRwWriteLock::Lock(uint32_t owner) {
  if (!is_locked_) {
    lock_.WriteLock(owner);
    is_locked_ = true;
  }
}

/**
 * Release the write lock
 * */
void ScopedRwWriteLock::Unlock() {
  if (is_locked_) {
    lock_.WriteUnlock();
    is_locked_ = false;
  }
}

}  // namespace hshm
