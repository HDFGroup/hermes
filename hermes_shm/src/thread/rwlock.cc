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
#include "hermes_shm/thread/thread_manager.h"
#include "hermes_shm/util/logging.h"

namespace hshm {

/**
 * Acquire the read lock
 * */
void RwLock::ReadLock() {
  bool ret = false;
  RwLockPayload expected, desired;
  size_t count = 0;
  auto thread_info = HSHM_THREAD_MANAGER->GetThreadStatic();
  do {
    if (count > 500) {
      HILOG(kDebug, "Taking a while");
      count = 5;
    }
    for (int i = 0; i < US_TO_CLOCKS(8); ++i) {
      expected.as_int_ = payload_.load();
      if (expected.IsWriteLocked()) {
        continue;
      }
      desired = expected;
      desired.bits_.r_ += 1;
      ret = payload_.compare_exchange_weak(
        expected.as_int_,
        desired.as_int_);
      if (ret) { return; }
    }

    if (count < 5) {
      thread_info->Yield();
    } else {
      usleep(100);
    }
    ++count;
  } while (true);
}

/**
 * Release the read lock
 * */
void RwLock::ReadUnlock() {
  bool ret;
  RwLockPayload expected, desired;
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
void RwLock::WriteLock() {
  bool ret = false;
  RwLockPayload expected, desired;
  size_t count = 0;
  auto thread_info = HSHM_THREAD_MANAGER->GetThreadStatic();
  do {
    if (count > 500) {
      HILOG(kDebug, "Taking a while");
      count = 5;
    }
    for (int i = 0; i < US_TO_CLOCKS(8); ++i) {
      expected.as_int_ = payload_.load();
      if (expected.IsReadLocked() || expected.IsWriteLocked()) {
        continue;
      }
      desired = expected;
      desired.bits_.w_ += 1;
      ret = payload_.compare_exchange_weak(
        expected.as_int_,
        desired.as_int_);
      if (ret) { return; }
    }

    if (count < 5) {
      thread_info->Yield();
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
  do {
    expected.as_int_ = payload_.load();
    desired = expected;
    desired.bits_.w_ -= 1;
    ret = payload_.compare_exchange_weak(
      expected.as_int_,
      desired.as_int_);
  } while (!ret);
}

/**
 * SCOPED R/W READ LOCK
 * */

/**
 * Constructor
 * */
ScopedRwReadLock::ScopedRwReadLock(RwLock &lock)
: lock_(lock), is_locked_(false) {
  Lock();
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
void ScopedRwReadLock::Lock() {
  if (!is_locked_) {
    lock_.ReadLock();
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

/**
 * SCOPED R/W WRITE LOCK
 * */

/**
 * Constructor
 * */
ScopedRwWriteLock::ScopedRwWriteLock(RwLock &lock)
: lock_(lock), is_locked_(false) {
  Lock();
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
void ScopedRwWriteLock::Lock() {
  if (!is_locked_) {
    lock_.WriteLock();
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
