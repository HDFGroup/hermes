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

namespace hermes_shm {

/**
 * Acquire the read lock
 * */
void RwLock::ReadLock() {
  bool ret = false;
  RwLockPayload expected, desired;
  auto thread_info = HERMES_THREAD_MANAGER->GetThreadStatic();
  do {
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
    thread_info->Yield();
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
  auto thread_info = HERMES_THREAD_MANAGER->GetThreadStatic();
  do {
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
    thread_info->Yield();
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
 * Verify the reference count for reads (for debugging)
 * */
void RwLock::assert_r_refcnt(int ref) {
  if (RwLockPayload(payload_).bits_.r_ != ref) {
    throw 1;
  }
}

/**
 * Verify the reference count for writes (for debugging)
 * */
void RwLock::assert_w_refcnt(int ref) {
  if (RwLockPayload(payload_).bits_.w_ > 1) {
    throw 1;
  }
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

}  // namespace hermes_shm
