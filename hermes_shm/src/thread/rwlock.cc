/*
 * Copyright (C) 2022  SCS Lab <scslab@iit.edu>,
 * Luke Logan <llogan@hawk.iit.edu>,
 * Jaime Cernuda Garcia <jcernudagarcia@hawk.iit.edu>
 * Jay Lofstead <gflofst@sandia.gov>,
 * Anthony Kougkas <akougkas@iit.edu>,
 * Xian-He Sun <sun@iit.edu>
 *
 * This file is part of HermesShm
 *
 * HermesShm is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */


#include "hermes_shm/thread/lock/rwlock.h"
#include "hermes_shm/thread/thread_manager.h"

namespace hermes_shm {

/**
 * Acquire the read lock
 * */
void RwLock::ReadLock() {
  bool ret = false;
  RwLockPayload expected, desired;
  auto thread_info = HERMES_SHM_THREAD_MANAGER->GetThreadStatic();
  do {
    expected.as_int_ = payload_.load();
    if (expected.IsWriteLocked()) {
      thread_info->Yield();
      continue;
    }
    desired = expected;
    desired.bits_.r_ += 1;
    ret = payload_.compare_exchange_weak(
      expected.as_int_,
      desired.as_int_);
  } while (!ret);
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
  auto thread_info = HERMES_SHM_THREAD_MANAGER->GetThreadStatic();
  do {
    expected.as_int_ = payload_.load();
    if (expected.IsReadLocked()) {
      thread_info->Yield();
      continue;
    }
    if (expected.IsWriteLocked()) {
      thread_info->Yield();
      continue;
    }
    desired = expected;
    desired.bits_.w_ += 1;
    ret = payload_.compare_exchange_weak(
      expected.as_int_,
      desired.as_int_);
  } while (!ret);
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
