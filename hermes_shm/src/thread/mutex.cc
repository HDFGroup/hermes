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

#include "hermes_shm/thread/lock.h"
#include "hermes_shm/thread/thread_manager.h"

namespace hermes_shm {

/**
 * Acquire the mutex
 * */
void Mutex::Lock() {
  auto thread_info = HERMES_SHM_THREAD_MANAGER->GetThreadStatic();
  while (!TryLock()) {
    thread_info->Yield();
  }
}

/**
 * Attempt to acquire the mutex
 * */
bool Mutex::TryLock() {
  if (lock_.load() != 0) return false;
  uint32_t tkt = lock_.fetch_add(1);
  if (tkt != 0) {
    lock_.fetch_sub(1);
    return false;
  }
  return true;
}

/**
 * Release the mutex
 * */
void Mutex::Unlock() {
  lock_.fetch_sub(1);
}

/**
 * SCOPED MUTEX
 * */

/**
 * Constructor
 * */
ScopedMutex::ScopedMutex(Mutex &lock)
: lock_(lock), is_locked_(false) {
}

/**
 * Release the mutex
 * */
ScopedMutex::~ScopedMutex() {
  Unlock();
}

/**
 * Acquire the mutex
 * */
void ScopedMutex::Lock() {
  if (!is_locked_) {
    lock_.Lock();
    is_locked_ = true;
  }
}

/**
 * Attempt to acquire the mutex
 * */
bool ScopedMutex::TryLock() {
  if (!is_locked_) {
    is_locked_ = lock_.TryLock();
  }
  return is_locked_;
}

/**
 * Acquire the mutex
 * */
void ScopedMutex::Unlock() {
  if (is_locked_) {
    lock_.Unlock();
    is_locked_ = false;
  }
}

}  // namespace hermes_shm
