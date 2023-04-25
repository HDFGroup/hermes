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
#include "hermes_shm/thread/thread_model_manager.h"
#include "hermes_shm/util/logging.h"

namespace hshm {

/**====================================
 * Mutex
 * ===================================*/

/**
 * Acquire the mutex
 * */
void Mutex::Lock(uint32_t owner) {
  size_t count = 0;
  do {
#ifdef HERMES_DEBUG_LOCK
    if (count > US_TO_CLOCKS(1000000)) {
      HILOG(kDebug, "Taking a while");
      count = 5;
    }
#endif
    for (int i = 0; i < 1; ++i) {
      if (TryLock(owner)) { return; }
    }
    HERMES_THREAD_MODEL->Yield();
    ++count;
  } while (true);
}

/**
 * Attempt to acquire the mutex
 * */
bool Mutex::TryLock(uint32_t owner) {
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

/**
 * Release the mutex
 * */
void Mutex::Unlock() {
#ifdef HERMES_DEBUG_LOCK
  owner_ = 0;
#endif
  lock_.fetch_sub(1);
}

/**====================================
 * Scoped Mutex
 * ===================================*/

/**
 * Constructor
 * */
ScopedMutex::ScopedMutex(Mutex &lock, uint32_t owner)
: lock_(lock), is_locked_(false) {
  Lock(owner);
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
void ScopedMutex::Lock(uint32_t owner) {
  if (!is_locked_) {
    lock_.Lock(owner);
    is_locked_ = true;
  }
}

/**
 * Attempt to acquire the mutex
 * */
bool ScopedMutex::TryLock(uint32_t owner) {
  if (!is_locked_) {
    is_locked_ = lock_.TryLock(owner);
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

}  // namespace hshm
