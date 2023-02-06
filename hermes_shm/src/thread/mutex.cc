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
