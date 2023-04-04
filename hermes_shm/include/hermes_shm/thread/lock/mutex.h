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

namespace hshm {

struct Mutex {
  std::atomic<uint32_t> lock_;
#ifdef HERMES_DEBUG_LOCK
  uint32_t owner_;
#endif

  /** Default constructor */
  Mutex() : lock_(0) {}

  /** Explicit initialization */
  void Init() {
    lock_ = 0;
  }

  /** Acquire lock */
  void Lock(uint32_t owner);

  /** Try to acquire the lock */
  bool TryLock(uint32_t owner);

  /** Unlock */
  void Unlock();
};

struct ScopedMutex {
  Mutex &lock_;
  bool is_locked_;

  /** Acquire the mutex */
  explicit ScopedMutex(Mutex &lock, uint32_t owner);

  /** Release the mutex */
  ~ScopedMutex();

  /** Explicitly acquire the mutex */
  void Lock(uint32_t owner);

  /** Explicitly try to lock the mutex */
  bool TryLock(uint32_t owner);

  /** Explicitly unlock the mutex */
  void Unlock();
};

}  // namespace hshm

#endif  // HERMES_THREAD_MUTEX_H_
