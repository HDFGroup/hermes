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

#ifndef HERMES_SHM_THREAD_MUTEX_H_
#define HERMES_SHM_THREAD_MUTEX_H_

#include <atomic>

namespace hermes_shm {

struct Mutex {
  std::atomic<uint32_t> lock_;

  Mutex() : lock_(0) {}

  void Init() {
    lock_ = 0;
  }
  void Lock();
  bool TryLock();
  void Unlock();
};

struct ScopedMutex {
  Mutex &lock_;
  bool is_locked_;

  explicit ScopedMutex(Mutex &lock);
  ~ScopedMutex();

  void Lock();
  bool TryLock();
  void Unlock();
};

}  // namespace hermes_shm

#endif  // HERMES_SHM_THREAD_MUTEX_H_
