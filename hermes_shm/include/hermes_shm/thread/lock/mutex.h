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
