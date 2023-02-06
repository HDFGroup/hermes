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


#ifndef HERMES_SHM_THREAD_RWLOCK_H_
#define HERMES_SHM_THREAD_RWLOCK_H_

#include <atomic>

namespace hermes_shm {

union RwLockPayload {
  struct {
    uint32_t r_;
    uint32_t w_;
  } bits_;
  uint64_t as_int_;

  RwLockPayload() = default;

  RwLockPayload(const RwLockPayload &other) {
    as_int_ = other.as_int_;
  }

  RwLockPayload(uint64_t other) {
    as_int_ = other;
  }

  bool IsWriteLocked() const {
    return bits_.w_ > 0;
  }

  bool IsReadLocked() const {
    return bits_.r_ > 0;
  }
};

struct RwLock {
  std::atomic<uint64_t> payload_;

  RwLock() : payload_(0) {}

  void Init() {
    payload_ = 0;
  }

  RwLock(const RwLock &other) = delete;

  RwLock(RwLock &&other) noexcept
  : payload_(other.payload_.load()) {}

  RwLock& operator=(RwLock &&other) {
    payload_ = other.payload_.load();
    return (*this);
  }

  void ReadLock();
  void ReadUnlock();

  void WriteLock();
  void WriteUnlock();

  void assert_r_refcnt(int ref);
  void assert_w_refcnt(int ref);
};

struct ScopedRwReadLock {
  RwLock &lock_;
  bool is_locked_;

  explicit ScopedRwReadLock(RwLock &lock);
  ~ScopedRwReadLock();

  void Lock();
  void Unlock();
};

struct ScopedRwWriteLock {
  RwLock &lock_;
  bool is_locked_;

  explicit ScopedRwWriteLock(RwLock &lock);
  ~ScopedRwWriteLock();

  void Lock();
  void Unlock();
};

}  // namespace hermes_shm

#endif  // HERMES_SHM_THREAD_RWLOCK_H_
