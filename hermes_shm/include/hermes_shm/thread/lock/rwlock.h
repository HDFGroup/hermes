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


#ifndef HERMES_THREAD_RWLOCK_H_
#define HERMES_THREAD_RWLOCK_H_

#include <atomic>

namespace hshm {

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

}  // namespace hshm

#endif  // HERMES_THREAD_RWLOCK_H_
