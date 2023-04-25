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

/** The information stored by RwLock */
union RwLockPayload {
  struct {
    uint32_t r_;
    uint32_t w_;
  } bits_;
  uint64_t as_int_;

  /** Default constructor */
  RwLockPayload() = default;

  /** Copy constructor */
  RwLockPayload(const RwLockPayload &other) {
    as_int_ = other.as_int_;
  }

  /** Copy constructor. From uint64_t. */
  explicit RwLockPayload(uint64_t other) {
    as_int_ = other;
  }

  /** Check if write locked */
  bool IsWriteLocked() const {
    return bits_.w_ > 0;
  }

  /** Check if read locked */
  bool IsReadLocked() const {
    return bits_.r_ > 0;
  }
};

/** A reader-writer lock implementation */
struct RwLock {
  std::atomic<uint64_t> payload_;
#ifdef HERMES_DEBUG_LOCK
  uint32_t owner_;
#endif

  /** Default constructor */
  RwLock() : payload_(0) {}

  /** Explicit constructor */
  void Init() {
    payload_ = 0;
  }

  /** Delete copy constructor */
  RwLock(const RwLock &other) = delete;

  /** Move constructor */
  RwLock(RwLock &&other) noexcept
  : payload_(other.payload_.load()) {}

  /** Move assignment operator */
  RwLock& operator=(RwLock &&other) noexcept {
    payload_ = other.payload_.load();
    return (*this);
  }

  /** Acquire read lock */
  void ReadLock(uint32_t owner);

  /** Release read lock */
  void ReadUnlock();

  /** Acquire write lock */
  void WriteLock(uint32_t owner);

  /** Release write lock */
  void WriteUnlock();
};

/** Acquire the read lock in a scope */
struct ScopedRwReadLock {
  RwLock &lock_;
  bool is_locked_;

  /** Acquire the read lock */
  explicit ScopedRwReadLock(RwLock &lock, uint32_t owner);

  /** Release the read lock */
  ~ScopedRwReadLock();

  /** Explicitly acquire read lock */
  void Lock(uint32_t owner);

  /** Explicitly release read lock */
  void Unlock();
};

/** Acquire scoped write lock */
struct ScopedRwWriteLock {
  RwLock &lock_;
  bool is_locked_;

  /** Acquire the write lock */
  explicit ScopedRwWriteLock(RwLock &lock, uint32_t owner);

  /** Release the write lock */
  ~ScopedRwWriteLock();

  /** Explicity acquire the write lock */
  void Lock(uint32_t owner);

  /** Explicitly release the write lock */
  void Unlock();
};

}  // namespace hshm

#endif  // HERMES_THREAD_RWLOCK_H_
