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

#include "hermes_shm/util/timer.h"
#include "hermes_shm/thread/lock.h"
#include "test_init.h"
#include "basic_test.h"
#include <iostream>

TEST_CASE("TestMutexTryLock") {
  Timer t;
  t.Resume();
  size_t ops = (1ull << 26);
  hshm::Mutex lock;
  for (size_t i = 0; i < ops; ++i) {
    lock.TryLock(0);
    lock.Unlock();
  }
  t.Pause();
  std::cout << ops / t.GetUsec() << std::endl;
}

TEST_CASE("TestMutex") {
  Timer t;
  t.Resume();
  size_t ops = (1ull << 26);
  hshm::Mutex lock;
  for (size_t i = 0; i < ops; ++i) {
    hshm::ScopedMutex scoped_lock(lock, 0);
  }
  t.Pause();
  std::cout << ops / t.GetUsec() << std::endl;
}

TEST_CASE("TestRwReadLock") {
  Timer t;
  t.Resume();
  size_t ops = (1ull << 26);
  hshm::RwLock lock_;
  for (size_t i = 0; i < ops; ++i) {
    hshm::ScopedRwReadLock read_lock(lock_, 0);
  }
  t.Pause();
  std::cout << ops / t.GetUsec() << std::endl;
}

TEST_CASE("TestRwWriteLock") {
  Timer t;
  t.Resume();
  size_t ops = (1ull << 26);
  hshm::RwLock lock_;
  for (size_t i = 0; i < ops; ++i) {
    hshm::ScopedRwReadLock write_lock(lock_, 0);
  }
  t.Pause();
  std::cout << ops / t.GetUsec() << std::endl;
}