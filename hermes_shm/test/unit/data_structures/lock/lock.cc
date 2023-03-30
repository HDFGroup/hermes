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


#include "basic_test.h"
#include "omp.h"
#include "hermes_shm/thread/lock.h"

using hshm::Mutex;
using hshm::RwLock;

void MutexTest() {
  size_t nthreads = 8;
  size_t loop_count = 10000;
  size_t count = 0;
  Mutex lock;

  HERMES_THREAD_MANAGER->GetThreadStatic();

  omp_set_dynamic(0);
#pragma omp parallel shared(lock) num_threads(nthreads)
  {
    // Support parallel write
#pragma omp barrier
    for (size_t i = 0; i < loop_count; ++i) {
      lock.Lock();
      count += 1;
      lock.Unlock();
    }
#pragma omp barrier
    REQUIRE(count == loop_count * nthreads);
#pragma omp barrier
  }
}

void barrier_for_reads(std::vector<int> &tid_start, size_t left) {
  size_t count;
  do {
    count = 0;
    for (size_t i = 0; i < left; ++i) {
      count += tid_start[i];
    }
  } while (count < left);
}

void RwLockTest() {
  size_t nthreads = 8;
  size_t left = nthreads / 2;
  std::vector<int> tid_start(left, 0);
  size_t loop_count = 100000;
  size_t count = 0;
  RwLock lock;

  HERMES_THREAD_MANAGER->GetThreadStatic();

  omp_set_dynamic(0);
#pragma omp parallel \
  shared(lock, nthreads, left, loop_count, count, tid_start) \
  num_threads(nthreads)
  {  // NOLINT
    int tid = omp_get_thread_num();

    // Support parallel write
#pragma omp barrier
    for (size_t i = 0; i < loop_count; ++i) {
      lock.WriteLock();
      count += 1;
      lock.WriteUnlock();
    }
#pragma omp barrier
    REQUIRE(count == loop_count * nthreads);
#pragma omp barrier

    // Support for parallel read and write
#pragma omp barrier

    size_t cur_count = count;
    if ((size_t)tid < left) {
      lock.ReadLock();
      tid_start[tid] = 1;
      barrier_for_reads(tid_start, left);
      for (size_t i = 0; i < loop_count; ++i) {
        REQUIRE(count == cur_count);
      }
      lock.ReadUnlock();
    } else {
      barrier_for_reads(tid_start, left);
      lock.WriteLock();
      for (size_t i = 0; i < loop_count; ++i) {
        count += 1;
      }
      lock.WriteUnlock();
    }
  }
}

TEST_CASE("Mutex") {
  MutexTest();
}

TEST_CASE("RwLock") {
  RwLockTest();
}
