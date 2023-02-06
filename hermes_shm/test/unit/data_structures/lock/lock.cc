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


#include "basic_test.h"
#include "omp.h"
#include "hermes_shm/thread/lock.h"

using hermes_shm::Mutex;
using hermes_shm::RwLock;

void MutexTest() {
  int nthreads = 8;
  int loop_count = 10000;
  int count = 0;
  Mutex lock;

  HERMES_SHM_THREAD_MANAGER->GetThreadStatic();

  omp_set_dynamic(0);
#pragma omp parallel shared(lock) num_threads(nthreads)
  {
    // Support parallel write
#pragma omp barrier
    for (int i = 0; i < loop_count; ++i) {
      lock.Lock();
      count += 1;
      lock.Unlock();
    }
#pragma omp barrier
    REQUIRE(count == loop_count * nthreads);
#pragma omp barrier
  }
}

void barrier_for_reads(std::vector<int> &tid_start, int left) {
  int count;
  do {
    count = 0;
    for (int i = 0; i < left; ++i) {
      count += tid_start[i];
    }
  } while (count < left);
}

void RwLockTest() {
  int nthreads = 8;
  int left = nthreads / 2;
  std::vector<int> tid_start(left, 0);
  int loop_count = 100000;
  int count = 0;
  RwLock lock;

  HERMES_SHM_THREAD_MANAGER->GetThreadStatic();

  omp_set_dynamic(0);
#pragma omp parallel \
  shared(lock, nthreads, left, loop_count, count, tid_start) \
  num_threads(nthreads)
  {
    int tid = omp_get_thread_num();

    // Support parallel write
#pragma omp barrier
    for (int i = 0; i < loop_count; ++i) {
      lock.WriteLock();
      count += 1;
      lock.WriteUnlock();
    }
#pragma omp barrier
    REQUIRE(count == loop_count * nthreads);
#pragma omp barrier

    // Support for parallel read and write
#pragma omp barrier

    int cur_count = count;
    if (tid < left) {
      lock.ReadLock();
      tid_start[tid] = 1;
      barrier_for_reads(tid_start, left);
      for (int i = 0; i < loop_count; ++i) {
        REQUIRE(count == cur_count);
      }
      lock.ReadUnlock();
    } else {
      barrier_for_reads(tid_start, left);
      lock.WriteLock();
      for (int i = 0; i < loop_count; ++i) {
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
