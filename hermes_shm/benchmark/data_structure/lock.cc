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
#include "test_init.h"

// Std
#include <string>
#include <queue>

// hermes
#include "hermes_shm/thread/lock.h"

/**
 * A series of performance tests for vectors
 * OUTPUT:
 * [test_name] [vec_type] [internal_type] [time_ms]
 * */
template<typename LockT>
class LockTest {
 public:
  std::string lock_type_;
  std::queue<int> queue_;
  LockT lock_;

  /**====================================
   * Test Runner
   * ===================================*/

  /** Test case constructor */
  LockTest() {
    if constexpr(std::is_same_v<std::mutex, LockT>) {
      lock_type_ = "std::mutex";
    } else if constexpr(std::is_same_v<hshm::RwLock, LockT>) {
      lock_type_ = "hshm::RwLock";
    } else if constexpr(std::is_same_v<hshm::Mutex, LockT>) {
      lock_type_ = "hshm::Mutex";
    } else {
      HELOG(kFatal, "none of the queue tests matched")
    }
  }

  /** Run the tests */
  void Test(size_t count_per_rank = 100000, int nthreads = 1) {
    // AllocateTest(count);
    EmplaceTest(count_per_rank, nthreads);
    GatherTest(count_per_rank, nthreads);
  }

  /**====================================
   * Tests
   * ===================================*/

  /** Emplace after reserving enough space */
  void EmplaceTest(size_t count_per_rank, int nthreads) {
    Timer t;

    size_t count = count_per_rank * nthreads;
    t.Resume();
    Emplace(count_per_rank, nthreads);
    t.Pause();

    TestOutput("Enqueue", t, count, nthreads);
  }

  /** Reader lock scalability */
  void GatherTest(size_t count_per_rank, int nthreads) {
    Timer t;

    size_t count = count_per_rank * nthreads;
    t.Resume();
    Gather(count_per_rank, nthreads);
    t.Pause();

    TestOutput("Gather", t, count, nthreads);
  }

 private:
  /**====================================
   * Helpers
   * ===================================*/

  /** Output as CSV */
  void TestOutput(const std::string &test_name, Timer &t,
                  size_t count, int nthreads) {
    HIPRINT("{},{},{},{},{}\n",
            test_name, lock_type_, nthreads, t.GetMsec(),
            (float)count / t.GetUsec())
  }

  /** Emplace elements into the queue */
  void Emplace(size_t count_per_rank, int nthreads) {
    omp_set_dynamic(0);
#pragma omp parallel num_threads(nthreads)
    {
      for (size_t i = 0; i < count_per_rank; ++i) {
        if constexpr(std::is_same_v<LockT, std::mutex>) {
          lock_.lock();
          queue_.emplace(1);
          lock_.unlock();
        } else if constexpr(std::is_same_v<LockT, hshm::Mutex>) {
          lock_.Lock(0);
          queue_.emplace(1);
          lock_.Unlock();
        } else if constexpr(std::is_same_v<LockT, hshm::RwLock>) {
          lock_.WriteLock(0);
          queue_.emplace(1);
          lock_.WriteUnlock();
        }
      }
    }
  }

  /** Emplace elements into the queue */
  void Gather(size_t count_per_rank, int nthreads) {
    std::vector<int> data(count_per_rank * nthreads);
    omp_set_dynamic(0);
#pragma omp parallel shared(data) num_threads(nthreads)
    {
      size_t sum = 0;
      for (size_t i = 0; i < count_per_rank; ++i) {
        if constexpr(std::is_same_v<LockT, std::mutex>) {
          lock_.lock();
          sum += data[i];
          lock_.unlock();
        } else if constexpr(std::is_same_v<LockT, hshm::Mutex>) {
          lock_.Lock(0);
          sum += data[i];
          lock_.Unlock();
        } else if constexpr(std::is_same_v<LockT, hshm::RwLock>) {
          lock_.ReadLock(0);
          sum += data[i];
          lock_.ReadUnlock();
        }
      }
    }
  }
};

TEST_CASE("LockBenchmark") {
  size_t count_per_rank = 1000000;
  LockTest<std::mutex>().Test(count_per_rank, 1);
  LockTest<std::mutex>().Test(count_per_rank, 8);
  LockTest<std::mutex>().Test(count_per_rank, 16);

  LockTest<hshm::Mutex>().Test(count_per_rank, 1);
  LockTest<hshm::Mutex>().Test(count_per_rank, 8);
  LockTest<hshm::Mutex>().Test(count_per_rank, 16);

  LockTest<hshm::RwLock>().Test(count_per_rank, 1);
  LockTest<hshm::RwLock>().Test(count_per_rank, 8);
  LockTest<hshm::RwLock>().Test(count_per_rank, 16);
}