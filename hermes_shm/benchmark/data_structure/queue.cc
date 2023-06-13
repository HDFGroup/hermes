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
#include "hermes_shm/data_structures/ipc/string.h"
#include "hermes_shm/data_structures/ipc/split_ticket_queue.h"
#include <hermes_shm/data_structures/ipc/mpsc_queue.h>
#include <hermes_shm/data_structures/ipc/spsc_queue.h>
#include <hermes_shm/data_structures/ipc/ticket_queue.h>

/**
 * A series of performance tests for vectors
 * OUTPUT:
 * [test_name] [vec_type] [internal_type] [time_ms]
 * */
template<typename T, typename QueueT,
  typename ListTPtr=SHM_X_OR_Y(QueueT, hipc::mptr<QueueT>, QueueT*)>
class QueueTest {
 public:
  std::string queue_type_;
  std::string internal_type_;
  QueueT *queue_;
  ListTPtr queue_ptr_;
  void *ptr_;
  hipc::uptr<T> x_;
  std::mutex lock_;
  int cpu_;

  /**====================================
   * Test Runner
   * ===================================*/

  /** Test case constructor */
  QueueTest() {
    if constexpr(std::is_same_v<std::queue<T>, QueueT>) {
      queue_type_ = "std::queue";
    } else if constexpr(std::is_same_v<hipc::mpsc_queue<T>, QueueT>) {
      queue_type_ = "hipc::mpsc_queue";
    } else if constexpr(std::is_same_v<hipc::spsc_queue<T>, QueueT>) {
      queue_type_ = "hipc::spsc_queue";
    } else if constexpr(std::is_same_v<hipc::ticket_queue<T>, QueueT>) {
      queue_type_ = "hipc::ticket_queue";
    } else if constexpr(std::is_same_v<hipc::split_ticket_queue<T>, QueueT>) {
      queue_type_ = "hipc::split_ticket_queue";
    } else {
      HELOG(kFatal, "none of the queue tests matched")
    }
    internal_type_ = InternalTypeName<T>::Get();
    x_ = hipc::make_uptr<T>();
  }

  /** Run the tests */
  void Test(size_t count_per_rank=100000, int nthreads = 1) {
    // AllocateTest(count);
    EmplaceTest(count_per_rank, nthreads);
    DequeueTest(count_per_rank, nthreads);
  }

  /**====================================
   * Tests
   * ===================================*/

  /** Test performance of queue allocation */
  void AllocateTest(size_t count) {
    Timer t;
    t.Resume();
    for (size_t i = 0; i < count; ++i) {
      Allocate(count, count, 1);
      Destroy();
    }
    t.Pause();

    TestOutput("Allocate", t);
  }

  /** Emplace after reserving enough space */
  void EmplaceTest(size_t count_per_rank, int nthreads) {
    Timer t;

    size_t count = count_per_rank * nthreads;
    Allocate(count, count_per_rank, nthreads);
    t.Resume();
    Emplace(count_per_rank, nthreads);
    t.Pause();

    TestOutput("Enqueue", t, count, nthreads);
    Destroy();
  }

  /** Emplace after reserving enough space */
  void DequeueTest(size_t count_per_rank, int nthreads) {
    Timer t;
    StringOrInt<T> var(124);

    size_t count = count_per_rank * nthreads;
    Allocate(count, count_per_rank, nthreads);
    Emplace(count, 1);

    t.Resume();
    Dequeue(count_per_rank, nthreads);
    t.Pause();

    TestOutput("Dequeue", t, count, nthreads);
    Destroy();
  }

 private:
  /**====================================
   * Helpers
   * ===================================*/

  /** Output as CSV */
  void TestOutput(const std::string &test_name, Timer &t,
                  size_t count, int nthreads) {
    HIPRINT("{},{},{},{},{},{}\n",
            test_name, queue_type_, internal_type_, nthreads, t.GetMsec(),
            (float)count / t.GetUsec())
  }

  /** Emplace elements into the queue */
  void Emplace(size_t count_per_rank, int nthreads) {
    omp_set_dynamic(0);
#pragma omp parallel num_threads(nthreads)
    {
      StringOrInt<T> var(124);
      for (size_t i = 0; i < count_per_rank; ++i) {
        if constexpr(std::is_same_v<QueueT, std::queue<T>>) {
          lock_.lock();
          queue_->emplace(var.Get());
          lock_.unlock();
        } else if constexpr(std::is_same_v<QueueT, hipc::mpsc_queue<T>>) {
          queue_->emplace(var.Get());
        } else if constexpr(std::is_same_v<QueueT, hipc::spsc_queue<T>>) {
          queue_->emplace(var.Get());
        } else if constexpr(std::is_same_v<QueueT, hipc::ticket_queue<T>>) {
          queue_->emplace(var.Get());
        } else if constexpr(
          std::is_same_v<QueueT, hipc::split_ticket_queue<T>>) {
          queue_->emplace(var.Get());
        }
      }
    }
  }

  /** Get element at position i */
  void Dequeue(size_t count_per_rank, int nthreads) {
    omp_set_dynamic(0);
#pragma omp parallel num_threads(nthreads)
    {
      for (size_t i = 0; i < count_per_rank; ++i) {
        if constexpr(std::is_same_v<QueueT, std::queue<T>>) {
          lock_.lock();
          T &x = queue_->front();
          USE(x);
          queue_->pop();
          lock_.unlock();
        } else if constexpr(std::is_same_v<QueueT, hipc::mpsc_queue<T>>) {
          queue_->pop(*x_);
          USE(*x_);
        } else if constexpr(std::is_same_v<QueueT, hipc::spsc_queue<T>>) {
          queue_->pop(*x_);
          USE(*x_);
        } else if constexpr(std::is_same_v<QueueT, hipc::ticket_queue<T>>) {
          while (queue_->pop(*x_).IsNull());
        } else if constexpr(
          std::is_same_v<QueueT, hipc::split_ticket_queue<T>>) {
          while (queue_->pop(*x_).IsNull());
        }
      }
    }
  }

  /** Allocate an arbitrary queue for the test cases */
  void Allocate(size_t count, size_t count_per_rank, int nthreads) {
    (void) count_per_rank; (void) nthreads;
    if constexpr(std::is_same_v<QueueT, std::queue<T>>) {
      queue_ptr_ = new std::queue<T>();
      queue_ = queue_ptr_;
    } else if constexpr(std::is_same_v<QueueT, hipc::mpsc_queue<T>>) {
      queue_ptr_ = hipc::make_mptr<QueueT>(count);
      queue_ = queue_ptr_.get();
    } else if constexpr(std::is_same_v<QueueT, hipc::spsc_queue<T>>) {
      queue_ptr_ = hipc::make_mptr<QueueT>(count);
      queue_ = queue_ptr_.get();
    } else if constexpr(std::is_same_v<QueueT, hipc::ticket_queue<T>>) {
      queue_ptr_ = hipc::make_mptr<QueueT>(count);
      queue_ = queue_ptr_.get();
    } else if constexpr(std::is_same_v<QueueT, hipc::split_ticket_queue<T>>) {
      queue_ptr_ = hipc::make_mptr<QueueT>(count_per_rank, nthreads);
      queue_ = queue_ptr_.get();
    }
  }

  /** Destroy the queue */
  void Destroy() {
    if constexpr(std::is_same_v<QueueT, std::queue<T>>) {
      delete queue_ptr_;
    } else if constexpr(std::is_same_v<QueueT, hipc::mpsc_queue<T>>) {
      queue_ptr_.shm_destroy();
    } else if constexpr(std::is_same_v<QueueT, hipc::spsc_queue<T>>) {
      queue_ptr_.shm_destroy();
    } else if constexpr(std::is_same_v<QueueT, hipc::ticket_queue<T>>) {
      queue_ptr_.shm_destroy();
    } else if constexpr(std::is_same_v<QueueT, hipc::split_ticket_queue<T>>) {
      queue_ptr_.shm_destroy();
    }
  }
};

void FullQueueTest() {
  const size_t count_per_rank = 1000000;
  // std::queue tests
  QueueTest<size_t, std::queue<size_t>>().Test(count_per_rank, 1);
  QueueTest<size_t, std::queue<size_t>>().Test(count_per_rank, 4);
  QueueTest<size_t, std::queue<size_t>>().Test(count_per_rank, 8);
  QueueTest<size_t, std::queue<size_t>>().Test(count_per_rank, 16);
  QueueTest<std::string, std::queue<std::string>>().Test();

  // hipc::ticket_queue tests
  QueueTest<size_t, hipc::ticket_queue<size_t>>().Test(count_per_rank, 1);
  QueueTest<size_t, hipc::ticket_queue<size_t>>().Test(count_per_rank, 4);
  QueueTest<size_t, hipc::ticket_queue<size_t>>().Test(count_per_rank, 8);
  QueueTest<size_t, hipc::ticket_queue<size_t>>().Test(count_per_rank, 16);

  // hipc::ticket_queue tests
  QueueTest<size_t, hipc::split_ticket_queue<size_t>>().Test(count_per_rank, 1);
  QueueTest<size_t, hipc::split_ticket_queue<size_t>>().Test(count_per_rank, 4);
  QueueTest<size_t, hipc::split_ticket_queue<size_t>>().Test(count_per_rank, 8);
  QueueTest<size_t, hipc::split_ticket_queue<size_t>>().Test(count_per_rank, 16);

  // hipc::mpsc_queue tests
  QueueTest<size_t, hipc::mpsc_queue<size_t>>().Test(count_per_rank, 1);
  QueueTest<std::string, hipc::mpsc_queue<std::string>>().Test();
  QueueTest<hipc::string, hipc::mpsc_queue<hipc::string>>().Test();

  // hipc::spsc_queue tests
  QueueTest<size_t, hipc::spsc_queue<size_t>>().Test(count_per_rank, 1);
  QueueTest<std::string, hipc::spsc_queue<std::string>>().Test();
  QueueTest<hipc::string, hipc::spsc_queue<hipc::string>>().Test();

}

TEST_CASE("QueueBenchmark") {
  FullQueueTest();
}
