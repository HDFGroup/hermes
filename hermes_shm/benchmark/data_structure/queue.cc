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
#include <hermes_shm/data_structures/ipc/mpsc_queue.h>
#include <hermes_shm/data_structures/ipc/spsc_queue.h>

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

  /**====================================
   * Test Runner
   * ===================================*/

  /** Test case constructor */
  QueueTest() {
    if constexpr(std::is_same_v<std::queue<T>, QueueT>) {
      queue_type_ = "std::queue";
    } else if constexpr(std::is_same_v<hipc::mpsc_queue<T>, QueueT>) {
      queue_type_ = "hipc::mpsc_queue";
    } else if constexpr(std::is_same_v<hipc::mpsc_queue_ext<T>, QueueT>) {
      queue_type_ = "hipc::mpsc_queue_ext";
    } else if constexpr(std::is_same_v<hipc::spsc_queue<T>, QueueT>) {
      queue_type_ = "hipc::spsc_queue";
    } else {
      HELOG(kFatal, "none of the queue tests matched")
    }
    internal_type_ = InternalTypeName<T>::Get();
    x_ = hipc::make_uptr<T>();
  }

  /** Run the tests */
  void Test() {
    size_t count = 10000;
    // AllocateTest(count);
    EmplaceTest(count);
    DequeueTest(count);
  }

  /**====================================
   * Tests
   * ===================================*/

  /** Test performance of queue allocation */
  void AllocateTest(size_t count) {
    Timer t;
    t.Resume();
    for (size_t i = 0; i < count; ++i) {
      Allocate(count);
      Destroy();
    }
    t.Pause();

    TestOutput("Allocate", t);
  }

  /** Emplace after reserving enough space */
  void EmplaceTest(size_t count) {
    Timer t;
    StringOrInt<T> var(124);

    Allocate(count);
    t.Resume();
    Emplace(count);
    t.Pause();

    TestOutput("Enqueue", t);
    Destroy();
  }

  /** Emplace after reserving enough space */
  void DequeueTest(size_t count) {
    Timer t;
    StringOrInt<T> var(124);

    Allocate(count);
    Emplace(count);

    t.Resume();
    Dequeue(count);
    t.Pause();

    TestOutput("Dequeue", t);
    Destroy();
  }

 private:
  /**====================================
   * Helpers
   * ===================================*/

  /** Output as CSV */
  void TestOutput(const std::string &test_name, Timer &t) {
    HIPRINT("{},{},{},{}\n",
            test_name, queue_type_, internal_type_, t.GetMsec())
  }

  /** Get element at position i */
  void Dequeue(size_t count) {
    for (size_t i = 0; i < count; ++i) {
      if constexpr(std::is_same_v<QueueT, std::queue<T>>) {
        T &x = queue_->front();
        USE(x);
        queue_->pop();
      } else if constexpr(std::is_same_v<QueueT, hipc::mpsc_queue<T>>) {
        queue_->pop(*x_);
        USE(*x_);
      } else if constexpr(std::is_same_v<QueueT, hipc::mpsc_queue_ext<T>>) {
        queue_->pop(*x_);
        USE(*x_);
      } else if constexpr(std::is_same_v<QueueT, hipc::spsc_queue<T>>) {
        queue_->pop(*x_);
        USE(*x_);
      }
    }
  }

  /** Emplace elements into the queue */
  void Emplace(size_t count) {
    StringOrInt<T> var(124);
    for (size_t i = 0; i < count; ++i) {
      if constexpr(std::is_same_v<QueueT, std::queue<T>>) {
        queue_->emplace(var.Get());
      } else if constexpr(std::is_same_v<QueueT, hipc::mpsc_queue<T>>) {
        queue_->emplace(var.Get());
      } else if constexpr(std::is_same_v<QueueT, hipc::mpsc_queue_ext<T>>) {
        queue_->emplace(var.Get());
      } else if constexpr(std::is_same_v<QueueT, hipc::spsc_queue<T>>) {
        queue_->emplace(var.Get());
      }
    }
  }

  /** Allocate an arbitrary queue for the test cases */
  void Allocate(size_t count) {
    if constexpr(std::is_same_v<QueueT, std::queue<T>>) {
      queue_ptr_ = new std::queue<T>();
      queue_ = queue_ptr_;
    } else if constexpr(std::is_same_v<QueueT, hipc::mpsc_queue<T>>) {
      queue_ptr_ = hipc::make_mptr<QueueT>(count);
      queue_ = queue_ptr_.get();
    } else if constexpr(std::is_same_v<QueueT, hipc::mpsc_queue_ext<T>>) {
      queue_ptr_ = hipc::make_mptr<QueueT>(count);
      queue_ = queue_ptr_.get();
    } else if constexpr(std::is_same_v<QueueT, hipc::spsc_queue<T>>) {
      queue_ptr_ = hipc::make_mptr<QueueT>(count);
      queue_ = queue_ptr_.get();
    }
  }

  /** Destroy the queue */
  void Destroy() {
    if constexpr(std::is_same_v<QueueT, std::queue<T>>) {
      delete queue_ptr_;
    } else if constexpr(std::is_same_v<QueueT, hipc::mpsc_queue<T>>) {
      queue_ptr_.shm_destroy();
    } else if constexpr(std::is_same_v<QueueT, hipc::mpsc_queue_ext<T>>) {
      queue_ptr_.shm_destroy();
    } else if constexpr(std::is_same_v<QueueT, hipc::spsc_queue<T>>) {
      queue_ptr_.shm_destroy();
    }
  }
};

void FullQueueTest() {
  // std::queue tests
  QueueTest<size_t, std::queue<size_t>>().Test();
  // QueueTest<std::string, std::queue<std::string>>().Test();

  // hipc::mpsc_queue tests
  QueueTest<size_t, hipc::mpsc_queue<size_t>>().Test();
  // QueueTest<std::string, hipc::mpsc_queue<std::string>>().Test();
  // QueueTest<hipc::string, hipc::mpsc_queue<hipc::string>>().Test();

  // hipc::mpsc_ext_queue tests
  QueueTest<size_t, hipc::mpsc_queue_ext<size_t>>().Test();
  // QueueTest<std::string, hipc::mpsc_queue_ext<std::string>>().Test();
  // QueueTest<hipc::string, hipc::mpsc_queue_ext<hipc::string>>().Test();

  // hipc::spsc_queue tests
  QueueTest<size_t, hipc::spsc_queue<size_t>>().Test();
  // QueueTest<std::string, hipc::spsc_queue<std::string>>().Test();
  // QueueTest<hipc::string, hipc::spsc_queue<hipc::string>>().Test();
}

TEST_CASE("QueueBenchmark") {
  FullQueueTest();
}
