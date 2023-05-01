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

#include <string>
#include "hermes_shm/data_structures/ipc/string.h"
#include "omp.h"

/** Stringstream for storing test output */
std::stringstream ss;

/** Print the CSV output */
static void PrintTestOutput() {
  std::cout << ss.str() << std::endl;
}

/** Number of tests executed */
int test_count = 0;

/** Global atomic counter */
template<typename AtomicT>
struct GlobalAtomicCounter {
  static AtomicT count_;
};
#define GLOBAL_COUNTER(Atomic, T)\
  template<>\
  Atomic<T> GlobalAtomicCounter<Atomic<T>>::count_ = Atomic<T>(0);
GLOBAL_COUNTER(std::atomic, uint16_t)
GLOBAL_COUNTER(std::atomic, uint32_t)
GLOBAL_COUNTER(std::atomic, uint64_t)
GLOBAL_COUNTER(hipc::nonatomic, uint16_t)
GLOBAL_COUNTER(hipc::nonatomic, uint32_t)
GLOBAL_COUNTER(hipc::nonatomic, uint64_t)

/** Atomic Test Suite */
template<typename AtomicT, typename T, std::memory_order MemoryOrder>
class AtomicInstructionTestSuite {
 public:
  std::string memory_order_;
  std::string atomic_type_;
  void *ptr_;

 public:
  /////////////////
  /// Test Cases
  /////////////////

  /** Constructor */
  AtomicInstructionTestSuite() {
    if constexpr(MemoryOrder == std::memory_order_relaxed) {
      memory_order_ = "std::memory_order_relaxed";
    } else if constexpr(MemoryOrder == std::memory_order_consume) {
      memory_order_ = "std::memory_order_consume";
    } else if constexpr(MemoryOrder == std::memory_order_acquire) {
      memory_order_ = "std::memory_order_acquire";
    } else if constexpr(MemoryOrder == std::memory_order_release) {
      memory_order_ = "std::memory_order_release";
    } else if constexpr(MemoryOrder == std::memory_order_acq_rel) {
      memory_order_ = "std::memory_order_acq_rel";
    } else if constexpr(MemoryOrder == std::memory_order_seq_cst) {
      memory_order_ = "std::memory_order_seq_cst";
    }

    if constexpr(std::is_same_v<std::atomic<T>, AtomicT>) {
      atomic_type_ = "std::atomic";
    } else if constexpr(std::is_same_v<hipc::nonatomic<T>, AtomicT>) {
      atomic_type_ = "non-atomic";
    }
  }

  /** Atomic Increment */
  void AtomicIncrement(size_t count) {
    Timer t;
#pragma omp barrier
    t.Resume();
    for (size_t i = 0; i < count; ++i) {
      GlobalAtomicCounter<AtomicT>::count_ += 1;
    }
#pragma omp barrier
    t.Pause();
    TestOutput("AtomicIncrement", count, t);
  }

  /** Atomic Fetch Add */
  void AtomicFetchAdd(size_t count) {
    Timer t;
#pragma omp barrier
    t.Resume();
    for (size_t i = 0; i < count; ++i) {
      GlobalAtomicCounter<AtomicT>::count_.fetch_add(1, MemoryOrder);
    }
#pragma omp barrier
    t.Pause();
    TestOutput("AtomicFetchAdd", count, t);
  }

  /** Atomic Assign */
  void AtomicAssign(size_t count) {
    Timer t;
#pragma omp barrier
    t.Resume();
    for (size_t i = 0; i < count; ++i) {
      GlobalAtomicCounter<AtomicT>::count_ = i;
    }
#pragma omp barrier
    t.Pause();
    TestOutput("AtomicAssign", count, t);
  }

  /** Atomic Exchange */
  void AtomicExchange(size_t count) {
    Timer t;
#pragma omp barrier
    t.Resume();
    for (size_t i = 0; i < count; ++i) {
      GlobalAtomicCounter<AtomicT>::count_.exchange(i, MemoryOrder);
    }
#pragma omp barrier
    t.Pause();
    TestOutput("AtomicExchange", count, t);
  }

  /** Atomic Fetch */
  void AtomicFetch(size_t count) {
    Timer t;
#pragma omp barrier
    t.Resume();
    for (size_t i = 0; i < count; ++i) {
      size_t x = GlobalAtomicCounter<AtomicT>::count_.load(MemoryOrder);
      USE(x);
    }
#pragma omp barrier
    t.Pause();
    TestOutput("AtomicFetch", count, t);
  }

  /////////////////
  /// Test Output
  /////////////////

  /** The CSV header */
  void TestOutputHeader() {
    ss << "test_name" << ","
       << "atomic_type" << ","
       << "type_size" << ","
       << "atomic_size" << ","
       << "memory_order" << ","
       << "count" << ","
       << "nthreads" << ","
       << "MOps" << ","
       << "time" << std::endl;
  }

  /** The CSV test case */
  void TestOutput(const std::string &test_name, size_t count, Timer &t) {
    int rank = omp_get_thread_num();
    if (rank != 0) { return; }
    if (test_count == 0) {
      TestOutputHeader();
    }
    size_t nthreads = omp_get_num_threads();
    ss << test_name << ","
       << atomic_type_ << ","
       << sizeof(T) << ","
       << sizeof(AtomicT) << ","
       << memory_order_ << ","
       << count << ","
       << omp_get_num_threads() << ","
       << count * nthreads / t.GetUsec() << ","
       << t.GetUsec() << std::endl;
    ++test_count;
  }
};

/**
 * Compare different atomic instructions on a particular thread.
 * Given the atomic type and memory order.
 * */
template<typename AtomicT, typename T, std::memory_order MemoryOrder>
void TestAtomicInstructionsPerThread() {
  size_t count = (1<<20);
  AtomicInstructionTestSuite<AtomicT, T, MemoryOrder>().AtomicIncrement(count);
  AtomicInstructionTestSuite<AtomicT, T, MemoryOrder>().AtomicFetchAdd(count);
  AtomicInstructionTestSuite<AtomicT, T, MemoryOrder>().AtomicAssign(count);
  AtomicInstructionTestSuite<AtomicT, T, MemoryOrder>().AtomicExchange(count);
  AtomicInstructionTestSuite<AtomicT, T, MemoryOrder>().AtomicFetch(count);
}

/**
 * Compare the differences between different memory orders on a particular
 * thread. Given the atomic type.
 * */
template<typename AtomicT, typename T>
void TestMemoryOrdersPerThread() {
  TestAtomicInstructionsPerThread<AtomicT, T, std::memory_order_relaxed>();
  TestAtomicInstructionsPerThread<AtomicT, T, std::memory_order_consume>();
  TestAtomicInstructionsPerThread<AtomicT, T, std::memory_order_acquire>();
  TestAtomicInstructionsPerThread<AtomicT, T, std::memory_order_release>();
  TestAtomicInstructionsPerThread<AtomicT, T, std::memory_order_acq_rel>();
  TestAtomicInstructionsPerThread<AtomicT, T, std::memory_order_seq_cst>();
}

/**
 * Compare the differences between different atomic types on a particular
 * thread.
 * */
void TestAtomicTypes() {
  TestMemoryOrdersPerThread<std::atomic<uint16_t>, uint16_t>();
  TestMemoryOrdersPerThread<std::atomic<uint32_t>, uint32_t>();
  TestMemoryOrdersPerThread<std::atomic<uint64_t>, uint64_t>();

  TestMemoryOrdersPerThread<hipc::nonatomic<uint16_t>, uint16_t>();
  TestMemoryOrdersPerThread<hipc::nonatomic<uint32_t>, uint32_t>();
  TestMemoryOrdersPerThread<hipc::nonatomic<uint64_t>, uint64_t>();
}

/**
 * Compare the differences between different memory orders and atomic
 * instructions with multiple threads. Given number of threads.
 * */
void TestAtomicInstructions(int nthreads) {
  omp_set_dynamic(0);
#pragma omp parallel num_threads(nthreads)
  {
#pragma omp barrier
    TestAtomicTypes();
#pragma omp barrier
  }
}

TEST_CASE("AtomicInstructionTest") {
  TestAtomicInstructions(1);
  TestAtomicInstructions(2);
  TestAtomicInstructions(4);
  TestAtomicInstructions(8);
  PrintTestOutput();
}
