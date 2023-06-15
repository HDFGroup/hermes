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
#include "omp.h"

#include <string>
#include "hermes_shm/data_structures/ipc/string.h"

/** Test cases for the allocator */
class AllocatorTestSuite {
 public:
  std::string alloc_type_;
  Allocator *alloc_;
  Timer timer_;

  /**====================================
   * Test Runner
   * ===================================*/

  /** Constructor */
  AllocatorTestSuite(AllocatorType alloc_type, Allocator *alloc)
    : alloc_(alloc) {
    switch (alloc_type) {
      case AllocatorType::kStackAllocator: {
        alloc_type_ = "hipc::StackAllocator";
        break;
      }
      case AllocatorType::kMallocAllocator: {
        alloc_type_ = "hipc::MallocAllocator";
        break;
      }
      case AllocatorType::kFixedPageAllocator: {
        alloc_type_ = "hipc::FixedPageAllocator";
        break;
      }
      case AllocatorType::kScalablePageAllocator: {
        alloc_type_ = "hipc::ScalablePageAllocator";
        break;
      }
      default: {
        HELOG(kFatal, "Could not find this allocator type");
        break;
      }
    }
  }

  /**====================================
   * Test Cases
   * ===================================*/

  /** Allocate and Free a single size in a single loop */
  void AllocateAndFreeFixedSize(size_t count, size_t size) {
    StartTimer();
    for (size_t i = 0; i < count; ++i) {
      Pointer p = alloc_->Allocate(size);
      alloc_->Free(p);
    }
    StopTimer();

    TestOutput("AllocateAndFreeFixedSize", size, count, timer_);
  }

  /** Allocate a fixed size in a loop, and then free in another loop */
  void AllocateThenFreeFixedSize(size_t count, size_t size) {
    StartTimer();
    std::vector<Pointer> cache(count);
    for (size_t i = 0; i < count; ++i) {
      cache[i] = alloc_->Allocate(size);
    }
    for (size_t i = 0; i < count; ++i) {
      alloc_->Free(cache[i]);
    }
    StopTimer();

    TestOutput("AllocateThenFreeFixedSize", count, size, timer_);
  }

  void seq(std::vector<size_t> &vec, size_t rep, size_t count) {
    for (size_t i = 0; i < count; ++i) {
      vec.emplace_back(rep);
    }
  }

  /** Allocate a window of pages, free the window. Random page sizes. */
  void AllocateAndFreeRandomWindow(size_t count) {
    std::mt19937 rng(23522523);
    std::vector<size_t> sizes_;

    seq(sizes_, 64, MEGABYTES(1) / 64);
    seq(sizes_, 190, MEGABYTES(1) / 190);
    seq(sizes_, KILOBYTES(1), MEGABYTES(1) / KILOBYTES(1));
    seq(sizes_, KILOBYTES(4), MEGABYTES(8) / KILOBYTES(4));
    seq(sizes_, KILOBYTES(32), MEGABYTES(4) / KILOBYTES(4));
    seq(sizes_, MEGABYTES(1), MEGABYTES(64) / MEGABYTES(1));
    std::shuffle(std::begin(sizes_), std::end(sizes_), rng);
    std::vector<Pointer> window(sizes_.size());
    size_t num_windows = 500;

    StartTimer();
    for (size_t w = 0; w < num_windows; ++w) {
      for (size_t i = 0; i < sizes_.size(); ++i) {
        auto &size = sizes_[i];
        window[i] = alloc_->Allocate(size);
      }
      for (size_t i = 0; i < sizes_.size(); ++i) {
        alloc_->Free(window[i]);
      }
    }
    StopTimer();
    
    TestOutput("AllocateAndFreeRandomWindow", 0, count, timer_);
  }

  /**====================================
   * Test Helpers
   * ===================================*/

  void StartTimer() {
    int rank = omp_get_thread_num();
    if (rank == 0) {
      timer_.Reset();
      timer_.Resume();
    }
#pragma omp barrier
  }

  void StopTimer() {
#pragma omp barrier
    int rank = omp_get_thread_num();
    if (rank == 0) {
      timer_.Pause();
    }
  }

  /** The CSV test case */
  void TestOutput(const std::string &test_name, size_t obj_size,
                  size_t count_per_rank, Timer &t) {
    int rank = omp_get_thread_num();
    if (rank != 0) { return; }
    int nthreads = omp_get_num_threads();
    double count = (double) count_per_rank * nthreads;
    HILOG(kInfo, "{},{},{},{},{},{},{}",
          test_name,
          alloc_type_,
          obj_size,
          t.GetMsec(),
          nthreads,
          count,
          count / t.GetMsec());
  }

  /** Print the CSV output */
  static void PrintTestHeader() {
    HILOG(kInfo, "test_name,alloc_type,obj_size,msec,nthreads,count,KOps");
  }
};

/** The minor number to use for allocators */
static int minor = 1;
const std::string shm_url = "test_allocators";

/** Create the allocator + backend for the test */
template<typename BackendT, typename AllocT, typename ...Args>
Allocator* Pretest(MemoryBackendType backend_type,
                   Args&& ...args) {
  int rank = omp_get_thread_num();
  allocator_id_t alloc_id(0, minor);
  Allocator *alloc;
  auto mem_mngr = HERMES_MEMORY_MANAGER;

  if (rank == 0) {
    // Create the allocator + backend
    mem_mngr->UnregisterAllocator(alloc_id);
    mem_mngr->UnregisterBackend(shm_url);
    mem_mngr->CreateBackend<BackendT>(
      mem_mngr->GetDefaultBackendSize(), shm_url);
    mem_mngr->CreateAllocator<AllocT>(
      shm_url, alloc_id, 0, std::forward<Args>(args)...);
  }
#pragma omp barrier

  alloc = mem_mngr->GetAllocator(alloc_id);
  if (alloc == nullptr) {
    HELOG(kFatal, "Failed to find the memory allocator?")
  }
  return alloc;
}

/** Destroy the allocator + backend from the test */
void Posttest() {
  int rank = omp_get_thread_num();
#pragma omp barrier
  if (rank == 0) {
    allocator_id_t alloc_id(0, minor);
    HERMES_MEMORY_MANAGER->UnregisterAllocator(
      alloc_id);
    HERMES_MEMORY_MANAGER->DestroyBackend(shm_url);
    minor += 1;
  }
# pragma omp barrier
}

/** A series of allocator benchmarks for a particular thread */
template<typename BackendT, typename AllocT, typename ...Args>
void AllocatorTest(AllocatorType alloc_type,
                   MemoryBackendType backend_type,
                   Args&& ...args) {
  Allocator *alloc = Pretest<BackendT, AllocT>(
    backend_type, std::forward<Args>(args)...);
  size_t count = (1 << 20);
  // Allocate many and then free many
  /*AllocatorTestSuite(alloc_type, alloc).AllocateThenFreeFixedSize(
    count, KILOBYTES(1));*/
  // Allocate and free immediately
  /*AllocatorTestSuite(alloc_type, alloc).AllocateAndFreeFixedSize(
    count, KILOBYTES(1));*/
  if (alloc_type != AllocatorType::kStackAllocator) {
    // Allocate and free randomly
    AllocatorTestSuite(alloc_type, alloc).AllocateAndFreeRandomWindow(
      count);
  }
  Posttest();
}

/** Test different allocators on a particular thread */
void FullAllocatorTestPerThread() {
  // Scalable page allocator
  AllocatorTest<hipc::PosixShmMmap, hipc::ScalablePageAllocator>(
    AllocatorType::kScalablePageAllocator,
    MemoryBackendType::kPosixShmMmap);
  // Malloc allocator
  AllocatorTest<hipc::NullBackend, hipc::MallocAllocator>(
    AllocatorType::kMallocAllocator,
    MemoryBackendType::kNullBackend);
  // Stack allocator
  AllocatorTest<hipc::PosixShmMmap, hipc::StackAllocator>(
    AllocatorType::kStackAllocator,
    MemoryBackendType::kPosixShmMmap);
}

/** Spawn multiple threads and run allocator tests */
void FullAllocatorTestThreaded(int nthreads) {
  omp_set_dynamic(0);
#pragma omp parallel num_threads(nthreads)
  {
#pragma omp barrier
    FullAllocatorTestPerThread();
#pragma omp barrier
  }
}

TEST_CASE("AllocatorBenchmark") {
  AllocatorTestSuite::PrintTestHeader();
  FullAllocatorTestThreaded(1);
  /*FullAllocatorTestThreaded(2);
  FullAllocatorTestThreaded(4);
  FullAllocatorTestThreaded(8);
  FullAllocatorTestThreaded(16);*/
}
