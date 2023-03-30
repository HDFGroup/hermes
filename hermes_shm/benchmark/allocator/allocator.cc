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
  static std::stringstream ss_;
  static int test_count_;

  ////////////////////
  /// Test Cases
  ////////////////////

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
    }
  }

  /** Allocate and Free a single size in a single loop */
  void AllocateAndFreeFixedSize(size_t count, size_t size) {
    Timer t;
    t.Resume();
    for (size_t i = 0; i < count; ++i) {
      Pointer p = alloc_->Allocate(size);
      alloc_->Free(p);
    }
#pragma omp barrier
    t.Pause();

    TestOutput("AllocateAndFreeFixedSize", size, t);
  }

  /** Allocate a fixed size in a loop, and then free in another loop */
  void AllocateThenFreeFixedSize(size_t count, size_t size) {
    Timer t;
    std::vector<Pointer> cache(count);
    t.Resume();
    for (size_t i = 0; i < count; ++i) {
      cache[i] = alloc_->Allocate(size);
    }
    for (size_t i = 0; i < count; ++i) {
      alloc_->Free(cache[i]);
    }
#pragma omp barrier
    t.Pause();

    TestOutput("AllocateThenFreeFixedSize", size, t);
  }

  /** Allocate, Free, Reallocate, Free in a loop */

  ////////////////////
  /// Test Output
  ////////////////////

  /** The CSV header */
  void TestOutputHeader() {
    ss_ << "test_name" << ","
        << "alloc_type" << ","
        << "alloc_size" << ","
        << "nthreads" << ","
        << "time" << std::endl;
  }

  /** The CSV test case */
  void TestOutput(const std::string &test_name, size_t obj_size, Timer &t) {
    int rank = omp_get_thread_num();
    if (rank != 0) { return; }
    if (test_count_ == 0) {
      TestOutputHeader();
    }
    ss_ << test_name << ","
        << alloc_type_ << ","
        << obj_size << ","
        << omp_get_num_threads() << ","
        << t.GetMsec() << std::endl;
    ++test_count_;
  }

  /** Print the CSV output */
  static void PrintTestOutput() {
    std::cout << ss_.str() << std::endl;
  }
};

/** The output text */
std::stringstream AllocatorTestSuite::ss_;

/** Number of tests currently conducted */
int AllocatorTestSuite::test_count_ = 0;

/** The minor number to use for allocators */
static int minor = 1;

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
    std::string shm_url = "test_allocators";
    mem_mngr->CreateBackend<BackendT>(
      MemoryManager::kDefaultBackendSize, shm_url);
    alloc = mem_mngr->CreateAllocator<AllocT>(
      shm_url, alloc_id, 0, std::forward<Args>(args)...);
  }
#pragma omp barrier
  if (rank != 0){
    // Retrieve the allocator + backend
    alloc = mem_mngr->GetAllocator(alloc_id);
  }

# pragma omp barrier
  if (rank == 0) {
    minor += 1;
  }

  return alloc;
}

/** Destroy the allocator + backend from the test */
void Posttest() {
  int rank = omp_get_thread_num();
#pragma omp barrier
  if (rank == 0) {
    std::string shm_url = "test_allocators";
    auto mem_mngr = HERMES_MEMORY_MANAGER;
  }
}

/** A series of allocator benchmarks for a particular thread */
template<typename BackendT, typename AllocT, typename ...Args>
void AllocatorTest(AllocatorType alloc_type,
                   MemoryBackendType backend_type,
                   Args&& ...args) {
  Allocator *alloc = Pretest<BackendT, AllocT>(
    backend_type, std::forward<Args>(args)...);
  // Allocate many, and then free many
  AllocatorTestSuite(alloc_type, alloc).AllocateThenFreeFixedSize(
    (2<<10), MEGABYTES(1));
  // Allocate and free immediately
  AllocatorTestSuite(alloc_type, alloc).AllocateAndFreeFixedSize(
    (2<<10), MEGABYTES(1));
  Posttest();
}

/** Test different allocators on a particular thread */
void FullAllocatorTestPerThread() {
  // Malloc allocator
  AllocatorTest<hipc::NullBackend, hipc::MallocAllocator>(
    AllocatorType::kMallocAllocator,
    MemoryBackendType::kNullBackend);
  // Stack allocator
  AllocatorTest<hipc::PosixShmMmap, hipc::StackAllocator>(
    AllocatorType::kStackAllocator,
    MemoryBackendType::kPosixShmMmap);
  // Fixed page allocator
  AllocatorTest<hipc::PosixShmMmap, hipc::FixedPageAllocator>(
    AllocatorType::kFixedPageAllocator,
    MemoryBackendType::kPosixShmMmap);
}

/** Spawn multiple threads and run allocator tests */
void FullAllocatorTestThreaded(int nthreads) {
  HERMES_THREAD_MANAGER->GetThreadStatic();

  omp_set_dynamic(0);
#pragma omp parallel num_threads(nthreads)
  {
#pragma omp barrier
    FullAllocatorTestPerThread();
#pragma omp barrier
  }
}

TEST_CASE("AllocatorBenchmark") {
  FullAllocatorTestThreaded(1);
  /*FullAllocatorTestThreaded(2);
  FullAllocatorTestThreaded(4);
  FullAllocatorTestThreaded(8);*/
  AllocatorTestSuite::PrintTestOutput();
}
