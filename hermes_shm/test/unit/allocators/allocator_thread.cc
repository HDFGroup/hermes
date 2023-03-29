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


#include "test_init.h"

void MultiThreadedPageAllocationTest(Allocator *alloc) {
  int nthreads = 8;
  HERMES_THREAD_MANAGER->GetThreadStatic();

  omp_set_dynamic(0);
#pragma omp parallel shared(alloc) num_threads(nthreads)
  {
#pragma omp barrier
    PageAllocationTest(alloc);
#pragma omp barrier
    try {
      MultiPageAllocationTest(alloc);
    } catch (std::shared_ptr<hshm::Error> &err) {
      err->print();
      exit(1);
    }
#pragma omp barrier
  }
}

TEST_CASE("StackAllocatorMultithreaded") {
  auto alloc = Pretest<hipc::PosixShmMmap, hipc::StackAllocator>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  MultiThreadedPageAllocationTest(alloc);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  Posttest();
}

TEST_CASE("ScalablePageAllocatorMultithreaded") {
  auto alloc = Pretest<hipc::PosixShmMmap, hipc::ScalablePageAllocator>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  MultiThreadedPageAllocationTest(alloc);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  Posttest();
}
