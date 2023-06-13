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
#include "basic_test.h"

struct Record {
  char *data;
  size_t size;
  Pointer ptr;
};

void MpiPageAllocationTest(Allocator *alloc, size_t count) {
  size_t window_length = 32;
  size_t min_page = 64;
  size_t max_page = MEGABYTES(1);
  std::mt19937 rng(23522523);
  std::uniform_int_distribution<size_t> uni(min_page, max_page);

  MPI_Barrier(MPI_COMM_WORLD);
  size_t num_windows = count / window_length;
  std::vector<Record> window(window_length);
  for (size_t w = 0; w < num_windows; ++w) {
    for (size_t i = 0; i < window_length; ++i) {
      window[i].size = uni(rng);
      window[i].data = alloc->AllocatePtr<char>(window[i].size,
                                                window[i].ptr);
      memset(window[i].data, (char)i, window[i].size);
    }
    for (size_t i = 0; i < window_length; ++i) {
      VerifyBuffer(window[i].data, window[i].size, (char)i);
      alloc->Free(window[i].ptr);
    }
  }
  MPI_Barrier(MPI_COMM_WORLD);
}

template<typename AllocT>
Allocator* TestAllocatorMpi() {
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  if (rank == 0) {
    PretestRank0<AllocT>();
  }
  MPI_Barrier(MPI_COMM_WORLD);
  if (rank != 0) {
    PretestRankN();
  }
  HILOG(kInfo, "Allocator: {}", (size_t)alloc_g);
  return alloc_g;
}

TEST_CASE("StackAllocatorMpi") {
  auto alloc = TestAllocatorMpi<hipc::StackAllocator>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  MpiPageAllocationTest(alloc, 100);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("ScalablePageAllocatorMpi") {
  auto alloc = TestAllocatorMpi<hipc::ScalablePageAllocator>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  MpiPageAllocationTest(alloc, 1000);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}
