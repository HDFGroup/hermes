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

void PageAllocationTest(Allocator *alloc) {
  int count = 1024;
  size_t page_size = KILOBYTES(4);
  auto mem_mngr = HERMES_MEMORY_MANAGER;

  // Allocate pages
  std::vector<Pointer> ps(count);
  void *ptrs[count];
  for (int i = 0; i < count; ++i) {
    ptrs[i] = alloc->AllocatePtr<void>(page_size, ps[i]);
    memset(ptrs[i], i, page_size);
    REQUIRE(ps[i].off_.load() != 0);
    REQUIRE(!ps[i].IsNull());
    REQUIRE(ptrs[i] != nullptr);
  }

  // Convert process pointers into independent pointers
  for (int i = 0; i < count; ++i) {
    Pointer p = mem_mngr->Convert(ptrs[i]);
    REQUIRE(p == ps[i]);
    REQUIRE(VerifyBuffer((char*)ptrs[i], page_size, i));
  }

  // Check the custom header
  auto hdr = alloc->GetCustomHeader<SimpleAllocatorHeader>();
  REQUIRE(hdr->checksum_ == HEADER_CHECKSUM);

  // Free pages
  for (int i = 0; i < count; ++i) {
    alloc->Free(ps[i]);
  }

  // Reallocate pages
  for (int i = 0; i < count; ++i) {
    ptrs[i] = alloc->AllocatePtr<void>(page_size, ps[i]);
    REQUIRE(ps[i].off_.load() != 0);
    REQUIRE(!ps[i].IsNull());
  }

  // Free again
  for (int i = 0; i < count; ++i) {
    alloc->Free(ps[i]);
  }
}

void MultiPageAllocationTest(Allocator *alloc) {
  std::vector<size_t> alloc_sizes = {
    64, 128, 256,
    KILOBYTES(1), KILOBYTES(4), KILOBYTES(64),
    MEGABYTES(1)
  };

  // Allocate and free pages between 64 bytes and 32MB
  {
    for (size_t r = 0; r < 4; ++r) {
      for (size_t i = 0; i < alloc_sizes.size(); ++i) {
        Pointer ps[16];
        for (size_t j = 0; j < 16; ++j) {
          ps[j] = alloc->Allocate(alloc_sizes[i]);
        }
        for (size_t j = 0; j < 16; ++j) {
          alloc->Free(ps[j]);
        }
      }
    }
  }
}

void ReallocationTest(Allocator *alloc) {
  std::vector<std::pair<size_t, size_t>> sizes = {
      {KILOBYTES(3), KILOBYTES(4)},
      {KILOBYTES(4), MEGABYTES(1)}
  };

  // Reallocate a small page to a larger page
  for (auto &[small_size, large_size] : sizes) {
    Pointer p;
    char *ptr = alloc->AllocatePtr<char>(small_size, p);
    memset(ptr, 10, small_size);
    char *new_ptr = alloc->ReallocatePtr<char>(p, large_size);
    for (size_t i = 0; i < small_size; ++i) {
      REQUIRE(ptr[i] == 10);
    }
    memset(new_ptr, 0, large_size);
    alloc->Free(p);
  }
}

void AlignedAllocationTest(Allocator *alloc) {
  std::vector<std::pair<size_t, size_t>> sizes = {
      {KILOBYTES(4), KILOBYTES(4)},
  };

  // Aligned allocate pages
  for (auto &[size, alignment] : sizes) {
    for (size_t i = 0; i < 1024; ++i) {
      Pointer p;
      char *ptr = alloc->AllocatePtr<char>(size, p, alignment);
      REQUIRE(((size_t)ptr % alignment) == 0);
      memset(alloc->Convert<void>(p), 0, size);
      alloc->Free(p);
    }
  }
}

TEST_CASE("StackAllocator") {
  auto alloc = Pretest<hipc::PosixShmMmap, hipc::StackAllocator>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  PageAllocationTest(alloc);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  Posttest();
}

TEST_CASE("MallocAllocator") {
  auto alloc = Pretest<hipc::NullBackend, hipc::MallocAllocator>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  PageAllocationTest(alloc);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);

  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  MultiPageAllocationTest(alloc);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);

  Posttest();
}

TEST_CASE("FixedPageAllocator") {
  auto alloc = Pretest<hipc::PosixShmMmap, hipc::FixedPageAllocator>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  PageAllocationTest(alloc);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);

  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  MultiPageAllocationTest(alloc);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);

  Posttest();
}

TEST_CASE("ScalablePageAllocator") {
  auto alloc = Pretest<hipc::PosixShmMmap, hipc::ScalablePageAllocator>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  PageAllocationTest(alloc);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);

  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  MultiPageAllocationTest(alloc);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);

  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  ReallocationTest(alloc);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);

  Posttest();
}
