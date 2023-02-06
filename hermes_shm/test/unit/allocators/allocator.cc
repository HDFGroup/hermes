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


#include "test_init.h"
#include "hermes_shm/memory/allocator/stack_allocator.h"
#include "hermes_shm/memory/allocator/multi_page_allocator.h"

void PageAllocationTest(Allocator *alloc) {
  int count = 1024;
  size_t page_size = KILOBYTES(4);
  auto mem_mngr = HERMES_SHM_MEMORY_MANAGER;

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
  size_t alloc_sizes[] = {
    64, 128, 256,
    KILOBYTES(1), KILOBYTES(4), KILOBYTES(64),
    MEGABYTES(1), MEGABYTES(16), MEGABYTES(32)
  };

  // Allocate and free pages between 64 bytes and 1MB
  {
    REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
    for (size_t r = 0; r < 10; ++r) {
      for (size_t i = 0; i < 1000; ++i) {
        Pointer ps[4];
        for (size_t j = 0; j < 4; ++j) {
          ps[j] = alloc->Allocate(alloc_sizes[i % 9]);
        }
        for (size_t j = 0; j < 4; ++j) {
          alloc->Free(ps[j]);
        }
      }
    }
    REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  }

  // Aligned allocate 4KB pages
  {
    for (size_t i = 0; i < 1024; ++i) {
      Pointer p = alloc->AlignedAllocate(KILOBYTES(4), KILOBYTES(4));
      memset(alloc->Convert<void>(p), 0, KILOBYTES(4));
      alloc->Free(p);
    }
  }

  // Reallocate a 4KB page to 16KB
  {
    Pointer p = alloc->Allocate(KILOBYTES(4));
    alloc->Reallocate(p, KILOBYTES(16));
    memset(alloc->Convert<void>(p), 0, KILOBYTES(16));
    alloc->Free(p);
  }
}

TEST_CASE("StackAllocator") {
  auto alloc = Pretest<lipc::PosixShmMmap, lipc::StackAllocator>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  PageAllocationTest(alloc);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  Posttest();
}

TEST_CASE("MultiPageAllocator") {
  auto alloc = Pretest<lipc::PosixShmMmap, lipc::MultiPageAllocator>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  PageAllocationTest(alloc);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);

  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  MultiPageAllocationTest(alloc);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);

  Posttest();
}

TEST_CASE("MallocAllocator") {
  auto alloc = Pretest<lipc::NullBackend, lipc::MultiPageAllocator>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  PageAllocationTest(alloc);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);

  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  MultiPageAllocationTest(alloc);
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);

  Posttest();
}