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

#include <mpi.h>
#include "hermes_shm/memory/memory_manager.h"

using hermes_shm::ipc::MemoryBackendType;
using hermes_shm::ipc::MemoryBackend;
using hermes_shm::ipc::allocator_id_t;
using hermes_shm::ipc::AllocatorType;
using hermes_shm::ipc::MemoryManager;

struct SimpleHeader {
  hermes_shm::ipc::Pointer p_;
};

TEST_CASE("MemoryManager") {
  int rank;
  char nonce = 8;
  size_t page_size = KILOBYTES(4);
  std::string shm_url = "test_mem_backend";
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  allocator_id_t alloc_id(0, 1);

  HERMES_SHM_ERROR_HANDLE_START()
  auto mem_mngr = HERMES_SHM_MEMORY_MANAGER;

  if (rank == 0) {
    std::cout << "Creating SHMEM (rank 0): " << shm_url << std::endl;
    mem_mngr->CreateBackend<hipc::PosixShmMmap>(
      MemoryManager::kDefaultBackendSize, shm_url);
    mem_mngr->CreateAllocator<hipc::StackAllocator>(
      shm_url, alloc_id, 0);
  }
  MPI_Barrier(MPI_COMM_WORLD);
  if (rank != 0) {
    std::cout << "Attaching SHMEM (rank 1): " << shm_url << std::endl;
    mem_mngr->AttachBackend(MemoryBackendType::kPosixShmMmap, shm_url);
  }
  MPI_Barrier(MPI_COMM_WORLD);
  if (rank == 0) {
    std::cout << "Allocating pages (rank 0)" << std::endl;
    hipc::Allocator *alloc = mem_mngr->GetAllocator(alloc_id);
    char *page = alloc->AllocatePtr<char>(page_size);
    memset(page, nonce, page_size);
    auto header = alloc->GetCustomHeader<SimpleHeader>();
    hipc::Pointer p1 = mem_mngr->Convert<void>(alloc_id, page);
    hipc::Pointer p2 = mem_mngr->Convert<char>(page);
    header->p_ = p1;
    REQUIRE(p1 == p2);
    REQUIRE(VerifyBuffer(page, page_size, nonce));
  }
  MPI_Barrier(MPI_COMM_WORLD);
  if (rank != 0) {
    std::cout << "Finding and checking pages (rank 1)" << std::endl;
    hipc::Allocator *alloc = mem_mngr->GetAllocator(alloc_id);
    SimpleHeader *header = alloc->GetCustomHeader<SimpleHeader>();
    char *page = alloc->Convert<char>(header->p_);
    REQUIRE(VerifyBuffer(page, page_size, nonce));
  }
  MPI_Barrier(MPI_COMM_WORLD);
  if (rank == 0) {
    mem_mngr->DestroyBackend(shm_url);
  }

  HERMES_SHM_ERROR_HANDLE_END()
}
