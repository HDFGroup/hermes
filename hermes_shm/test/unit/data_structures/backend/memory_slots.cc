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
#include <iostream>
#include "hermes_shm/memory/backend/posix_shm_mmap.h"

using hermes_shm::ipc::PosixShmMmap;

TEST_CASE("MemorySlot") {
  int rank;
  char nonce = 8;
  std::string shm_url = "test_mem_backend";
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  HERMES_SHM_ERROR_HANDLE_START()

  PosixShmMmap backend;
  if (rank == 0) {
    std::cout << "HERE?" << std::endl;
    SECTION("Creating SHMEM (rank 0)") {
      backend.shm_init(MEGABYTES(1), shm_url);
      memset(backend.data_, nonce, backend.data_size_);
    }
  }
  MPI_Barrier(MPI_COMM_WORLD);
  if (rank != 0) {
    SECTION("Attaching SHMEM (rank 1)") {
      backend.shm_deserialize(shm_url);
      char *ptr = backend.data_;
      REQUIRE(VerifyBuffer(ptr, backend.data_size_, nonce));
    }
  }
  MPI_Barrier(MPI_COMM_WORLD);
  if (rank == 0) {
    SECTION("Destroying shmem (rank 1)") {
      backend.shm_destroy();
    }
  }

  HERMES_SHM_ERROR_HANDLE_END()
}
