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

  HERMES_ERROR_HANDLE_START()

  PosixShmMmap backend;
  if (rank == 0) {
    {
      std::cout << "Creating SHMEM (rank 0)" << std::endl;
      if(!backend.shm_init(MEGABYTES(1), shm_url)) {
        throw std::runtime_error("Couldn't create backend");
      }
      std::cout << "Backend data: " << (void*)backend.data_ << std::endl;
      std::cout << "Backend sz: " << backend.data_size_ << std::endl;
      memset(backend.data_, nonce, backend.data_size_);
      std::cout << "Wrote backend data" << std::endl;
    }
  }
  MPI_Barrier(MPI_COMM_WORLD);
  if (rank != 0) {
    {
      std::cout << "Attaching SHMEM (rank 1)" << std::endl;
      backend.shm_deserialize(shm_url);
      char *ptr = backend.data_;
      REQUIRE(VerifyBuffer(ptr, backend.data_size_, nonce));
    }
  }
  MPI_Barrier(MPI_COMM_WORLD);
  if (rank == 0) {
    {
      std::cout << "Destroying shmem (rank 1)" << std::endl;
      backend.shm_destroy();
    }
  }

  HERMES_ERROR_HANDLE_END()
}
