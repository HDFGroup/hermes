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

#include "hermes_shm/memory/backend/posix_shm_mmap.h"

using hermes_shm::ipc::PosixShmMmap;

TEST_CASE("BackendReserve") {
  PosixShmMmap b1;

  // Reserve + Map 8GB of memory
  b1.shm_init(GIGABYTES(8), "shmem_test");

  // Set 2GB of SHMEM
  memset(b1.data_, 0, GIGABYTES(2));

  // Destroy SHMEM
  b1.shm_destroy();
}
