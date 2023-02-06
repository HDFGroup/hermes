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
