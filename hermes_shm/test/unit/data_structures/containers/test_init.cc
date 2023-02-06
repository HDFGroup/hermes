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
#include <mpi.h>

#include "hermes_shm/memory/allocator/stack_allocator.h"

Allocator *alloc_g = nullptr;

void Posttest() {
  std::string shm_url = "test_allocators";
  auto mem_mngr = HERMES_SHM_MEMORY_MANAGER;
  mem_mngr->DestroyBackend(shm_url);
  alloc_g = nullptr;
}

void MainPretest() {
  Pretest<lipc::StackAllocator>();
}

void MainPosttest() {
  Posttest();
}
