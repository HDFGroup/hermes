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

#ifndef HERMES_SHM_TEST_UNIT_DATA_STRUCTURES_TEST_INIT_H_
#define HERMES_SHM_TEST_UNIT_DATA_STRUCTURES_TEST_INIT_H_

#include "hermes_shm/data_structures/data_structure.h"
#include <mpi.h>

using hermes_shm::ipc::PosixShmMmap;
using hermes_shm::ipc::MemoryBackendType;
using hermes_shm::ipc::MemoryBackend;
using hermes_shm::ipc::allocator_id_t;
using hermes_shm::ipc::AllocatorType;
using hermes_shm::ipc::Allocator;
using hermes_shm::ipc::Pointer;

using hermes_shm::ipc::MemoryBackendType;
using hermes_shm::ipc::MemoryBackend;
using hermes_shm::ipc::allocator_id_t;
using hermes_shm::ipc::AllocatorType;
using hermes_shm::ipc::Allocator;
using hermes_shm::ipc::MemoryManager;
using hermes_shm::ipc::Pointer;

extern Allocator *alloc_g;

void Posttest();

#endif  // HERMES_SHM_TEST_UNIT_DATA_STRUCTURES_TEST_INIT_H_
