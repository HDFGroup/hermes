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

#ifndef HERMES_SHM_TEST_UNIT_ALLOCATORS_TEST_INIT_H_
#define HERMES_SHM_TEST_UNIT_ALLOCATORS_TEST_INIT_H_

#include "basic_test.h"
#include "omp.h"
#include "hermes_shm/memory/memory_manager.h"

using hermes_shm::ipc::MemoryBackendType;
using hermes_shm::ipc::MemoryBackend;
using hermes_shm::ipc::allocator_id_t;
using hermes_shm::ipc::AllocatorType;
using hermes_shm::ipc::Allocator;
using hermes_shm::ipc::MemoryManager;
using hermes_shm::ipc::Pointer;

#define HEADER_CHECKSUM 8482942

struct SimpleAllocatorHeader {
  int checksum_;
};

template<typename BackendT, typename AllocT>
Allocator* Pretest() {
  std::string shm_url = "test_allocators";
  allocator_id_t alloc_id(0, 1);
  auto mem_mngr = HERMES_SHM_MEMORY_MANAGER;
  mem_mngr->CreateBackend<BackendT>(
    MemoryManager::kDefaultBackendSize, shm_url);
  mem_mngr->CreateAllocator<AllocT>(
    shm_url, alloc_id, sizeof(SimpleAllocatorHeader));
  auto alloc = mem_mngr->GetAllocator(alloc_id);
  auto hdr = alloc->GetCustomHeader<SimpleAllocatorHeader>();
  hdr->checksum_ = HEADER_CHECKSUM;
  return alloc;
}

void Posttest();
void PageAllocationTest(Allocator *alloc);

#endif  // HERMES_SHM_TEST_UNIT_ALLOCATORS_TEST_INIT_H_
