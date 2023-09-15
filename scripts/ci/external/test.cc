//
// Created by lukemartinlogan on 6/6/23.
//

#include "hermes_shm/hermes_shm.h"

int main() {
  std::string shm_url = "test_serializers";
  hipc::allocator_id_t alloc_id(0, 1);
  auto mem_mngr = HERMES_MEMORY_MANAGER;
  mem_mngr->UnregisterAllocator(alloc_id);
  mem_mngr->UnregisterBackend(shm_url);
  mem_mngr->CreateBackend<hipc::PosixShmMmap>(
    hipc::MemoryManager::GetDefaultBackendSize(), shm_url);
  mem_mngr->CreateAllocator<hipc::StackAllocator>(shm_url, alloc_id, 0);
}