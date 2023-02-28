//
// Created by lukemartinlogan on 2/21/23.
//

#include "hermes_shm/memory/memory_registry.h"
#include "hermes_shm/memory/allocator/stack_allocator.h"
#include "hermes_shm/memory/backend/posix_mmap.h"

namespace hermes_shm::ipc {

MemoryRegistry::MemoryRegistry() {
  root_allocator_id_.bits_.major_ = 0;
  root_allocator_id_.bits_.minor_ = -1;
  auto root_backend = std::make_unique<PosixMmap>();
  auto root_allocator = std::make_unique<StackAllocator>();
  root_backend->shm_init(MEGABYTES(128));
  root_backend->Own();
  root_allocator->shm_init(root_allocator_id_, 0,
                           root_backend->data_,
                           root_backend->data_size_);
  root_backend_ = std::move(root_backend);
  root_allocator_ = std::move(root_allocator);
  default_allocator_ = root_allocator_.get();
}

}  // hermes_shm::ipc