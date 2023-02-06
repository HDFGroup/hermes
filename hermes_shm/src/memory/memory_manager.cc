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

#include <hermes_shm/memory/memory_manager.h>
#include "hermes_shm/memory/backend/memory_backend_factory.h"
#include "hermes_shm/memory/allocator/allocator_factory.h"
#include <hermes_shm/introspect/system_info.h>

namespace hermes_shm::ipc {

MemoryBackend* MemoryManager::GetBackend(const std::string &url) {
  return backends_[url].get();
}

void MemoryManager::DestroyBackend(const std::string &url) {
  auto backend = GetBackend(url);
  backend->shm_destroy();
  backends_.erase(url);
}

void MemoryManager::ScanBackends() {
  for (auto &[url, backend] : backends_) {
    auto alloc = AllocatorFactory::shm_deserialize(backend.get());
    RegisterAllocator(alloc);
  }
}

void MemoryManager::RegisterAllocator(std::unique_ptr<Allocator> &alloc) {
  if (default_allocator_ == nullptr ||
      default_allocator_ == &root_allocator_) {
    default_allocator_ = alloc.get();
  }
  allocators_.emplace(alloc->GetId(), std::move(alloc));
}

}  // namespace hermes_shm::ipc
