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


#include <hermes_shm/memory/memory_manager.h>
#include "hermes_shm/memory/backend/memory_backend_factory.h"
#include "hermes_shm/memory/allocator/allocator_factory.h"
#include <hermes_shm/introspect/system_info.h>
#include <hermes_shm/constants/singleton_macros.h>

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
