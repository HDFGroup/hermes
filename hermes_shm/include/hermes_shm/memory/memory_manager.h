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

#ifndef HERMES_SHM_MEMORY_MEMORY_MANAGER_H_
#define HERMES_SHM_MEMORY_MEMORY_MANAGER_H_

#include "hermes_shm/memory/allocator/allocator.h"
#include "backend/memory_backend.h"
#include "hermes_shm/memory/allocator/allocator_factory.h"
#include <hermes_shm/constants/data_structure_singleton_macros.h>

namespace lipc = hermes_shm::ipc;

namespace hermes_shm::ipc {

class MemoryManager {
 private:
  allocator_id_t root_allocator_id_;
  PosixMmap root_backend_;
  StackAllocator root_allocator_;
  std::unordered_map<std::string, std::unique_ptr<MemoryBackend>> backends_;
  std::unordered_map<allocator_id_t, std::unique_ptr<Allocator>> allocators_;
  Allocator *default_allocator_;

 public:
  /** The default amount of memory a single allocator manages */
  static const size_t kDefaultBackendSize = GIGABYTES(64);

  /**
   * Constructor. Create the root allocator and backend, which is used
   * until the user specifies a new default. The root allocator stores
   * only private memory.
   * */
  MemoryManager() {
    root_allocator_id_.bits_.major_ = 0;
    root_allocator_id_.bits_.minor_ = -1;
    root_backend_.shm_init(HERMES_SHM_SYSTEM_INFO->ram_size_);
    root_backend_.Own();
    root_allocator_.shm_init(&root_backend_, root_allocator_id_, 0);
    default_allocator_ = &root_allocator_;
  }

  /**
   * Create a memory backend. Memory backends are divided into slots.
   * Each slot corresponds directly with a single allocator.
   * There can be multiple slots per-backend, enabling multiple allocation
   * policies over a single memory region.
   * */
  template<typename BackendT, typename ...Args>
  MemoryBackend* CreateBackend(size_t size,
                               const std::string &url,
                               Args&& ...args) {
    backends_.emplace(url,
                      MemoryBackendFactory::shm_init<BackendT>(size, url),
                      std::forward<Args>(args)...);
    auto backend = backends_[url].get();
    backend->Own();
    return backend;
  }

  /**
   * Attaches to an existing memory backend located at \a url url.
   * */
  MemoryBackend* AttachBackend(MemoryBackendType type,
                               const std::string &url) {
    backends_.emplace(url, MemoryBackendFactory::shm_deserialize(type, url));
    auto backend = backends_[url].get();
    ScanBackends();
    backend->Disown();
    return backend;
  }

  /**
   * Returns a pointer to a backend that has already been attached.
   * */
  MemoryBackend* GetBackend(const std::string &url);

  /**
   * Destroys the memory allocated by the entire backend.
   * */
  void DestroyBackend(const std::string &url);

  /**
   * Scans all attached backends for new memory allocators.
   * */
  void ScanBackends();

  /**
   * Registers an allocator. Used internally by ScanBackends, but may
   * also be used externally.
   * */
  void RegisterAllocator(std::unique_ptr<Allocator> &alloc);

  /**
   * Create and register a memory allocator for a particular backend.
   * */
  template<typename AllocT, typename ...Args>
  Allocator* CreateAllocator(const std::string &url,
                             allocator_id_t alloc_id,
                             size_t custom_header_size,
                             Args&& ...args) {
    auto backend = GetBackend(url);
    if (alloc_id.IsNull()) {
      alloc_id = allocator_id_t(HERMES_SHM_SYSTEM_INFO->pid_,
                                allocators_.size());
    }
    auto alloc = AllocatorFactory::shm_init<AllocT>(
      backend, alloc_id, custom_header_size, std::forward<Args>(args)...);
    RegisterAllocator(alloc);
    return GetAllocator(alloc_id);
  }

  /**
   * Locates an allocator of a particular id
   * */
  Allocator* GetAllocator(allocator_id_t alloc_id) {
    if (alloc_id.IsNull()) { return nullptr; }
    if (alloc_id == root_allocator_.GetId()) {
      return &root_allocator_;
    }
    auto iter = allocators_.find(alloc_id);
    if (iter == allocators_.end()) {
      ScanBackends();
    }
    return reinterpret_cast<Allocator*>(allocators_[alloc_id].get());
  }

  /**
   * Gets the allocator used for initializing other allocators.
   * */
  Allocator* GetRootAllocator() {
    return &root_allocator_;
  }

  /**
   * Gets the allocator used by default when no allocator is
   * used to construct an object.
   * */
  Allocator* GetDefaultAllocator() {
    return default_allocator_;
  }

  /**
   * Sets the allocator used by default when no allocator is
   * used to construct an object.
   * */
  void SetDefaultAllocator(Allocator *alloc) {
    default_allocator_ = alloc;
  }

  /**
   * Convert a process-independent pointer into a process-specific pointer.
   * */
  template<typename T, typename POINTER_T=Pointer>
  T* Convert(const POINTER_T &p) {
    if (p.IsNull()) {
      return nullptr;
    }
    return GetAllocator(p.allocator_id_)->template
      Convert<T, POINTER_T>(p);
  }

  /**
   * Convert a process-specific pointer into a process-independent pointer
   *
   * @param allocator_id the allocator the pointer belongs to
   * @param ptr the pointer to convert
   * */
  template<typename T, typename POINTER_T=Pointer>
  POINTER_T Convert(allocator_id_t allocator_id, T *ptr) {
    return GetAllocator(allocator_id)->template
      Convert<T, POINTER_T>(ptr);
  }

  /**
   * Convert a process-specific pointer into a process-independent pointer when
   * the allocator is unkown.
   *
   * @param ptr the pointer to convert
   * */
  template<typename T, typename POINTER_T=Pointer>
  POINTER_T Convert(T *ptr) {
    for (auto &[alloc_id, alloc] : allocators_) {
      if (alloc->ContainsPtr(ptr)) {
        return alloc->template
          Convert<T, POINTER_T>(ptr);
      }
    }
    return Pointer::GetNull();
  }
};

}  // namespace hermes_shm::ipc

#endif  // HERMES_SHM_MEMORY_MEMORY_MANAGER_H_
