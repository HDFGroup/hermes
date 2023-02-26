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

#ifndef HERMES_MEMORY_MEMORY_MANAGER_H_
#define HERMES_MEMORY_MEMORY_MANAGER_H_

#include "hermes_shm/memory/backend/memory_backend_factory.h"
#include "hermes_shm/memory/allocator/allocator_factory.h"
#include "hermes_shm/memory/memory_registry.h"
#include "hermes_shm/constants/macros.h"
#include <hermes_shm/constants/data_structure_singleton_macros.h>

namespace hipc = hermes_shm::ipc;

namespace hermes_shm::ipc {

class MemoryManager {
 public:
  /** The default amount of memory a single allocator manages */
  static const size_t kDefaultBackendSize = GIGABYTES(64);

  /** Default constructor. */
  MemoryManager() = default;

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
    auto backend_u = MemoryBackendFactory::shm_init<BackendT>(
      size, url, std::forward<Args>(args)...);
    auto backend = HERMES_MEMORY_REGISTRY->RegisterBackend(url, backend_u);
    backend->Own();
    return backend;
  }

  /**
   * Attaches to an existing memory backend located at \a url url.
   * */
  MemoryBackend* AttachBackend(MemoryBackendType type,
                               const std::string &url) {
    auto backend_u = MemoryBackendFactory::shm_deserialize(type, url);
    auto backend = HERMES_MEMORY_REGISTRY->RegisterBackend(url, backend_u);
    ScanBackends();
    backend->Disown();
    return backend;
  }

  /**
   * Returns a pointer to a backend that has already been attached.
   * */
  MemoryBackend* GetBackend(const std::string &url) {
    return HERMES_MEMORY_REGISTRY->GetBackend(url);
  }

  /**
   * Unregister backend
   * */
  void UnregisterBackend(const std::string &url) {
    HERMES_MEMORY_REGISTRY->UnregisterBackend(url);
  }

  /**
   * Destroy backend
   * */
  void DestroyBackend(const std::string &url) {
    auto backend = GetBackend(url);
    backend->Own();
    UnregisterBackend(url);
  }

  /**
   * Scans all attached backends for new memory allocators.
   * */
  void ScanBackends();

  /**
   * Registers an allocator. Used internally by ScanBackends, but may
   * also be used externally.
   * */
  void RegisterAllocator(std::unique_ptr<Allocator> &alloc) {
    HERMES_MEMORY_REGISTRY->RegisterAllocator(alloc);
  }

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
      alloc_id = allocator_id_t(HERMES_SYSTEM_INFO->pid_,
                                HERMES_MEMORY_REGISTRY->allocators_.size());
    }
    auto alloc = AllocatorFactory::shm_init<AllocT>(
      alloc_id, custom_header_size, backend, std::forward<Args>(args)...);
    RegisterAllocator(alloc);
    return GetAllocator(alloc_id);
  }

  /**
   * Locates an allocator of a particular id
   * */
  Allocator* GetAllocator(allocator_id_t alloc_id) {
    auto alloc = HERMES_MEMORY_REGISTRY->GetAllocator(alloc_id);
    if (!alloc) {
      ScanBackends();
    }
    return HERMES_MEMORY_REGISTRY->GetAllocator(alloc_id);
  }

  /**
   * Gets the allocator used for initializing other allocators.
   * */
  Allocator* GetRootAllocator() {
    return HERMES_MEMORY_REGISTRY->GetRootAllocator();
  }

  /**
   * Gets the allocator used by default when no allocator is
   * used to construct an object.
   * */
  Allocator* GetDefaultAllocator() {
    return HERMES_MEMORY_REGISTRY->GetDefaultAllocator();
  }

  /**
   * Sets the allocator used by default when no allocator is
   * used to construct an object.
   * */
  void SetDefaultAllocator(Allocator *alloc) {
    HERMES_MEMORY_REGISTRY->SetDefaultAllocator(alloc);
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
    for (auto &[alloc_id, alloc] : HERMES_MEMORY_REGISTRY->allocators_) {
      if (alloc->ContainsPtr(ptr)) {
        return alloc->template
          Convert<T, POINTER_T>(ptr);
      }
    }
    return Pointer::GetNull();
  }
};

}  // namespace hermes_shm::ipc

#endif  // HERMES_MEMORY_MEMORY_MANAGER_H_
