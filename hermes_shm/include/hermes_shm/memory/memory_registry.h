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

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_MEMORY_MEMORY_REGISTRY_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_MEMORY_MEMORY_REGISTRY_H_

#include "hermes_shm/memory/allocator/allocator.h"
#include "backend/memory_backend.h"
#include "hermes_shm/memory/allocator/stack_allocator.h"
#include "hermes_shm/memory/backend/posix_mmap.h"
#include "hermes_shm/util/errors.h"

namespace hipc = hshm::ipc;

namespace hshm::ipc {

#define MAX_ALLOCATORS 64

class MemoryRegistry {
 public:
  allocator_id_t root_allocator_id_;
  PosixMmap root_backend_;
  StackAllocator root_allocator_;
  std::unordered_map<std::string, std::unique_ptr<MemoryBackend>> backends_;
  std::unique_ptr<Allocator> allocators_made_[MAX_ALLOCATORS];
  Allocator *allocators_[MAX_ALLOCATORS];
  Allocator *default_allocator_;

 public:
  /**
   * Constructor. Create the root allocator and backend, which is used
   * until the user specifies a new default. The root allocator stores
   * only private memory.
   * */
  MemoryRegistry();

  /**
   * Register a unique memory backend. Throws an exception if the backend
   * already exists. This is because unregistering a backend can cause
   * ramifications across allocators.
   *
   * @param url the backend's unique identifier
   * @param backend the backend to register
   * */
  HSHM_ALWAYS_INLINE MemoryBackend* RegisterBackend(
      const std::string &url,
      std::unique_ptr<MemoryBackend> &backend) {
    auto ptr = backend.get();
    if (GetBackend(url)) {
      throw MEMORY_BACKEND_REPEATED.format();
    }
    backends_.emplace(url, std::move(backend));
    return ptr;
  }

  /** Unregister memory backend */
  HSHM_ALWAYS_INLINE void UnregisterBackend(const std::string &url) {
    backends_.erase(url);
  }

  /**
   * Returns a pointer to a backend that has already been attached.
   * */
  HSHM_ALWAYS_INLINE MemoryBackend* GetBackend(const std::string &url) {
    auto iter = backends_.find(url);
    if (iter == backends_.end()) {
      return nullptr;
    }
    return (*iter).second.get();
  }

  /** Registers an allocator. */
  HSHM_ALWAYS_INLINE Allocator* RegisterAllocator(
      std::unique_ptr<Allocator> &alloc) {
    if (default_allocator_ == nullptr ||
      default_allocator_ == &root_allocator_ ||
      default_allocator_->GetId() == alloc->GetId()) {
      default_allocator_ = alloc.get();
    }
    RegisterAllocator(alloc.get());
    auto idx = alloc->GetId().ToIndex();
    auto &alloc_made = allocators_made_[idx];
    alloc_made = std::move(alloc);
    return alloc_made.get();
  }

  /** Registers an allocator. */
  HSHM_ALWAYS_INLINE void RegisterAllocator(Allocator *alloc) {
    auto idx = alloc->GetId().ToIndex();
    allocators_[idx] = alloc;
  }

  /** Unregisters an allocator */
  HSHM_ALWAYS_INLINE void UnregisterAllocator(allocator_id_t alloc_id) {
    if (alloc_id == default_allocator_->GetId()) {
      default_allocator_ = &root_allocator_;
    }
    allocators_made_[alloc_id.ToIndex()] = nullptr;
    allocators_[alloc_id.ToIndex()] = nullptr;
  }

  /**
   * Locates an allocator of a particular id
   * */
  HSHM_ALWAYS_INLINE Allocator* GetAllocator(allocator_id_t alloc_id) {
    return allocators_[alloc_id.ToIndex()];
  }

  /**
   * Gets the allocator used for initializing other allocators.
   * */
  HSHM_ALWAYS_INLINE Allocator* GetRootAllocator() {
    return &root_allocator_;
  }

  /**
   * Gets the allocator used by default when no allocator is
   * used to construct an object.
   * */
  HSHM_ALWAYS_INLINE Allocator* GetDefaultAllocator() {
    return reinterpret_cast<Allocator*>(default_allocator_);
  }

  /**
   * Sets the allocator used by default when no allocator is
   * used to construct an object.
   * */
  HSHM_ALWAYS_INLINE void SetDefaultAllocator(Allocator *alloc) {
    default_allocator_ = alloc;
  }
};

}  // namespace hshm::ipc

#endif  // HERMES_SHM_INCLUDE_HERMES_SHM_MEMORY_MEMORY_REGISTRY_H_
