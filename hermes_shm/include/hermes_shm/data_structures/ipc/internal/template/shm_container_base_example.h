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

#ifndef HERMES_INCLUDE_HERMES_DATA_STRUCTURES_INTERNAL_SHM_CONTAINER_EXAMPLE_H_
#define HERMES_INCLUDE_HERMES_DATA_STRUCTURES_INTERNAL_SHM_CONTAINER_EXAMPLE_H_

#include "hermes_shm/data_structures/ipc/internal/shm_container.h"
#include "hermes_shm/memory/memory_manager.h"

namespace honey {

class ShmContainerExample;

#define CLASS_NAME ShmContainerExample
#define TYPED_CLASS ShmContainerExample

class ShmContainerExample : public hipc::ShmContainer {
 public:
  /**====================================
   * Shm Overrides
   * ===================================*/

  /** Constructor. Empty. */
  explicit CLASS_NAME(hipc::Allocator *alloc) {
    alloc_id_ = alloc->GetId();
  }

  /** Default initialization */
  void shm_init() {
    SetNull();
  }

  /** Load from shared memory */
  void shm_deserialize_main() {}

  /** Destroy object */
  void shm_destroy_main() {}

  /** Internal copy operation */
  void shm_strong_copy_main(const CLASS_NAME &other) {
  }

  /** Internal move operation */
  void shm_strong_move_main(CLASS_NAME &&other) {
  }

  /** Check if header is NULL */
  bool IsNull() {
  }

  /** Nullify object header */
  void SetNull() {
  }

 public:
  /**====================================
   * Variables & Types
   * ===================================*/
  hipc::allocator_id_t alloc_id_;

  /**====================================
   * Constructors
   * ===================================*/

  /** Default constructor. Deleted. */
  CLASS_NAME() = delete;

  /** Move constructor. Deleted. */
  CLASS_NAME(CLASS_NAME &&other) = delete;

  /** Copy constructor. Deleted. */
  CLASS_NAME(const CLASS_NAME &other) = delete;

  /** Initialize container */
  void shm_init_container(hipc::Allocator *alloc) {
    alloc_id_ = alloc->GetId();
  }

  /**====================================
   * Destructor
   * ===================================*/

  /** Destructor. */
  HSHM_ALWAYS_INLINE ~CLASS_NAME() = default;

  /** Destruction operation */
  HSHM_ALWAYS_INLINE void shm_destroy() {
    if (IsNull()) { return; }
    shm_destroy_main();
    SetNull();
  }

  /**====================================
   * Header Operations
   * ===================================*/

  /** Get a typed pointer to the object */
  template<typename POINTER_T>
  HSHM_ALWAYS_INLINE POINTER_T GetShmPointer() const {
    return GetAllocator()->template Convert<TYPED_CLASS, POINTER_T>(this);
  }

  /**====================================
   * Query Operations
   * ===================================*/

  /** Get the allocator for this container */
  HSHM_ALWAYS_INLINE hipc::Allocator* GetAllocator() const {
    return HERMES_MEMORY_REGISTRY_REF.GetAllocator(alloc_id_);
  }

  /** Get the shared-memory allocator id */
  HSHM_ALWAYS_INLINE hipc::allocator_id_t& GetAllocatorId() const {
    return GetAllocator()->GetId();
  }
};

}  // namespace hshm::ipc

#undef CLASS_NAME
#undef TYPED_CLASS
#undef TYPED_HEADER

#endif //HERMES_INCLUDE_HERMES_DATA_STRUCTURES_INTERNAL_SHM_CONTAINER_EXAMPLE_H_
