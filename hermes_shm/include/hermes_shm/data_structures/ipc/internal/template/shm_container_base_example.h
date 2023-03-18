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
#include "hermes_shm/data_structures/ipc/internal/shm_deserialize.h"

namespace honey {

class ShmContainerExample;

#define CLASS_NAME ShmContainerExample
#define TYPED_CLASS ShmContainerExample
#define TYPED_HEADER ShmHeader<ShmContainerExample>

template<typename T>
class ShmHeader;

template<>
class ShmHeader<ShmContainerExample> : public hipc::ShmBaseHeader {
};

class ShmContainerExample : public hipc::ShmContainer {
 public:
  /**====================================
   * Shm Overrides
   * ===================================*/

  /** Constructor. Empty. */
  explicit CLASS_NAME(TYPED_HEADER *header,
                      hipc::Allocator *alloc) {
    shm_make_header(header, alloc);
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
    memcpy(header_, other.header_, sizeof(*header_));
  }

  /** Check if header is NULL */
  bool IsNull() {
    return header_ == nullptr;
  }

  /** Nullify object header */
  void SetNull() {
  }

 public:
  /**====================================
   * Variables & Types
   * ===================================*/

  typedef TYPED_HEADER header_t; /** Header type query */
  header_t *header_; /**< Header of the shared-memory data structure */
  hipc::Allocator *alloc_; /**< hipc::Allocator used for this data structure */

  /**====================================
   * Constructors
   * ===================================*/

  /** Default constructor. Deleted. */
  CLASS_NAME() = delete;

  /** Move constructor. Deleted. */
  CLASS_NAME(CLASS_NAME &&other) = delete;

  /** Copy constructor. Deleted. */
  CLASS_NAME(const CLASS_NAME &other) = delete;

  /** Initialize header + allocator */
  void shm_init_header(TYPE_UNWRAP(TYPED_HEADER) *header,
                       hipc::Allocator *alloc) {
    header_ = header;
    hipc::Allocator::ConstructObj<TYPE_UNWRAP(TYPED_HEADER)>(*header_);
    alloc_ = alloc;
  }

  /**====================================
   * Destructor
   * ===================================*/

  /** Destructor. */
  ~CLASS_NAME() = default;

  /** Destruction operation */
  void shm_destroy() {
    if (IsNull()) { return; }
    shm_destroy_main();
    SetNull();
  }

  /**====================================
   * Serialization
   * ===================================*/

  /** Serialize into a Pointer */
  void shm_serialize(hipc::TypedPointer<TYPED_CLASS> &ar) const {
    ar = alloc_->template
      Convert<TYPED_HEADER, hipc::Pointer>(header_);
  }

  /** Serialize into an AtomicPointer */
  void shm_serialize(hipc::TypedAtomicPointer<TYPED_CLASS> &ar) const {
    ar = alloc_->template
      Convert<TYPED_HEADER, hipc::AtomicPointer>(header_);
  }

  /**====================================
   * Deserialization
   * ===================================*/

  /** Deserialize object from a raw pointer */
  bool shm_deserialize(const hipc::TypedPointer<TYPED_CLASS> &ar) {
    return shm_deserialize(
      HERMES_MEMORY_REGISTRY->GetAllocator(ar.allocator_id_),
      ar.ToOffsetPointer()
    );
  }

  /** Deserialize object from allocator + offset */
  bool shm_deserialize(hipc::Allocator *alloc, hipc::OffsetPointer header_ptr) {
    if (header_ptr.IsNull()) { return false; }
    return shm_deserialize(alloc->Convert<
                             TYPED_HEADER,
                             hipc::OffsetPointer>(header_ptr),
                           alloc);
  }

  /** Deserialize object from "Deserialize" object */
  bool shm_deserialize(hipc::ShmDeserialize<TYPED_CLASS> other) {
    return shm_deserialize(other.header_, other.alloc_);
  }

  /** Deserialize object from allocator + header */
  bool shm_deserialize(TYPED_HEADER *header,
                       hipc::Allocator *alloc) {
    header_ = header;
    alloc_ = alloc;
    shm_deserialize_main();
    return true;
  }

  /** Constructor. Deserialize the object from the reference. */
  explicit CLASS_NAME(hipc::Ref<TYPED_CLASS> &obj) {
    shm_deserialize(obj->header_, obj->GetAllocator());
  }

  /** Constructor. Deserialize the object deserialize reference. */
  explicit CLASS_NAME(hipc::ShmDeserialize<TYPED_CLASS> other) {
    shm_deserialize(other);
  }

  /**====================================
   * Header Operations
   * ===================================*/

  /** Get a typed pointer to the object */
  template<typename POINTER_T>
  POINTER_T GetShmPointer() const {
    return alloc_->Convert<TYPED_HEADER, POINTER_T>(header_);
  }

  /** Get a ShmDeserialize object */
  hipc::ShmDeserialize<CLASS_NAME> GetShmDeserialize() const {
    return hipc::ShmDeserialize<CLASS_NAME>(header_, alloc_);
  }

  /**====================================
   * Query Operations
   * ===================================*/

  /** Get the allocator for this container */
  hipc::Allocator* GetAllocator() {
    return alloc_;
  }

  /** Get the allocator for this container */
  hipc::Allocator* GetAllocator() const {
    return alloc_;
  }

  /** Get the shared-memory allocator id */
  hipc::allocator_id_t GetAllocatorId() const {
    return alloc_->GetId();
  }
};

}  // namespace hermes_shm::ipc

#undef CLASS_NAME
#undef TYPED_CLASS
#undef TYPED_HEADER

#endif //HERMES_INCLUDE_HERMES_DATA_STRUCTURES_INTERNAL_SHM_CONTAINER_EXAMPLE_H_
