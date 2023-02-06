//
// Created by lukemartinlogan on 1/24/23.
//

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_INTERNAL_SHM_CONTAINER_EXAMPLE_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_INTERNAL_SHM_CONTAINER_EXAMPLE_H_

#include "hermes_shm/data_structures/internal/shm_container.h"

namespace honey {

class ShmContainerExample;

#define CLASS_NAME ShmContainerExample
#define TYPED_CLASS ShmContainerExample
#define TYPED_HEADER ShmHeader<ShmContainerExample>

template<typename T>
class ShmHeader;

template<>
 class ShmHeader<ShmContainerExample> : public lipc::ShmBaseHeader {
};

class ShmContainerExample {
 public:
  /**====================================
   * Variables & Types
   * ===================================*/

  typedef TYPED_HEADER header_t; /** Header type query */
  header_t *header_; /**< Header of the shared-memory data structure */
  lipc::Allocator *alloc_; /**< lipc::Allocator used for this data structure */
  hermes_shm::bitfield32_t flags_; /**< Flags used data structure status */

 public:
  /**====================================
   * Shm Overrides
   * ===================================*/

  /** Default constructor */
  CLASS_NAME() = default;

  /** Default shm constructor */
  void shm_init_main(TYPED_HEADER *header,
                     lipc::Allocator *alloc) {
    shm_init_allocator(alloc);
    shm_init_header(header);
  }

  /** Move constructor */
  void shm_weak_move_main(TYPED_HEADER *header,
                          lipc::Allocator *alloc, 
                          CLASS_NAME &other) {
    shm_init_main(header, alloc);
  }

  /** Copy constructor */
  void shm_strong_copy_main(TYPED_HEADER *header,
                            lipc::Allocator *alloc, 
                            const CLASS_NAME &other) {
    shm_init_main(header, alloc);
  }

  /** Destroy the shared-memory data. */
  void shm_destroy_main() {}

  /** Store into shared memory */
  void shm_serialize_main() const {}

  /** Load from shared memory */
  void shm_deserialize_main() {}

  /**====================================
   * Constructors
   * ===================================*/

  /** Constructor. Allocate header with default allocator. */
  template<typename ...Args>
  explicit CLASS_NAME(Args&& ...args) {
    shm_init(std::forward<Args>(args)...);
  }

  /** Constructor. Allocate header with default allocator. */
  template<typename ...Args>
  void shm_init(Args&& ...args) {
    shm_destroy(false);
    shm_init_main(lipc::typed_nullptr<TYPED_HEADER>(),
                  lipc::typed_nullptr<lipc::Allocator>(),
                  std::forward<Args>(args)...);
  }

  /** Constructor. Allocate header with specific allocator. */
  template<typename ...Args>
  void shm_init(lipc::Allocator *alloc, Args&& ...args) {
    shm_destroy(false);
    shm_init_main(lipc::typed_nullptr<TYPED_HEADER>(),
                  alloc,
                  std::forward<Args>(args)...);
  }

  /** Constructor. Initialize an already-allocated header. */
  template<typename ...Args>
  void shm_init(TYPED_HEADER &header,
                lipc::Allocator *alloc, Args&& ...args) {
    shm_destroy(false);
    shm_init_main(&header, alloc, std::forward<Args>(args)...);
  }

  /** Initialize the data structure's allocator */
  inline void shm_init_allocator(lipc::Allocator *alloc) {
    if (IsValid()) { return; }
    if (alloc == nullptr) {
      alloc_ = HERMES_SHM_MEMORY_MANAGER->GetDefaultAllocator();
    } else {
      alloc_ = alloc;
    }
  }

  /**
   * Initialize a data structure's header.
   * A container will never re-set or re-allocate its header once it has
   * been set the first time.
   * */
  template<typename ...Args>
  void shm_init_header(TYPED_HEADER *header,
                       Args&& ...args) {
    if (IsValid()) {
      header_->SetBits(SHM_CONTAINER_DATA_VALID);
    } else if (header == nullptr) {
      lipc::Pointer p;
      header_ = alloc_->template
        AllocateConstructObjs<TYPED_HEADER>(
        1, p, std::forward<Args>(args)...);
      header_->SetBits(
        SHM_CONTAINER_DATA_VALID |
          SHM_CONTAINER_HEADER_DESTRUCTABLE);
      flags_.SetBits(
        SHM_CONTAINER_VALID |
          SHM_CONTAINER_DESTRUCTABLE);
    } else {
      lipc::Pointer header_ptr;
      header_ = header;
      lipc::Allocator::ConstructObj<TYPED_HEADER>(
        *header_, std::forward<Args>(args)...);
      header_->SetBits(
        SHM_CONTAINER_DATA_VALID);
      flags_.SetBits(
        SHM_CONTAINER_VALID |
          SHM_CONTAINER_DESTRUCTABLE);
    }
  }

  /**====================================
   * Serialization
   * ===================================*/

  /** Serialize into a Pointer */
  void shm_serialize(lipc::TypedPointer<TYPED_CLASS> &ar) const {
    ar = alloc_->template
      Convert<TYPED_HEADER, lipc::Pointer>(header_);
    shm_serialize_main();
  }

  /** Serialize into an AtomicPointer */
  void shm_serialize(lipc::TypedAtomicPointer<TYPED_CLASS> &ar) const {
    ar = alloc_->template
      Convert<TYPED_HEADER, lipc::AtomicPointer>(header_);
    shm_serialize_main();
  }

  /** Override << operators */
  SHM_SERIALIZE_OPS((TYPED_CLASS))

  /**====================================
   * Deserialization
   * ===================================*/

  /** Deserialize object from a raw pointer */
  bool shm_deserialize(const lipc::TypedPointer<TYPED_CLASS> &ar) {
    return shm_deserialize(
      HERMES_SHM_MEMORY_MANAGER->GetAllocator(ar.allocator_id_),
      ar.ToOffsetPointer()
    );
  }

  /** Deserialize object from allocator + offset */
  bool shm_deserialize(lipc::Allocator *alloc, lipc::OffsetPointer header_ptr) {
    if (header_ptr.IsNull()) { return false; }
    return shm_deserialize(alloc,
                           alloc->Convert<
                             TYPED_HEADER,
                             lipc::OffsetPointer>(header_ptr));
  }

  /** Deserialize object from another object (weak copy) */
  bool shm_deserialize(const CLASS_NAME &other) {
    if (other.IsNull()) { return false; }
    return shm_deserialize(other.GetAllocator(), other.header_);
  }

  /** Deserialize object from allocator + header */
  bool shm_deserialize(lipc::Allocator *alloc,
                       TYPED_HEADER *header) {
    flags_.UnsetBits(SHM_CONTAINER_DESTRUCTABLE);
    alloc_ = alloc;
    header_ = header;
    flags_.SetBits(SHM_CONTAINER_VALID);
    shm_deserialize_main();
    return true;
  }

  /** Constructor. Deserialize the object from the reference. */
  template<typename ...Args>
  void shm_init(lipc::ShmRef<TYPED_CLASS> &obj) {
    shm_deserialize(obj->GetAllocator(), obj->header_);
  }

  /** Override >> operators */
  SHM_DESERIALIZE_OPS((TYPED_CLASS))

  /**====================================
   * Destructors
   * ===================================*/

  /** Destructor */
  ~CLASS_NAME() {
    if (IsDestructable()) {
      shm_destroy(true);
    }
  }

  /** Shm Destructor */
  void shm_destroy(bool destroy_header = true) {
    if (!IsValid()) { return; }
    if (IsDataValid()) {
      shm_destroy_main();
    }
    UnsetDataValid();
    if (destroy_header &&
      header_->OrBits(SHM_CONTAINER_HEADER_DESTRUCTABLE)) {
      alloc_->FreePtr<TYPED_HEADER>(header_);
      UnsetValid();
    }
  }

  /**====================================
   * Move Operations
   * ===================================*/

  /** Move constructor */
  CLASS_NAME(CLASS_NAME &&other) noexcept {
    shm_weak_move(
      lipc::typed_nullptr<TYPED_HEADER>(),
      lipc::typed_nullptr<lipc::Allocator>(),
      other);
  }

  /** Move assignment operator */
  CLASS_NAME& operator=(CLASS_NAME &&other) noexcept {
    if (this != &other) {
      shm_weak_move(
        lipc::typed_nullptr<TYPED_HEADER>(),
        lipc::typed_nullptr<lipc::Allocator>(),
        other);
    }
    return *this;
  }

  /** Move shm_init constructor */
  void shm_init_main(TYPED_HEADER *header,
                     lipc::Allocator *alloc,
                     CLASS_NAME &&other) noexcept {
    shm_weak_move(header, alloc, other);
  }

  /** Move operation */
  void shm_weak_move(TYPED_HEADER *header,
                     lipc::Allocator *alloc,
                     CLASS_NAME &other) {
    if (other.IsNull()) { return; }
    if (IsValid() && other.GetAllocator() != GetAllocator()) {
      shm_strong_copy(header, alloc, other);
      other.shm_destroy(true);
      return;
    }
    shm_destroy(false);
    shm_weak_move_main(header, alloc, other);
    if (!other.IsDestructable()) {
      UnsetDestructable();
    }
    other.UnsetDataValid();
    other.shm_destroy(true);
  }

  /**====================================
   * Copy Operations
   * ===================================*/

  /** Copy constructor */
  CLASS_NAME(const CLASS_NAME &other) noexcept {
    shm_init(other);
  }

  /** Copy assignment constructor */
  CLASS_NAME& operator=(const CLASS_NAME &other) {
    if (this != &other) {
      shm_strong_copy(
        lipc::typed_nullptr<TYPED_HEADER>(),
        lipc::typed_nullptr<lipc::Allocator>(),
        other);
    }
    return *this;
  }

  /** Copy shm_init constructor */
  void shm_init_main(TYPED_HEADER *header,
                     lipc::Allocator *alloc,
                     const CLASS_NAME &other) {
    shm_strong_copy(header, alloc, other);
  }

  /** Strong Copy operation */
  void shm_strong_copy(TYPED_HEADER *header,
                       lipc::Allocator *alloc,
                       const CLASS_NAME &other) {
    if (other.IsNull()) { return; }
    shm_destroy(false);
    shm_strong_copy_main(header, alloc, other);
    SetDestructable();
  }

  /**====================================
   * Container Flag Operations
   * ===================================*/

  /** Sets this object as destructable */
  void SetDestructable() {
    flags_.SetBits(SHM_CONTAINER_DESTRUCTABLE);
  }

  /** Sets this object as not destructable */
  void UnsetDestructable() {
    flags_.UnsetBits(SHM_CONTAINER_DESTRUCTABLE);
  }

  /** Check if this container is destructable */
  bool IsDestructable() const {
    return flags_.OrBits(SHM_CONTAINER_DESTRUCTABLE);
  }

  /** Check if container has a valid header */
  bool IsValid() const {
    return flags_.OrBits(SHM_CONTAINER_VALID);
  }

  /** Set container header invalid */
  void UnsetValid() {
    flags_.UnsetBits(SHM_CONTAINER_VALID |
      SHM_CONTAINER_DESTRUCTABLE | SHM_CONTAINER_HEADER_DESTRUCTABLE);
  }

  /**====================================
   * Header Flag Operations
   * ===================================*/

  /** Check if header's data is valid */
  bool IsDataValid() const {
    return header_->OrBits(SHM_CONTAINER_DATA_VALID);
  }

  /** Check if header's data is valid */
  void UnsetDataValid() const {
    header_->UnsetBits(SHM_CONTAINER_DATA_VALID);
  }

  /** Check if null */
  bool IsNull() const {
    return !IsValid() || !IsDataValid();
  }

  /** Get a typed pointer to the object */
  template<typename POINTER_T>
  POINTER_T GetShmPointer() const {
    return alloc_->Convert<TYPED_HEADER, POINTER_T>(header_);
  }

  /**====================================
   * Query Operations
   * ===================================*/

  /** Get the allocator for this container */
  lipc::Allocator* GetAllocator() {
    return alloc_;
  }

  /** Get the allocator for this container */
  lipc::Allocator* GetAllocator() const {
    return alloc_;
  }

  /** Get the shared-memory allocator id */
  lipc::allocator_id_t GetAllocatorId() const {
    return alloc_->GetId();
  }
};

}  // namespace hermes_shm::ipc

#undef CLASS_NAME
#undef TYPED_CLASS
#undef TYPED_HEADER

#endif //HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_INTERNAL_SHM_CONTAINER_EXAMPLE_H_
