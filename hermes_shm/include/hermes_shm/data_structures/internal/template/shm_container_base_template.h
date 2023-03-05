public:
/**====================================
 * Variables & Types
 * ===================================*/

typedef TYPED_HEADER header_t; /** Header type query */
header_t *header_; /**< Header of the shared-memory data structure */
hipc::Allocator *alloc_; /**< hipc::Allocator used for this data structure */
hermes_shm::bitfield32_t flags_; /**< Flags used data structure status */

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
  shm_init_main(hipc::typed_nullptr<TYPED_HEADER>(),
                hipc::typed_nullptr<hipc::Allocator>(),
                std::forward<Args>(args)...);
}

/** Constructor. Allocate header with specific allocator. */
template<typename ...Args>
void shm_init(hipc::Allocator *alloc, Args&& ...args) {
  shm_destroy(false);
  shm_init_main(hipc::typed_nullptr<TYPED_HEADER>(),
                alloc,
                std::forward<Args>(args)...);
}

/** Constructor. Initialize an already-allocated header. */
template<typename ...Args>
void shm_init(TYPED_HEADER &header,
              hipc::Allocator *alloc, Args&& ...args) {
  shm_destroy(false);
  shm_init_main(&header, alloc, std::forward<Args>(args)...);
}

/** Initialize the data structure's allocator */
inline void shm_init_allocator(hipc::Allocator *alloc) {
  if (IsValid()) { return; }
  if (alloc == nullptr) {
    alloc_ = HERMES_MEMORY_REGISTRY->GetDefaultAllocator();
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
    hipc::Pointer p;
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
    hipc::Pointer header_ptr;
    header_ = header;
    hipc::Allocator::ConstructObj<TYPED_HEADER>(
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
void shm_serialize(hipc::TypedPointer<TYPED_CLASS> &ar) const {
  ar = alloc_->template
    Convert<TYPED_HEADER, hipc::Pointer>(header_);
  shm_serialize_main();
}

/** Serialize into an AtomicPointer */
void shm_serialize(hipc::TypedAtomicPointer<TYPED_CLASS> &ar) const {
  ar = alloc_->template
    Convert<TYPED_HEADER, hipc::AtomicPointer>(header_);
  shm_serialize_main();
}

/** Override >> operators */
void operator>>(hipc::TypedPointer<TYPE_UNWRAP(TYPED_CLASS)> &ar) const {
  shm_serialize(ar);
}
void operator>>(
  hipc::TypedAtomicPointer<TYPE_UNWRAP(TYPED_CLASS)> &ar) const {
  shm_serialize(ar);
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
  return shm_deserialize(alloc,
                         alloc->Convert<
                           TYPED_HEADER,
                           hipc::OffsetPointer>(header_ptr));
}

/** Deserialize object from another object (weak copy) */
bool shm_deserialize(const CLASS_NAME &other) {
  if (other.IsNull()) { return false; }
  return shm_deserialize(other.GetAllocator(), other.header_);
}

/** Deserialize object from "Deserialize" object */
bool shm_deserialize(hipc::ShmDeserialize<TYPED_CLASS> other) {
  return shm_deserialize(other.alloc_, other.header_);
}

/** Deserialize object from allocator + header */
bool shm_deserialize(hipc::Allocator *alloc,
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
void shm_init(hipc::ShmRef<TYPED_CLASS> &obj) {
  shm_deserialize(obj->GetAllocator(), obj->header_);
}

/** Constructor. Deserialize the object deserialize reference. */
template<typename ...Args>
void shm_init(hipc::ShmDeserialize<TYPED_CLASS> other) {
  shm_deserialize(other);
}

/** Override << operators */
void operator<<(const hipc::TypedPointer<TYPE_UNWRAP(TYPED_CLASS)> &ar) {
  shm_deserialize(ar);
}
void operator<<(
  const hipc::TypedAtomicPointer<TYPE_UNWRAP(TYPED_CLASS)> &ar) {
  shm_deserialize(ar);
}
void operator<<(
  const hipc::ShmDeserialize<TYPE_UNWRAP(TYPED_CLASS)> &ar) {
  shm_deserialize(ar);
}

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
  hipc::typed_nullptr<TYPED_HEADER>(),
  hipc::typed_nullptr<hipc::Allocator>(),
  other);
}

/** Move assignment operator */
CLASS_NAME& operator=(CLASS_NAME &&other) noexcept {
if (this != &other) {
shm_weak_move(
  hipc::typed_nullptr<TYPED_HEADER>(),
  hipc::typed_nullptr<hipc::Allocator>(),
  other);
}
return *this;
}

/** Move shm_init constructor */
void shm_init_main(TYPED_HEADER *header,
                   hipc::Allocator *alloc,
                   CLASS_NAME &&other) noexcept {
  shm_weak_move(header, alloc, other);
}

/** Move operation */
void shm_weak_move(TYPED_HEADER *header,
                   hipc::Allocator *alloc,
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
      hipc::typed_nullptr<TYPED_HEADER>(),
      hipc::typed_nullptr<hipc::Allocator>(),
      other);
  }
  return *this;
}

/** Copy shm_init constructor */
void shm_init_main(TYPED_HEADER *header,
                   hipc::Allocator *alloc,
                   const CLASS_NAME &other) {
  shm_strong_copy(header, alloc, other);
}

/** Strong Copy operation */
void shm_strong_copy(TYPED_HEADER *header,
                     hipc::Allocator *alloc,
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