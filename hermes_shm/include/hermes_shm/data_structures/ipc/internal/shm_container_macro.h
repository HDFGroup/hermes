#ifndef HERMES_DATA_STRUCTURES_INTERNAL_SHM_CONTAINER_MACRO_H_
#define HERMES_DATA_STRUCTURES_INTERNAL_SHM_CONTAINER_MACRO_H_
#define SHM_CONTAINER_TEMPLATE(CLASS_NAME,TYPED_CLASS,TYPED_HEADER)\
public:\
/**====================================\
 * Variables & Types\
 * ===================================*/\
\
typedef TYPE_UNWRAP(TYPED_HEADER) header_t; /** Header type query */\
header_t *header_; /**< Header of the shared-memory data structure */\
hipc::Allocator *alloc_; /**< hipc::Allocator used for this data structure */\
\
/**====================================\
 * Constructors\
 * ===================================*/\
\
/** Default constructor. Deleted. */\
TYPE_UNWRAP(CLASS_NAME)() = delete;\
\
/** Move constructor. Deleted. */\
TYPE_UNWRAP(CLASS_NAME)(TYPE_UNWRAP(CLASS_NAME) &&other) = delete;\
\
/** Copy constructor. Deleted. */\
TYPE_UNWRAP(CLASS_NAME)(const TYPE_UNWRAP(CLASS_NAME) &other) = delete;\
\
/** Initialize header + allocator */\
void shm_init_header(TYPE_UNWRAP(TYPE_UNWRAP(TYPED_HEADER)) *header,\
                     hipc::Allocator *alloc) {\
  header_ = header;\
  hipc::Allocator::ConstructObj<TYPE_UNWRAP(TYPE_UNWRAP(TYPED_HEADER))>(*header_);\
  alloc_ = alloc;\
}\
\
/**====================================\
 * Destructor\
 * ===================================*/\
\
/** Destructor. */\
~TYPE_UNWRAP(CLASS_NAME)() = default;\
\
/** Destruction operation */\
void shm_destroy() {\
  if (IsNull()) { return; }\
  shm_destroy_main();\
  SetNull();\
}\
\
/**====================================\
 * Serialization\
 * ===================================*/\
\
/** Serialize into a Pointer */\
void shm_serialize(hipc::TypedPointer<TYPE_UNWRAP(TYPED_CLASS)> &ar) const {\
  ar = alloc_->template\
    Convert<TYPE_UNWRAP(TYPED_HEADER), hipc::Pointer>(header_);\
}\
\
/** Serialize into an AtomicPointer */\
void shm_serialize(hipc::TypedAtomicPointer<TYPE_UNWRAP(TYPED_CLASS)> &ar) const {\
  ar = alloc_->template\
    Convert<TYPE_UNWRAP(TYPED_HEADER), hipc::AtomicPointer>(header_);\
}\
\
/**====================================\
 * Deserialization\
 * ===================================*/\
\
/** Deserialize object from a raw pointer */\
bool shm_deserialize(const hipc::TypedPointer<TYPE_UNWRAP(TYPED_CLASS)> &ar) {\
  return shm_deserialize(\
    HERMES_MEMORY_REGISTRY->GetAllocator(ar.allocator_id_),\
    ar.ToOffsetPointer()\
  );\
}\
\
/** Deserialize object from allocator + offset */\
bool shm_deserialize(hipc::Allocator *alloc, hipc::OffsetPointer header_ptr) {\
  if (header_ptr.IsNull()) { return false; }\
  return shm_deserialize(alloc->Convert<\
                           TYPE_UNWRAP(TYPED_HEADER),\
                           hipc::OffsetPointer>(header_ptr),\
                         alloc);\
}\
\
/** Deserialize object from "Deserialize" object */\
bool shm_deserialize(hipc::ShmDeserialize<TYPE_UNWRAP(TYPED_CLASS)> other) {\
  return shm_deserialize(other.header_, other.alloc_);\
}\
\
/** Deserialize object from allocator + header */\
bool shm_deserialize(TYPE_UNWRAP(TYPED_HEADER) *header,\
                     hipc::Allocator *alloc) {\
  header_ = header;\
  alloc_ = alloc;\
  shm_deserialize_main();\
  return true;\
}\
\
/** Constructor. Deserialize the object from the reference. */\
explicit TYPE_UNWRAP(CLASS_NAME)(hipc::Ref<TYPE_UNWRAP(TYPED_CLASS)> &obj) {\
shm_deserialize(obj->header_, obj->GetAllocator());\
}\
\
/** Constructor. Deserialize the object deserialize reference. */\
explicit TYPE_UNWRAP(CLASS_NAME)(hipc::ShmDeserialize<TYPE_UNWRAP(TYPED_CLASS)> other) {\
shm_deserialize(other);\
}\
\
/**====================================\
 * Header Operations\
 * ===================================*/\
\
/** Get a typed pointer to the object */\
template<typename POINTER_T>\
POINTER_T GetShmPointer() const {\
  return alloc_->Convert<TYPE_UNWRAP(TYPED_HEADER), POINTER_T>(header_);\
}\
\
/** Get a ShmDeserialize object */\
hipc::ShmDeserialize<TYPE_UNWRAP(CLASS_NAME)> GetShmDeserialize() const {\
  return hipc::ShmDeserialize<TYPE_UNWRAP(CLASS_NAME)>(header_, alloc_);\
}\
\
/**====================================\
 * Query Operations\
 * ===================================*/\
\
/** Get the allocator for this container */\
hipc::Allocator* GetAllocator() {\
  return alloc_;\
}\
\
/** Get the allocator for this container */\
hipc::Allocator* GetAllocator() const {\
  return alloc_;\
}\
\
/** Get the shared-memory allocator id */\
hipc::allocator_id_t GetAllocatorId() const {\
  return alloc_->GetId();\
}\

#endif  // HERMES_DATA_STRUCTURES_INTERNAL_SHM_CONTAINER_MACRO_H_
