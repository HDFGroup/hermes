#ifndef HERMES_DATA_STRUCTURES_INTERNAL_SHM_CONTAINER_EXTEND_MACRO_H_
#define HERMES_DATA_STRUCTURES_INTERNAL_SHM_CONTAINER_EXTEND_MACRO_H_
#define SHM_CONTAINER_EXTEND_TEMPLATE(CLASS_NAME,TYPED_CLASS,TYPED_HEADER)\
/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *\
 * Distributed under BSD 3-Clause license.                                   *\
 * Copyright by The HDF Group.                                               *\
 * Copyright by the Illinois Institute of Technology.                        *\
 * All rights reserved.                                                      *\
 *                                                                           *\
 * This file is part of Hermes. The full Hermes copyright notice, including  *\
 * terms governing use, modification, and redistribution, is contained in    *\
 * the COPYING file, which can be found at the top directory. If you do not  *\
 * have access to the file, you may request a copy from help@hdfgroup.org.   *\
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */\
\
public:\
/**====================================\
 * Variables & Types\
 *===================================*/\
\
typedef TYPE_UNWRAP(TYPED_HEADER) header_t; /**< Required by all ShmContainers */\
header_t *header_; /**< The shared-memory header for this container */\
ContainerT obj_; /**< The object being wrapped around */\
\
public:\
/**====================================\
 * Constructors\
 * ===================================*/\
\
/** Constructor. Allocate header with default allocator. */\
template<typename ...Args>\
explicit TYPE_UNWRAP(CLASS_NAME)(Args &&...args) {\
shm_init(std::forward<Args>(args)...);\
}\
\
/** Constructor. Allocate header with default allocator. */\
template<typename ...Args>\
void shm_init(Args &&...args) {\
}\
\
/**====================================\
 * Serialization\
 * ===================================*/\
\
/** Serialize into a Pointer */\
void shm_serialize(TypedPointer<TYPE_UNWRAP(TYPED_CLASS)> &ar) const {\
  obj_.shm_serialize(ar);\
}\
\
/** Serialize into an AtomicPointer */\
void shm_serialize(TypedAtomicPointer<TYPE_UNWRAP(TYPED_CLASS)> &ar) const {\
  obj_.shm_serialize(ar);\
}\
\
/** Override << operators */\
SHM_SERIALIZE_OPS((TYPE_UNWRAP(TYPED_CLASS)))\
\
/**====================================\
 * Deserialization\
 * ===================================*/\
\
/** Deserialize object from a raw pointer */\
bool shm_deserialize(const TypedPointer<TYPE_UNWRAP(TYPED_CLASS)> &ar) {\
  return obj_.shm_deserialize(ar);\
}\
\
/** Deserialize object from allocator + offset */\
bool shm_deserialize(Allocator *alloc, OffsetPointer header_ptr) {\
  return obj_.shm_deserialize(alloc, header_ptr);\
}\
\
/** Deserialize object from another object (weak copy) */\
bool shm_deserialize(const TYPE_UNWRAP(CLASS_NAME) &other) {\
  return obj_.shm_deserialize(other);\
}\
\
/** Deserialize object from allocator + header */\
bool shm_deserialize(Allocator *alloc,\
                     TYPE_UNWRAP(TYPED_HEADER) *header) {\
  return obj_.shm_deserialize(alloc, header);\
}\
\
/** Constructor. Deserialize the object from the reference. */\
template<typename ...Args>\
void shm_init(hipc::ShmRef<TYPE_UNWRAP(TYPED_CLASS)> &obj) {\
  shm_deserialize(obj->GetAllocator(), obj->header_);\
}\
\
/** Override >> operators */\
SHM_DESERIALIZE_OPS ((TYPE_UNWRAP(TYPED_CLASS)))\
\
/**====================================\
 * Destructors\
 * ===================================*/\
\
/** Destructor */\
~TYPE_UNWRAP(CLASS_NAME)() {\
  obj_.shm_destroy(true);\
}\
\
/** Shm Destructor */\
void shm_destroy(bool destroy_header = true) {\
  obj_.shm_destroy(false);\
  if (!IsValid()) { return; }\
  if (IsDataValid()) {\
    shm_destroy_main();\
  }\
  UnsetDataValid();\
  if (destroy_header &&\
    obj_.header_->OrBits(SHM_CONTAINER_HEADER_DESTRUCTABLE)) {\
    GetAllocator()->template\
      FreePtr<TYPE_UNWRAP(TYPED_HEADER)>(header_);\
    UnsetValid();\
  }\
}\
\
/**====================================\
 * Move Operations\
 * ===================================*/\
\
/** Move constructor */\
TYPE_UNWRAP(CLASS_NAME)(TYPE_UNWRAP(CLASS_NAME) &&other) noexcept\
: obj_(std::move(other)) {}\
\
/** Move assignment operator */\
TYPE_UNWRAP(CLASS_NAME)& operator=(TYPE_UNWRAP(CLASS_NAME) &&other) noexcept {\
obj_ = std::move(other.obj_);\
return *this;\
}\
\
/** Move shm_init constructor */\
void shm_init_main(TYPE_UNWRAP(TYPED_HEADER) *header,\
                   hipc::Allocator *alloc,\
                   TYPE_UNWRAP(CLASS_NAME) &&other) noexcept {\
  shm_weak_move(header, alloc, other);\
}\
\
/** Move operation */\
void shm_weak_move(TYPE_UNWRAP(TYPED_HEADER) *header,\
                   hipc::Allocator *alloc,\
                   TYPE_UNWRAP(CLASS_NAME) &other) {\
  obj_.shm_weak_move(header, alloc, other);\
}\
\
/**====================================\
 * Copy Operations\
 * ===================================*/\
\
/** Copy constructor */\
TYPE_UNWRAP(CLASS_NAME)(const TYPE_UNWRAP(CLASS_NAME) &other) noexcept {\
  shm_init(other);\
}\
\
/** Copy assignment constructor */\
TYPE_UNWRAP(CLASS_NAME) &operator=(const TYPE_UNWRAP(CLASS_NAME) &other) {\
  if (this != &other) {\
    shm_strong_copy(\
      typed_nullptr<TYPE_UNWRAP(TYPED_HEADER) >(),\
      typed_nullptr<Allocator>(),\
      other);\
  }\
  return *this;\
}\
\
/** Copy shm_init constructor */\
void shm_init_main(TYPE_UNWRAP(TYPED_HEADER) *header,\
                   hipc::Allocator *alloc,\
                   const TYPE_UNWRAP(CLASS_NAME) &other) {\
  shm_strong_copy(header, alloc, other);\
}\
\
/** Strong Copy operation */\
void shm_strong_copy(TYPE_UNWRAP(TYPED_HEADER) *header, hipc::Allocator *alloc,\
                     const TYPE_UNWRAP(CLASS_NAME) &other) {\
  if (other.IsNull()) { return; }\
  shm_destroy(false);\
  shm_strong_copy_main(header, alloc, other);\
  SetDestructable();\
}\
\
/**====================================\
 * Container Flag Operations\
 * ===================================*/\
\
/** Sets this object as destructable */\
void SetDestructable() {\
  obj_.SetDestructable();\
}\
\
/** Sets this object as not destructable */\
void UnsetDestructable() {\
  obj_.UnsetDestructable();\
}\
\
/** Check if this container is destructable */\
bool IsDestructable() const {\
  return obj_.IsDestructable();\
}\
\
/** Check if container has a valid header */\
bool IsValid() const {\
  return obj_.IsValid();\
}\
\
/** Set container header invalid */\
void UnsetValid() {\
  obj_.UnsetValid();\
}\
\
/**====================================\
 * Header Flag Operations\
 * ===================================*/\
\
/** Check if header's data is valid */\
bool IsDataValid() const {\
  return obj_.IsDataValid();\
}\
\
/** Check if header's data is valid */\
void UnsetDataValid() const {\
  return obj_.UnsetDataValid();\
}\
\
/** Check if null */\
bool IsNull() const {\
  return obj_.IsNull();\
}\
\
/** Get a typed pointer to the object */\
template<typename POINTER_T>\
POINTER_T GetShmPointer() const {\
  return GetAllocator()->template\
    Convert<TYPE_UNWRAP(TYPED_HEADER), POINTER_T>(header_);\
}\
\
/**====================================\
 * Query Operations\
 * ===================================*/\
\
/** Get the allocator for this container */\
Allocator* GetAllocator() {\
  return obj_.GetAllocator();\
}\
\
/** Get the allocator for this container */\
Allocator* GetAllocator() const {\
  return obj_.GetAllocator();\
}\
\
/** Get the shared-memory allocator id */\
allocator_id_t GetAllocatorId() const {\
  return GetAllocator()->GetId();\
}\

#endif  // HERMES_DATA_STRUCTURES_INTERNAL_SHM_CONTAINER_EXTEND_MACRO_H_
