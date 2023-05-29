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


#ifndef HERMES_DATA_STRUCTURES_PTR_H_
#define HERMES_DATA_STRUCTURES_PTR_H_

#include "hermes_shm/constants/macros.h"
#include "hermes_shm/memory/memory.h"
#include "hermes_shm/memory/allocator/allocator.h"
#include "hermes_shm/memory/memory_registry.h"
#include "hermes_shm/data_structures/ipc/internal/shm_internal.h"
#include "hermes_shm/data_structures/ipc/internal/shm_smart_ptr.h"

namespace hshm::ipc {

/**
 * MACROS to simplify the ptr namespace
 * */
#define CLASS_NAME smart_ptr_base
#define TYPED_CLASS smart_ptr_base<T>

/**
 * Flags used for the smart pointer
 * */
#define POINTER_IS_OWNED BIT_OPT(uint32_t, 0)

/**
 * Creates a unique instance of a shared-memory data structure
 * and deletes eventually.
 * */
template<typename T, bool unique>
class smart_ptr_base {
 public:
  T *obj_;
  Allocator *alloc_;
  bitfield32_t flags_;

 public:
  /**====================================
  * Initialization + Destruction
  * ===================================*/

  /** Default constructor. */
  HSHM_ALWAYS_INLINE smart_ptr_base() = default;

  /** Create the mptr contents */
  template<typename ...Args>
  void shm_init(Allocator *alloc, Args&& ...args) {
    alloc_ = alloc;
    OffsetPointer p;
    if constexpr(IS_SHM_ARCHIVEABLE(T)) {
      obj_ = alloc_->template AllocateConstructObjs<T>(
        1, p, alloc, std::forward<Args>(args)...);
    } else {
      obj_ = alloc_->template AllocateConstructObjs<T>(
        1, p, std::forward<Args>(args)...);
    }
    if constexpr(unique) {
      flags_.SetBits(POINTER_IS_OWNED);
    }
  }

  /** Destructor. Does not free data. */
  HSHM_ALWAYS_INLINE ~smart_ptr_base() {
    if constexpr(unique) {
      if (flags_.Any(POINTER_IS_OWNED)) {
        shm_destroy();
      }
    }
  }

  /** Explicit destructor */
  HSHM_ALWAYS_INLINE void shm_destroy() {
    if constexpr(IS_SHM_ARCHIVEABLE(T)) {
      obj_->shm_destroy();
    }
    alloc_->template FreeDestructObjs<T>(obj_, 1);
  }

  /**====================================
  * Dereference Operations
  * ===================================*/

  /** Gets a pointer to the internal object */
  HSHM_ALWAYS_INLINE T* get() {
    return obj_;
  }

  /** Gets a pointer to the internal object */
  HSHM_ALWAYS_INLINE const T* get() const {
    return obj_;
  }

  /** Dereference operator */
  HSHM_ALWAYS_INLINE T& operator*() {
    return *get();
  }

  /** Constant Dereference operator */
  HSHM_ALWAYS_INLINE const T& operator*() const {
    return *get();
  }

  /** Pointer operator */
  HSHM_ALWAYS_INLINE T* operator->() {
    return get();
  }

  /** Constant pointer operator */
  HSHM_ALWAYS_INLINE const T* operator->() const {
    return get();
  }

  /**====================================
  * Move + Copy Operations
  * ===================================*/

  /** Copy constructor (equivalent to move) */
  HSHM_ALWAYS_INLINE smart_ptr_base(const smart_ptr_base &other) {
    shm_strong_copy(other);
    if constexpr(unique) {
      flags_.UnsetBits(POINTER_IS_OWNED);
    }
  }

  /** Copy assignment operator (equivalent to move) */
  HSHM_ALWAYS_INLINE smart_ptr_base& operator=(const smart_ptr_base &other) {
    if (this != &other) {
      shm_strong_copy(other);
      if constexpr(unique) {
        flags_.UnsetBits(POINTER_IS_OWNED);
      }
    }
    return *this;
  }

  /** Move constructor */
  HSHM_ALWAYS_INLINE smart_ptr_base(smart_ptr_base&& other) noexcept {
    shm_strong_move(std::forward<smart_ptr_base>(other));
  }

  /** Move assignment operator */
  HSHM_ALWAYS_INLINE smart_ptr_base&
  operator=(smart_ptr_base&& other) noexcept {
    if (this != &other) {
      shm_strong_move(std::forward<smart_ptr_base>(other));
    }
    return *this;
  }

  /** Internal copy operation */
  HSHM_ALWAYS_INLINE void shm_strong_copy(const smart_ptr_base &other) {
    obj_ = other.obj_;
    alloc_ = other.alloc_;
    flags_ = other.flags_;
  }

  /** Internal move operation */
  HSHM_ALWAYS_INLINE void shm_strong_move(smart_ptr_base &&other) {
    shm_strong_copy(other);
    if constexpr(unique) {
      flags_ = other.flags_;
      other.flags_.UnsetBits(POINTER_IS_OWNED);
    }
  }

  /**====================================
  * Deserialization
  * ===================================*/

  /** Constructor. Deserialize from a TypedPointer<T> */
  HSHM_ALWAYS_INLINE explicit smart_ptr_base(const TypedPointer<T> &ar) {
    shm_deserialize(ar);
  }

  /** Constructor. Deserialize from a TypedAtomicPointer<T> */
  HSHM_ALWAYS_INLINE explicit smart_ptr_base(const TypedAtomicPointer<T> &ar) {
    shm_deserialize(ar);
  }

  /** Deserialize from a process-independent pointer */
  template<typename PointerT>
  HSHM_ALWAYS_INLINE void shm_deserialize(const PointerT &ar) {
    auto alloc = HERMES_MEMORY_REGISTRY_REF.GetAllocator(ar.allocator_id_);
    obj_ = alloc->template Convert<T, PointerT>(ar);
    if constexpr(unique) {
      flags_.UnsetBits(POINTER_IS_OWNED);
    }
  }

  /** Deserialize from an offset pointer */
  HSHM_ALWAYS_INLINE void shm_deserialize(Allocator *alloc, const OffsetPointer &ar) {
    obj_ = alloc->template Convert<T, OffsetPointer>(ar);
    if constexpr(unique) {
      flags_.UnsetBits(POINTER_IS_OWNED);
    }
  }

  /**====================================
  * Serialization
  * ===================================*/

  /** Serialize to a process-independent pointer */
  template<typename PointerT>
  HSHM_ALWAYS_INLINE void shm_serialize(PointerT &ar) const {
    ar = alloc_->template Convert(obj_);
  }

  /**====================================
  * Serialization + Deserialization Ops
  * ===================================*/
  SHM_SERIALIZE_DESERIALIZE_OPS((T))

  /**====================================
  * Hash function
  * ===================================*/
  HSHM_ALWAYS_INLINE size_t hash() const {
    return std::hash<T>{}(*obj_);
  }
};

/**====================================
* Helper Functions
* ===================================*/

/** Mptr is non-unique and requires explicit destruction */
template<typename T>
using mptr = smart_ptr_base<T, false>;

/** Uptr has a specific container owning the object */
template<typename T>
using uptr = smart_ptr_base<T, true>;

/** Construct an mptr with default allocator */
template<typename PointerT, typename ...Args>
static PointerT make_ptr_base(Args&& ...args) {
  PointerT ptr;
  ptr.shm_init(std::forward<Args>(args)...);
  return ptr;
}

/** Create a manual pointer with default allocator */
template<typename T, typename ...Args>
mptr<T> make_mptr(Args&& ...args) {
  auto alloc = HERMES_MEMORY_REGISTRY->GetDefaultAllocator();
  return make_ptr_base<mptr<T>>(alloc, std::forward<Args>(args)...);
}

/** Create a manual pointer with non-default allocator */
template<typename T, typename ...Args>
mptr<T> make_mptr(Allocator *alloc, Args&& ...args) {
  return make_ptr_base<mptr<T>>(alloc, std::forward<Args>(args)...);
}

/** Create a unique pointer with default allocator */
template<typename T, typename ...Args>
uptr<T> make_uptr(Args&& ...args) {
  auto alloc = HERMES_MEMORY_REGISTRY->GetDefaultAllocator();
  return make_ptr_base<uptr<T>>(alloc, std::forward<Args>(args)...);
}

/** Create a unique pointer with non-default allocator */
template<typename T, typename ...Args>
uptr<T> make_uptr(Allocator *alloc, Args&& ...args) {
  return make_ptr_base<uptr<T>>(alloc, std::forward<Args>(args)...);
}

}  // namespace hshm::ipc

#undef CLASS_NAME
#undef TYPED_CLASS

/**====================================
* Hash Functions
* ===================================*/
namespace std {

/** Hash function for mptr */
template<typename T>
struct hash<hshm::ipc::mptr<T>> {
  size_t operator()(const hshm::ipc::mptr<T> &obj) const {
    return obj.hash();
  }
};

/** Hash function for uptr */
template<typename T>
struct hash<hshm::ipc::uptr<T>> {
  size_t operator()(const hshm::ipc::uptr<T> &obj) const {
    return obj.hash();
  }
};

}  // namespace std

#endif  // HERMES_DATA_STRUCTURES_PTR_H_
