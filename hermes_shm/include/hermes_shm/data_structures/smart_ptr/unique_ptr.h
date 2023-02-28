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


/**
 * Create a data structure in either shared or regular memory.
 * Destroy the data structure when the unique pointer is deconstructed
 * */

#ifndef HERMES_DATA_STRUCTURES_UNIQUE_PTR_H_
#define HERMES_DATA_STRUCTURES_UNIQUE_PTR_H_

#include "hermes_shm/memory/memory.h"
#include "hermes_shm/data_structures/internal/shm_internal.h"

namespace hermes_shm::ipc {

/**
 * MACROS to simplify the unique_ptr namespace
 * */
#define CLASS_NAME unique_ptr
#define TYPED_CLASS unique_ptr<T>

/**
 * Create a unique instance of a shared-memory data structure.
 * The process which creates the data structure owns it and is responsible
 * for freeing. Other processes can deserialize the data structure, but
 * cannot destroy its data.
 * */
template<typename T>
class unique_ptr : public ShmSmartPtr<T> {
 public:
  SHM_SMART_PTR_TEMPLATE(T);

 public:
  /** Default constructor */
  unique_ptr() = default;

  /** Destroys all allocated memory */
  ~unique_ptr() = default;

  /** Allocates + constructs an object in shared memory */
  template<typename ...Args>
  void shm_init(Args&& ...args) {
    obj_.shm_init(std::forward<Args>(args)...);
  }

  /** Disable the copy constructor */
  unique_ptr(const unique_ptr &other) = delete;

  /** Move constructor */
  unique_ptr(unique_ptr&& other) noexcept {
    obj_ = std::move(other.obj_);
  }

  /** Move assignment operator */
  unique_ptr<T>& operator=(unique_ptr<T> &&other) {
    if (this != &other) {
      obj_ = std::move(other.obj_);
    }
    return *this;
  }

  /** Constructor. From a TypedPointer<uptr<T>> */
  explicit unique_ptr(TypedPointer<TYPED_CLASS> &ar) {
    obj_.shm_deserialize(ar);
  }

  /** Constructor. From a TypedAtomicPointer<uptr<T>> */
  explicit unique_ptr(TypedAtomicPointer<TYPED_CLASS> &ar) {
    obj_.shm_deserialize(ar);
  }

  /** Constructor. From a ShmDeserialize. */
  explicit unique_ptr(const ShmDeserialize<T> &ar) {
    obj_.shm_deserialize(ar);
  }

  /** Serialize into a TypedPointer<unique_ptr> */
  SHM_SERIALIZE_DESERIALIZE_WRAPPER(unique_ptr)
};

template<typename T>
using uptr = unique_ptr<T>;

template<typename T, typename ...Args>
static uptr<T> make_uptr(Args&& ...args) {
  uptr<T> ptr;
  ptr.shm_init(std::forward<Args>(args)...);
  return ptr;
}

}  // namespace hermes_shm::ipc

#undef CLASS_NAME
#undef TYPED_CLASS

namespace std {

/** Hash function for unique_ptr */
template<typename T>
struct hash<hermes_shm::ipc::unique_ptr<T>> {
  size_t operator()(const hermes_shm::ipc::unique_ptr<T> &obj) const {
    return std::hash<T>{}(obj.get_ref_const());
  }
};

}  // namespace std

#endif  // HERMES_DATA_STRUCTURES_UNIQUE_PTR_H_
