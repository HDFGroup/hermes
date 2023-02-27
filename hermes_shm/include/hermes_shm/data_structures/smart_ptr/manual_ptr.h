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

#include "hermes_shm/memory/memory.h"
#include "unique_ptr.h"
#include "hermes_shm/data_structures/internal/shm_internal.h"

namespace hermes_shm::ipc {

/**
 * MACROS to simplify the ptr namespace
 * */
#define CLASS_NAME manual_ptr
#define TYPED_CLASS manual_ptr<T>

/**
 * Creates a unique instance of a shared-memory data structure
 * and deletes eventually.
 * */
template<typename T>
class manual_ptr : public ShmSmartPtr<T> {
 public:
  SHM_SMART_PTR_TEMPLATE(T);

 public:
  /** Default constructor does nothing */
  manual_ptr() = default;

  /** Allocates + constructs an object in shared memory */
  template<typename ...Args>
  void shm_init(Args&& ...args) {
    obj_.shm_init(std::forward<Args>(args)...);
    obj_.UnsetDestructable();
  }

  /** Destructor. Does not free data. */
  ~manual_ptr() {
    obj_.UnsetDestructable();
  }

  /** Copy constructor */
  manual_ptr(const manual_ptr &other) {
    obj_.shm_deserialize(other.obj_);
  }

  /** Copy assignment operator */
  manual_ptr<T>& operator=(const manual_ptr<T> &other) {
    if (this != &other) {
      obj_.shm_deserialize(other.obj_);
    }
    return *this;
  }

  /** Move constructor */
  manual_ptr(manual_ptr&& other) noexcept {
    obj_ = std::move(other.obj_);
  }

  /** Constructor. From a TypedPointer<T> */
  explicit manual_ptr(const TypedPointer<TYPED_CLASS> &ar) {
    obj_.shm_deserialize(ar);
  }

  /** Constructor. From a TypedAtomicPointer<T> */
  explicit manual_ptr(const TypedAtomicPointer<TYPED_CLASS> &ar) {
    obj_.shm_deserialize(ar);
  }

  /** Constructor. From a ShmDeserialize. */
  explicit manual_ptr(const ShmDeserialize<T> &ar) {
    obj_.shm_deserialize(ar);
  }

  /** (De)serialize the obj from a TypedPointer<T> */
  SHM_SERIALIZE_DESERIALIZE_WRAPPER((T));
};

template<typename T>
using mptr = manual_ptr<T>;

template<typename T, typename ...Args>
static mptr<T> make_mptr(Args&& ...args) {
  mptr<T> ptr;
  ptr.shm_init(std::forward<Args>(args)...);
  return ptr;
}

}  // namespace hermes_shm::ipc

#undef CLASS_NAME
#undef TYPED_CLASS

namespace std {

/** Hash function for ptr */
template<typename T>
struct hash<hermes_shm::ipc::manual_ptr<T>> {
  size_t operator()(const hermes_shm::ipc::manual_ptr<T> &obj) const {
    return std::hash<T>{}(obj.get_ref_const());
  }
};

}  // namespace std

#endif  // HERMES_DATA_STRUCTURES_PTR_H_
