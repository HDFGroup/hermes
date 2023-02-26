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

#ifndef HERMES_INCLUDE_HERMES_DATA_STRUCTURES_INTERNAL_SHM_ShmRef_H_
#define HERMES_INCLUDE_HERMES_DATA_STRUCTURES_INTERNAL_SHM_ShmRef_H_

#include "hermes_shm/constants/macros.h"
#include "shm_macros.h"
#include "shm_archive.h"
#include "shm_deserialize.h"

namespace hermes_shm::ipc {

/**
 * A ShmReference to a shared-memory object
 * */
template<typename T>
struct _ShmRefShm {
  T obj_;

  /** Default constructor */
  _ShmRefShm() = default;

  /** Destructor */
  ~_ShmRefShm() {
    obj_.UnsetDestructable();
  }

  /** Constructor. From TypedPointer. */
  explicit _ShmRefShm(TypedPointer<T> other) {
    obj_.shm_deserialize(other);
  }

  /** Constructor. From ShmDeserialize. */
  explicit _ShmRefShm(ShmDeserialize<T> other) {
    obj_.shm_deserialize(other);
  }

  /** Copy constructor */
  _ShmRefShm(const _ShmRefShm &other) {
    obj_.shm_deserialize(other.obj_);
  }

  /** Move constructor */
  _ShmRefShm(_ShmRefShm &&other) noexcept {
    obj_.shm_deserialize(other.obj_);
  }

  /** Copy assign operator */
  _ShmRefShm& operator=(const _ShmRefShm &other) {
    if (this != &other) {
      obj_.shm_deserialize(other.obj_);
    }
    return *this;
  }

  /** Move assign operator */
  _ShmRefShm& operator=(_ShmRefShm &&other) noexcept {
    if (this != &other) {
      obj_.shm_deserialize(other.obj_);
    }
    return *this;
  }

  /** Get ShmReference to the internal data structure */
  T& get_ref() {
    return obj_;
  }

  /** Get a constant ShmReference */
  const T& get_ref_const() const {
    return obj_;
  }
};

/**
 * A ShmReference to a POD type stored in shared memory.
 * */
template<typename T>
struct _ShmRefNoShm {
  T *obj_;

  /** Default constructor */
  _ShmRefNoShm() = default;

  /** Constructor. */
  explicit _ShmRefNoShm(T &other) {
    obj_ = &other;
  }

  /** Copy constructor */
  _ShmRefNoShm(const _ShmRefNoShm &other) {
    obj_ = other.obj_;
  }

  /** Move constructor */
  _ShmRefNoShm(_ShmRefNoShm &&other) noexcept {
    obj_ = other.obj_;
  }

  /** Copy assign operator */
  _ShmRefNoShm& operator=(const _ShmRefNoShm &other) {
    if (this != &other) {
      obj_ = other.obj_;
    }
    return *this;
  }

  /** Move assign operator */
  _ShmRefNoShm& operator=(_ShmRefNoShm &&other) noexcept {
    if (this != &other) {
      obj_ = other.obj_;
    }
    return *this;
  }

  /** Get ShmReference to the internal data structure */
  T& get_ref() {
    return *obj_;
  }

  /** Get a constant ShmReference */
  const T& get_ref_const() const {
    return *obj_;
  }
};

/** Determine whether ShmRef stores _ShmRefShm or _ShmRefNoShm */
#define CHOOSE_SHM_ShmRef_TYPE(T) SHM_X_OR_Y(T, _ShmRefShm<T>, _ShmRefNoShm<T>)

/**
 * A Reference to a shared-memory object or a simple object
 * stored in shared-memory.
 * */
template<typename T>
struct ShmRef {
  typedef CHOOSE_SHM_ShmRef_TYPE(T) T_ShmRef;
  T_ShmRef obj_;

  /** Constructor. */
  template<typename ...Args>
  explicit ShmRef(Args&& ...args) : obj_(std::forward<Args>(args)...) {}

  /** Default constructor */
  ShmRef() = default;

  /** Copy Constructor */
  ShmRef(const ShmRef &other) : obj_(other.obj_) {}

  /** Copy assign operator */
  ShmRef& operator=(const ShmRef &other) {
    obj_ = other.obj_;
    return *this;
  }

  /** Move Constructor */
  ShmRef(ShmRef &&other) noexcept : obj_(std::move(other.obj_)) {}

  /** Move assign operator */
  ShmRef& operator=(ShmRef &&other) noexcept {
    obj_ = std::move(other.obj_);
    return *this;
  }

  /** Get ShmReference to the internal data structure */
  T& get_ref() {
    return obj_.get_ref();
  }

  /** Get a constant ShmReference */
  const T& get_ref_const() const {
    return obj_.get_ref_const();
  }

  /** DeShmReference operator */
  T& operator*() {
    return get_ref();
  }

  /** Constant deShmReference operator */
  const T& operator*() const {
    return get_ref_const();
  }

  /** Pointer operator */
  T* operator->() {
    return &get_ref();
  }

  /** Constant pointer operator */
  const T* operator->() const {
    return &get_ref_const();
  }
};

}  // namespace hermes_shm::ipc

#endif //HERMES_INCLUDE_HERMES_DATA_STRUCTURES_INTERNAL_SHM_ShmRef_H_
