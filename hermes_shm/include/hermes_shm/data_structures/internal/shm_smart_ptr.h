/*
 * Copyright (C) 2022  SCS Lab <scslab@iit.edu>,
 * Luke Logan <llogan@hawk.iit.edu>,
 * Jaime Cernuda Garcia <jcernudagarcia@hawk.iit.edu>
 * Jay Lofstead <gflofst@sandia.gov>,
 * Anthony Kougkas <akougkas@iit.edu>,
 * Xian-He Sun <sun@iit.edu>
 *
 * This file is part of HermesShm
 *
 * HermesShm is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */


#ifndef HERMES_SHM_DATA_STRUCTURES_INTERNAL_SHM_DATA_STRUCTURE_POINTER_H_
#define HERMES_SHM_DATA_STRUCTURES_INTERNAL_SHM_DATA_STRUCTURE_POINTER_H_

#include "hermes_shm/memory/memory.h"
#include "hermes_shm/memory/allocator/allocator.h"
#include "hermes_shm/memory/memory_manager.h"
#include "hermes_shm/data_structures/internal/shm_macros.h"
#include <hermes_shm/constants/data_structure_singleton_macros.h>

#include "hermes_shm/data_structures/internal/shm_archive.h"

namespace hermes_shm::ipc {

/**
 * Indicates a data structure represents a memory paradigm for Shm.
 * */
class ShmSmartPointer : public ShmArchiveable {};

/**
 * A base class used for creating shared-memory pointer management
 * classes (manual_ptr, unique_ptr, shared_ptr).
 *
 * Smart pointers are not stored directly in shared memory. They are
 * wrappers around shared-memory objects which manage the construction
 * and destruction of objects.
 * */
template<typename T>
class ShmSmartPtr : public ShmSmartPointer {
 public:
  T obj_; /**< The stored shared-memory object */

 public:
  /** Default constructor */
  ShmSmartPtr() = default;

  /** Sets this pointer to NULL */
  void SetNull() {
    obj_.SetNull();
  }

  /** Checks if this pointer is null */
  bool IsNull() const {
    return obj_.IsNull();
  }

  /** Gets a pointer to the internal object */
  T* get() {
    return &obj_;
  }

  /** Gets a pointer to the internal object */
  T* get() const {
    return &obj_;
  }

  /** Gets a pointer to the internal object */
  T* get_const() const {
    return &obj_;
  }

  /** Gets a reference to the internal object */
  T& get_ref() {
    return obj_;
  }

  /** Gets a reference to the internal object */
  T& get_ref() const {
    return obj_;
  }

  /** Gets a reference to the internal object */
  const T& get_ref_const() const {
    return obj_;
  }

  /** Dereference operator */
  T& operator*() {
    return get_ref();
  }

  /** Constant dereference operator */
  const T& operator*() const {
    return get_ref_const();
  }

  /** Pointer operator */
  T* operator->() {
    return get();
  }

  /** Constant pointer operator */
  const T* operator->() const {
    return get_const();
  }

  /** Destroy the data allocated by this pointer */
  void shm_destroy() {
    obj_.shm_destroy();
  }
};



}  // namespace hermes_shm::ipc

/**
 * Namespace simplification for a SHM data structure pointer
 * */
#define SHM_SMART_PTR_TEMPLATE(T) \
  using ShmSmartPtr<T>::shm_destroy;\
  using ShmSmartPtr<T>::obj_;\
  using ShmSmartPtr<T>::get;\
  using ShmSmartPtr<T>::get_ref;\
  using ShmSmartPtr<T>::get_const;\
  using ShmSmartPtr<T>::get_ref_const;\
  using ShmSmartPtr<T>::SetNull;\
  using ShmSmartPtr<T>::IsNull;

/**
 * A macro for defining shared memory serializations
 * */
#define SHM_SERIALIZE_WRAPPER(AR_TYPE)\
  void shm_serialize(lipc::TypedPointer<TYPE_UNWRAP(AR_TYPE)> &type) const {\
    obj_.shm_serialize(type);\
  }\
  void shm_serialize(lipc::TypedAtomicPointer<TYPE_UNWRAP(AR_TYPE)> &type) const {\
    obj_.shm_serialize(type);\
  }\
  SHM_SERIALIZE_OPS(AR_TYPE)

/**
 * A macro for defining shared memory deserializations
 * */
#define SHM_DESERIALIZE_WRAPPER(AR_TYPE)\
  void shm_deserialize(const lipc::TypedPointer<TYPE_UNWRAP(AR_TYPE)> &type) {\
    obj_.shm_deserialize(type);\
  }\
  void shm_deserialize(const lipc::TypedAtomicPointer<TYPE_UNWRAP(AR_TYPE)> &type) {\
    obj_.shm_deserialize(type);\
  }\
  SHM_DESERIALIZE_OPS(AR_TYPE)

/**
 * A macro for defining shared memory (de)serializations
 * */
#define SHM_SERIALIZE_DESERIALIZE_WRAPPER(AR_TYPE)\
  SHM_SERIALIZE_WRAPPER(AR_TYPE)\
  SHM_DESERIALIZE_WRAPPER(AR_TYPE)

#endif  // HERMES_SHM_DATA_STRUCTURES_INTERNAL_SHM_DATA_STRUCTURE_POINTER_H_
