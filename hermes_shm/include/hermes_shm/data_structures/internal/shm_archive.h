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


#ifndef HERMES_SHM_DATA_STRUCTURES_SHM_ARCHIVE_H_
#define HERMES_SHM_DATA_STRUCTURES_SHM_ARCHIVE_H_

#include "hermes_shm/memory/memory_manager.h"
#include "shm_macros.h"

namespace hermes_shm::ipc {

/**
 * Indicates that a data structure can be stored directly in memory or
 * shared memory.
 * */
class ShmPredictable {};

/**
 * Indicates that a data structure can be archived in shared memory
 * and has a corresponding TypedPointer override.
 * */
class ShmArchiveable : public ShmPredictable {
  /**
   * Initialize a SHM data structure in shared-memory.
   * Constructors may wrap around these.
   * */
  // void shm_init(...);

  /**
   * Destroys the shared-memory allocated by the object.
   * Destructors may wrap around this.
   * */
  // void shm_destroy();

  /**
   * Deep copy of an object. Wrapped by copy constructor
   * */
  // void shm_strong_copy(const CLASS_NAME &other);
  // SHM_INHERIT_COPY_OPS(CLASS_NAME)

  /**
   * Copies only the object's pointers.
   * */
  // void WeakCopy(const CLASS_NAME &other);

  /**
   * Moves the object's contents into another object
   * */
  // void shm_weak_move(CLASS_NAME &other);
  // SHM_INHERIT_MOVE_OPS(CLASS_NAME)

  /**
   * Store object into a TypedPointer
   * */
  // void shm_serialize(TypedPointer<TYPED_CLASS> &ar) const;
  // SHM_SERIALIZE_OPS(TYPED_CLASS)

  /**
   * Construct object from a TypedPointer.
   * */
  // void shm_deserialize(const TypedPointer<TYPED_CLASS> &ar);
  // SHM_DESERIALIZE_OPS(TYPED_CLASS)
};

/**
 * Enables a specific TypedPointer type to be serialized
 * */
#define SHM_SERIALIZE_OPS(TYPED_CLASS)\
  void operator>>(lipc::TypedPointer<TYPE_UNWRAP(TYPED_CLASS)> &ar) const {\
    shm_serialize(ar);\
  }\
  void operator>>(lipc::TypedAtomicPointer<TYPE_UNWRAP(TYPED_CLASS)> &ar) const {\
    shm_serialize(ar);\
  }

/**
 * Enables a specific TypedPointer type to be deserialized
 * */
#define SHM_DESERIALIZE_OPS(TYPED_CLASS)\
  void operator<<(const lipc::TypedPointer<TYPE_UNWRAP(TYPED_CLASS)> &ar) {\
    shm_deserialize(ar);\
  }\
  void operator<<(\
    const lipc::TypedAtomicPointer<TYPE_UNWRAP(TYPED_CLASS)> &ar) {\
    shm_deserialize(ar);\
  }

/** Enables serialization + deserialization for data structures */
#define SHM_SERIALIZE_DESERIALIZE_OPS(AR_TYPE)\
  SHM_SERIALIZE_OPS(AR_TYPE)\
  SHM_DESERIALIZE_OPS(AR_TYPE)


}  // namespace hermes_shm::ipc

#endif  // HERMES_SHM_DATA_STRUCTURES_SHM_ARCHIVE_H_
