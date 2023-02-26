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


#ifndef HERMES_DATA_STRUCTURES_SHM_ARCHIVE_H_
#define HERMES_DATA_STRUCTURES_SHM_ARCHIVE_H_

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
  void operator>>(hipc::TypedPointer<TYPE_UNWRAP(TYPED_CLASS)> &ar) const {\
    shm_serialize(ar);\
  }\
  void operator>>(hipc::TypedAtomicPointer<TYPE_UNWRAP(TYPED_CLASS)> &ar) const {\
    shm_serialize(ar);\
  }

/**
 * Enables a specific TypedPointer type to be deserialized
 * */
#define SHM_DESERIALIZE_OPS(TYPED_CLASS)\
  void operator<<(const hipc::TypedPointer<TYPE_UNWRAP(TYPED_CLASS)> &ar) {\
    shm_deserialize(ar);\
  }\
  void operator<<(\
    const hipc::TypedAtomicPointer<TYPE_UNWRAP(TYPED_CLASS)> &ar) {\
    shm_deserialize(ar);\
  }

/** Enables serialization + deserialization for data structures */
#define SHM_SERIALIZE_DESERIALIZE_OPS(AR_TYPE)\
  SHM_SERIALIZE_OPS(AR_TYPE)\
  SHM_DESERIALIZE_OPS(AR_TYPE)


}  // namespace hermes_shm::ipc

#endif  // HERMES_DATA_STRUCTURES_SHM_ARCHIVE_H_
