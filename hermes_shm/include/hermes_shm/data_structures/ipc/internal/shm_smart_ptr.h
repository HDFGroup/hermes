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


#ifndef HERMES_DATA_STRUCTURES_INTERNAL_SHM_DATA_STRUCTURE_POINTER_H_
#define HERMES_DATA_STRUCTURES_INTERNAL_SHM_DATA_STRUCTURE_POINTER_H_

namespace hshm::ipc {

/**
 * Indicates a data structure represents a memory paradigm for Shm.
 * */
class ShmSmartPointer {};

}  // namespace hshm::ipc

/**
 * Enables a specific TypedPointer type to be serialized
 * */
#define SHM_SERIALIZE_OPS(TYPED_CLASS)\
  void operator>>(hipc::TypedPointer<TYPE_UNWRAP(TYPED_CLASS)> &ar) const {\
    shm_serialize(ar);\
  }\
  void operator>>(\
      hipc::TypedAtomicPointer<TYPE_UNWRAP(TYPED_CLASS)> &ar) const {\
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

/**
 * A macro for defining shared memory (de)serializations
 * */
#define SHM_SERIALIZE_DESERIALIZE_OPS(AR_TYPE)\
  SHM_SERIALIZE_OPS(AR_TYPE)\
  SHM_DESERIALIZE_OPS(AR_TYPE)

#endif  // HERMES_DATA_STRUCTURES_INTERNAL_SHM_DATA_STRUCTURE_POINTER_H_
