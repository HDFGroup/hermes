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


#ifndef HERMES_MEMORY_SHM_MACROS_H_
#define HERMES_MEMORY_SHM_MACROS_H_

#include "hermes_shm/constants/macros.h"

/**
 * Determine whether or not \a T type is designed for shared memory
 * */
#define IS_SHM_ARCHIVEABLE(T) \
  std::is_base_of<hshm::ipc::ShmArchiveable, T>::value

/**
 * Determine whether or not \a T type is a SHM smart pointer
 * */
#define IS_SHM_SMART_POINTER(T) \
  std::is_base_of<hshm::ipc::ShmSmartPointer, T>::value

/**
 * SHM_X_OR_Y: X if T is SHM_SERIALIZEABLE, Y otherwise
 * */
#define SHM_X_OR_Y(T, X, Y) \
  typename std::conditional<         \
    IS_SHM_ARCHIVEABLE(T), \
    TYPE_UNWRAP(X), TYPE_UNWRAP(Y)>::type

/**
 * SHM_ARCHIVE_OR_PTR_T: Returns ShmArchive if SHM_ARCHIVEABLE, and T*
 * otherwise.
 * */
#define SHM_ARCHIVE_OR_PTR_T(T) \
  SHM_X_OR_Y(T, hipc::ShmArchive<T>, T*)

/**
 * SHM_HEADER_OR_T: Returns header_t if SHM_ARCHIVEABLE, and T otherwise.
 * Used by ShmDeserialize.
 * */
template<typename T, typename = void>
struct HeaderOrT { using type = T; };
template<typename T>
struct HeaderOrT<T, std::void_t<typename T::NestedType>> {
  using type = typename T::header_t;
};

#define SHM_HEADER_OR_T(T) \
  typename HeaderOrT<T>::type

/**
 * SHM_DESERIALIZE_OR_REF: Return value of shm_ar::internal_ref().
 * */
#define SHM_DESERIALIZE_OR_REF(T)\
  SHM_X_OR_Y(T, ShmDeserialize<T>, T&)

#endif  // HERMES_MEMORY_SHM_MACROS_H_
