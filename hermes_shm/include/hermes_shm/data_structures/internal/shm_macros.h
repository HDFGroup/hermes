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

#ifndef HERMES_SHM_MEMORY_SHM_MACROS_H_
#define HERMES_SHM_MEMORY_SHM_MACROS_H_

#include <hermes_shm/constants/macros.h>

/**
 * Determine whether or not \a T type is designed for shared memory
 * */
#define IS_SHM_ARCHIVEABLE(T) \
  std::is_base_of<hermes_shm::ipc::ShmArchiveable, T>::value

/**
 * Determine whether or not \a T type is a SHM smart pointer
 * */
#define IS_SHM_SMART_POINTER(T) \
  std::is_base_of<hermes_shm::ipc::ShmSmartPointer, T>::value

/**
 * SHM_X_OR_Y: X if T is SHM_SERIALIZEABLE, Y otherwise
 * */
#define SHM_X_OR_Y(T, X, Y) \
  typename std::conditional<         \
    IS_SHM_ARCHIVEABLE(T), \
    X, Y>::type

/**
 * SHM_T_OR_PTR_T: Returns T if SHM_ARCHIVEABLE, and T* otherwise. Used
 * internally by lipc::ShmRef<T>.
 *
 * @param T: The type being stored in the shmem data structure
 * */
#define SHM_T_OR_PTR_T(T) \
  SHM_X_OR_Y(T, T, T*)

/**
 * ShmHeaderOrT: Returns TypedPointer<T> if SHM_ARCHIVEABLE, and T
 * otherwise. Used to construct an lipc::ShmRef<T>.
 *
 * @param T The type being stored in the shmem data structure
 * */
#define SHM_ARCHIVE_OR_T(T) \
  SHM_X_OR_Y(T, lipc::TypedPointer<T>, T)

/**
 * SHM_T_OR_SHM_STRUCT_T: Used by unique_ptr and manual_ptr to internally
 * store either a shm-compatible type or a POD (piece of data) type.
 *
 * @param T: The type being stored in the shmem data structure
 * */
#define SHM_T_OR_SHM_STRUCT_T(T) \
  SHM_X_OR_Y(T, T, ShmStruct<T>)

/**
 * SHM_T_OR_CONST_T: Determines whether or not an object should be
 * a constant or not.
 * */
#define SHM_CONST_T_OR_T(T, IS_CONST) \
  typename std::conditional<         \
    IS_CONST, \
    const T, T>::type

/**
 * SHM_ARCHIVE_OR_REF: Return value of shm_ar::internal_ref().
 * */
#define SHM_ARCHIVE_OR_REF(T)\
  SHM_X_OR_Y(T, TypedPointer<T>, T&)

#endif  // HERMES_SHM_MEMORY_SHM_MACROS_H_
