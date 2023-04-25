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
  std::is_base_of<hshm::ipc::ShmContainer, TYPE_UNWRAP(T)>::value

/**
 * SHM_X_OR_Y: X if T is SHM_SERIALIZEABLE, Y otherwise
 * */
#define SHM_X_OR_Y(T, X, Y) \
  typename std::conditional<         \
    IS_SHM_ARCHIVEABLE(T), \
    TYPE_UNWRAP(X), TYPE_UNWRAP(Y)>::type

#endif  // HERMES_MEMORY_SHM_MACROS_H_
