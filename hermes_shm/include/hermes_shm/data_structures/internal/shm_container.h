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


#ifndef HERMES_SHM_SHM_CONTAINER_H_
#define HERMES_SHM_SHM_CONTAINER_H_

#include "hermes_shm/memory/memory_manager.h"
#include "hermes_shm/constants/macros.h"
#include "shm_container_macro.h"
#include "shm_macros.h"
#include "shm_archive.h"
#include "shm_ref.h"
#include "shm_deserialize.h"

namespace lipc = hermes_shm::ipc;

namespace hermes_shm::ipc {

/** Bits used for determining how to destroy an object */
/// The container's header has been allocated
#define SHM_CONTAINER_VALID BIT_OPT(uint16_t, 0)
/// The container header is initialized
#define SHM_CONTAINER_DATA_VALID BIT_OPT(uint16_t, 1)
/// The header was allocated by this container
#define SHM_CONTAINER_HEADER_DESTRUCTABLE BIT_OPT(uint16_t, 2)
/// The container should free all data when destroyed
#define SHM_CONTAINER_DESTRUCTABLE BIT_OPT(uint16_t, 3)

/** The shared-memory header used for data structures */
template<typename T>
struct ShmHeader;

/** The ShmHeader used for base containers */
struct ShmBaseHeader {
  bitfield32_t flags_;

  /** Default constructor */
  ShmBaseHeader() = default;

  /** Copy constructor */
  ShmBaseHeader(const ShmBaseHeader &other) {}

  /** Copy assignment operator */
  ShmBaseHeader& operator=(const ShmBaseHeader &other) {
    return *this;
  }

  /**
   * Disable copying of the flag field, as all flags
   * pertain to a particular ShmHeader allocation.
   * */
  void strong_copy(const ShmBaseHeader &other) {}

  /** Publicize bitfield operations */
  INHERIT_BITFIELD_OPS(flags_, uint16_t)
};

/** The ShmHeader used for wrapper containers */
struct ShmWrapperHeader {};

/**
 * ShmContainers all have a header, which is stored in
 * shared memory as a TypedPointer.
 * */
class ShmContainer : public ShmArchiveable {};

/** Typed nullptr */
template<typename T>
static inline T* typed_nullptr() {
  return reinterpret_cast<T*>(NULL);
}

}  // namespace hermes_shm::ipc

#endif  // HERMES_SHM_SHM_CONTAINER_H_
