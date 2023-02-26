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


#ifndef HERMES_SHM_CONTAINER_H_
#define HERMES_SHM_CONTAINER_H_

#include "hermes_shm/memory/memory_registry.h"
#include "hermes_shm/constants/macros.h"
#include "shm_container_macro.h"
#include "shm_macros.h"
#include "shm_archive.h"
#include "shm_ref.h"
#include "shm_deserialize.h"

namespace hipc = hermes_shm::ipc;

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

#endif  // HERMES_SHM_CONTAINER_H_
