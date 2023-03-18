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
#include "hermes_shm/memory/memory.h"
#include "shm_deserialize.h"

namespace hermes_shm::ipc {

/**
 * Constructs a TypedPointer in-place
 * */
template<typename T>
class _ShmArchive_Header {
 public:
  typedef typename T::header_t header_t;
  char obj_hdr_[sizeof(header_t)];  /**< Store object without constructing */
};

/**
 * Constructs an object in-place
 * */
template<typename T>
class _ShmArchive_T {
 public:
  typedef T header_t;
  char obj_hdr_[sizeof(T)];  /**< Store object without constructing */
};

/**
 * Whether or not to use _ShmArchive or _ShmArchive_T
 * */
#define SHM_MAKE_HEADER_OR_T(T) \
  SHM_X_OR_Y(T, _ShmArchive_Header<T>, _ShmArchive_T<T>)

/**
 * Represents the layout of a data structure in shared memory.
 * */
template<typename T>
class ShmArchive {
 public:
  typedef SHM_DESERIALIZE_OR_REF(T) T_Ar;
  typedef SHM_MAKE_HEADER_OR_T(T) T_Hdr;
  typedef typename T_Hdr::header_t header_t;
  T_Hdr obj_;

  /** Default constructor */
  ShmArchive() = default;

  /** Destructor */
  ~ShmArchive() = default;

  /** Pointer to internal object */
  header_t* get() {
    return &get_ref();
  }

  /** Pointer to internal object (const) */
  const header_t* get() const {
    return &get_ref();
  }

  /** Reference to internal object */
  header_t& get_ref() {
    return reinterpret_cast<header_t&>(obj_.obj_hdr_);
  }

  /** Reference to internal object (const) */
  const header_t& get_ref() const {
    return reinterpret_cast<header_t&>(obj_.obj_hdr_);
  }

  /** Copy constructor */
  ShmArchive(const ShmArchive &other) = delete;

  /** Copy assignment operator */
  ShmArchive& operator=(const ShmArchive &other) = delete;

  /** Move constructor */
  ShmArchive(ShmArchive &&other) = delete;

  /** Move assignment operator */
  ShmArchive& operator=(ShmArchive &&other) = delete;
};

}  // namespace hermes_shm::ipc

#endif  // HERMES_DATA_STRUCTURES_SHM_ARCHIVE_H_
