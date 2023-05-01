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
#include "hermes_shm/types/argpack.h"
#include "hermes_shm/memory/allocator/allocator.h"

namespace hshm::ipc {

/**
 * Represents the layout of a data structure in shared memory.
 * */
template<typename T>
class ShmArchive {
 public:
  typedef T internal_t;
  char obj_[sizeof(T)];

  /** Default constructor */
  ShmArchive() = default;

  /** Destructor */
  ~ShmArchive() = default;

  /** Pointer to internal object */
  HSHM_ALWAYS_INLINE T* get() {
    return reinterpret_cast<T*>(obj_);
  }

  /** Pointer to internal object (const) */
  HSHM_ALWAYS_INLINE const T* get() const {
    return reinterpret_cast<T*>(obj_);
  }

  /** Reference to internal object */
  HSHM_ALWAYS_INLINE T& get_ref() {
    return reinterpret_cast<T&>(obj_);
  }

  /** Reference to internal object (const) */
  HSHM_ALWAYS_INLINE const T& get_ref() const {
    return reinterpret_cast<const T&>(obj_);
  }

  /** Dereference operator */
  HSHM_ALWAYS_INLINE T& operator*() {
    return get_ref();
  }

  /** Dereference operator */
  HSHM_ALWAYS_INLINE const T& operator*() const {
    return get_ref();
  }

  /** Arrow operator */
  HSHM_ALWAYS_INLINE T* operator->() {
    return get();
  }

  /** Arrow operator */
  HSHM_ALWAYS_INLINE const T* operator->() const {
    return get();
  }

  /** Copy constructor */
  ShmArchive(const ShmArchive &other) = delete;

  /** Copy assignment operator */
  ShmArchive& operator=(const ShmArchive &other) = delete;

  /** Move constructor */
  ShmArchive(ShmArchive &&other) = delete;

  /** Move assignment operator */
  ShmArchive& operator=(ShmArchive &&other) = delete;

  /** Initialize */
  template<typename ...Args>
  void shm_init(Args&& ...args) {
    Allocator::ConstructObj<T>(get_ref(), std::forward<Args>(args)...);
  }

  /** Initialize piecewise */
  template<typename ArgPackT_1, typename ArgPackT_2>
  void shm_init_piecewise(ArgPackT_1 &&args1, ArgPackT_2 &&args2) {
    return hshm::PassArgPack::Call(
      MergeArgPacks::Merge(
        make_argpack(get_ref()),
        std::forward<ArgPackT_1>(args1),
        std::forward<ArgPackT_2>(args2)),
      [](auto&& ...args) constexpr {
        Allocator::ConstructObj<T>(std::forward<decltype(args)>(args)...);
      });
  }
};

#define HSHM_AR_GET_TYPE(AR) \
  (typename std::remove_reference<decltype(AR)>::type::internal_t)

#define HSHM_MAKE_AR0(AR, ALLOC) \
  if constexpr(IS_SHM_ARCHIVEABLE(HSHM_AR_GET_TYPE(AR))) { \
    (AR).shm_init(ALLOC); \
  } else { \
    (AR).shm_init(); \
  }

#define HSHM_MAKE_AR(AR, ALLOC, ...) \
  if constexpr(IS_SHM_ARCHIVEABLE(HSHM_AR_GET_TYPE(AR))) { \
    (AR).shm_init(ALLOC, __VA_ARGS__); \
  } else { \
    (AR).shm_init(__VA_ARGS__); \
  }

#define HSHM_MAKE_AR_PW(AR, ALLOC, ...) \
  if constexpr(IS_SHM_ARCHIVEABLE(HSHM_AR_GET_TYPE(AR))) { \
    (AR).shm_init_piecewise(make_argpack(ALLOC), __VA_ARGS__); \
  } else { \
    (AR).shm_init_piecewise(make_argpack(), __VA_ARGS__); \
  }

#define HSHM_DESTROY_AR(AR) \
  if constexpr(IS_SHM_ARCHIVEABLE(HSHM_AR_GET_TYPE(AR))) { \
    (AR).get_ref().shm_destroy(); \
  }

}  // namespace hshm::ipc

#endif  // HERMES_DATA_STRUCTURES_SHM_ARCHIVE_H_
