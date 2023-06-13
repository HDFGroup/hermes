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

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_UTIL_SINGLETON__GLOBAL_SINGLETON_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_UTIL_SINGLETON__GLOBAL_SINGLETON_H_

#include <memory>
#include "hermes_shm/constants/macros.h"

namespace hshm {

/**
 * Makes a singleton. Constructs during initialization of program.
 * Requires user to define the static storage of obj_ in separate file.
 * */
template<typename T>
class GlobalSingleton {
 public:
  static T *obj_;

 public:
  GlobalSingleton() = default;

  /** Get instance of type T */
  HSHM_ALWAYS_INLINE static T* GetInstance() {
    return obj_;
  }

  /** Get ref of type T */
  HSHM_ALWAYS_INLINE static T& GetRef() {
    return *obj_;
  }

  /** Static initialization method for obj */
  static T& _GetObj();
};
template<typename T>
T* GlobalSingleton<T>::obj_ = &GlobalSingleton<T>::_GetObj();
#define DEFINE_GLOBAL_SINGLETON_CC(T)\
  template<> T& hshm::GlobalSingleton<T>::_GetObj() {\
    static T obj; \
    return obj;\
  }

}  // namespace hshm

#endif  // HERMES_SHM_INCLUDE_HERMES_SHM_UTIL_SINGLETON__GLOBAL_SINGLETON_H_
