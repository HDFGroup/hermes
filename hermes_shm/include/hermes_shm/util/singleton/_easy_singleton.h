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

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_UTIL_SINGLETON_EASY_SINGLETON_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_UTIL_SINGLETON_EASY_SINGLETON_H_

#include <memory>
#include "hermes_shm/thread/lock/mutex.h"
#include "hermes_shm/constants/macros.h"

namespace hshm {

/**
 * A class to represent singleton pattern
 * Does not require specific initialization of the static variable
 * */
template<typename T>
class EasySingleton {
 protected:
  /** static instance. */
  static T* obj_;
  static hshm::Mutex lock_;

 public:
  /**
   * Uses unique pointer to build a static global instance of variable.
   * @tparam T
   * @return instance of T
   */
  template<typename ...Args>
  static T* GetInstance(Args&& ...args) {
    if (obj_ == nullptr) {
      hshm::ScopedMutex lock(lock_, 0);
      if (obj_ == nullptr) {
        obj_ = new T(std::forward<Args>(args)...);
      }
    }
    return obj_;
  }
};
template <typename T>
T* EasySingleton<T>::obj_ = nullptr;
template <typename T>
hshm::Mutex EasySingleton<T>::lock_ = hshm::Mutex();

}  // namespace hshm

#endif  // HERMES_SHM_INCLUDE_HERMES_SHM_UTIL_SINGLETON_EASY_SINGLETON_H_
