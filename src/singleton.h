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

#ifndef HERMES_ADAPTER_SINGLETON_H
#define HERMES_ADAPTER_SINGLETON_H

#include <iostream>
#include <memory>
#include <utility>

namespace hermes {
/**
 * Make a class singleton when used with the class. format for class name T
 * Singleton<T>::GetInstance()
 * @tparam T
 */

template<typename T>
class Singleton {
 private:
  static std::unique_ptr<T> obj_;
 public:
  Singleton() = default;

  /** Get or create an instance of type T */
  template<typename ...Args>
  inline static T* GetInstance(Args ...args) {
    if(!obj_) { obj_ = std::make_unique<T>(args...); }
    return obj_.get();
  }
};

}  // namespace hermes
#endif  // HERMES_ADAPTER_SINGLETON_H
