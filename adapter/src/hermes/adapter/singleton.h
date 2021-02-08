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
/**
 * Make a class singleton when used with the class. format for class name T
 * Singleton<T>::GetInstance()
 * @tparam T
 */
namespace hermes::adapter {
template <typename T>
class Singleton {
 public:
  /**
   * Uses unique pointer to build a static global instance of variable.
   * @tparam T
   * @return instance of T
   */
  template <typename... Args>
  static std::shared_ptr<T> GetInstance(Args... args) {
    if (instance == nullptr)
      instance = std::make_shared<T>(std::forward<Args>(args)...);
    return instance;
  }

  /**
   * Operators
   */
  Singleton& operator=(const Singleton) = delete; /* deleting = operators*/

 public:
  /**
   * Constructor
   */
  Singleton(const Singleton&) = delete; /* deleting copy constructor. */

 protected:
  /**
   * static instance.
   */
  static std::shared_ptr<T> instance;
  Singleton() {} /* hidden default constructor. */
};

template <typename T>
std::shared_ptr<T> Singleton<T>::instance = nullptr;
}  // namespace hermes::adapter
#endif  // HERMES_ADAPTER_SINGLETON_H
