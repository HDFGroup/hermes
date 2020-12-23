//
// Created by manihariharan on 12/23/20.
//

#ifndef HERMES_SINGLETON_H
#define HERMES_SINGLETON_H
#include <iostream>
#include <memory>
#include <utility>
/**
 * Make a class singleton when used with the class. format for class name T
 * Singleton<T>::GetInstance()
 * @tparam T
 */
namespace hermes::adapter {
template<typename T>
class Singleton {
 public:
  /**
   * Members of Singleton Class
   */
  /**
   * Uses unique pointer to build a static global instance of variable.
   * @tparam T
   * @return instance of T
   */
  template <typename... Args>
  static std::shared_ptr<T> GetInstance(Args... args) {
    if (instance == nullptr)
      instance = std::shared_ptr<T>(new T(std::forward<Args>(args)...));
    return instance;
  }

  /**
   * Operators
   */
  Singleton& operator= (const Singleton) = delete; /* deleting = operatos*/
  /**
   * Constructor
   */
 public:
  Singleton(const Singleton&) = delete; /* deleting copy constructor. */

 protected:
  static std::shared_ptr<T> instance;
  Singleton() {} /* hidden default constructor. */
};

template<typename T>
std::shared_ptr<T> Singleton<T>::instance = nullptr;
}
#endif  // HERMES_SINGLETON_H
