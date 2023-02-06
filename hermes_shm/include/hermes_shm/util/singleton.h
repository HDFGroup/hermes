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

#ifndef SCS_SINGLETON_H
#define SCS_SINGLETON_H

#include <memory>
#include "hermes_shm/thread/lock/mutex.h"

namespace scs {

template<typename T>
class Singleton {
 private:
  static T obj_;
 public:
  Singleton() = default;
  static T* GetInstance() {
    return &obj_;
  }
};

}  // namespace scs

#endif  // SCS_SINGLETON_H
