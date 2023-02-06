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

#ifndef HERMES_SHM_THREAD_THREAD_MANAGER_H_
#define HERMES_SHM_THREAD_THREAD_MANAGER_H_

#include "thread.h"
#include "thread_factory.h"
#include <hermes_shm/constants/data_structure_singleton_macros.h>

namespace hermes_shm {

class ThreadManager {
 public:
  ThreadType type_;
  std::unique_ptr<ThreadStatic> thread_static_;

  ThreadManager() : type_(ThreadType::kPthread) {}

  void SetThreadType(ThreadType type) {
    type_ = type;
  }

  ThreadStatic* GetThreadStatic() {
    if (!thread_static_) {
      thread_static_ = ThreadStaticFactory::Get(type_);
    }
    return thread_static_.get();
  }
};

}  // namespace hermes_shm

#endif  // HERMES_SHM_THREAD_THREAD_MANAGER_H_
