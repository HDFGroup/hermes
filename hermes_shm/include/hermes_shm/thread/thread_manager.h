/*
 * Copyright (C) 2022  SCS Lab <scslab@iit.edu>,
 * Luke Logan <llogan@hawk.iit.edu>,
 * Jaime Cernuda Garcia <jcernudagarcia@hawk.iit.edu>
 * Jay Lofstead <gflofst@sandia.gov>,
 * Anthony Kougkas <akougkas@iit.edu>,
 * Xian-He Sun <sun@iit.edu>
 *
 * This file is part of HermesShm
 *
 * HermesShm is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */


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
