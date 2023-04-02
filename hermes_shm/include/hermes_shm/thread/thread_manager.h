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


#ifndef HERMES_THREAD_THREAD_MANAGER_H_
#define HERMES_THREAD_THREAD_MANAGER_H_

#include "thread.h"
#include "thread_factory.h"
#include <hermes_shm/constants/data_structure_singleton_macros.h>
#include <hermes_shm/introspect/system_info.h>
#include <mutex>

#define US_TO_CLOCKS(x) (x * 56)

namespace hshm {

union NodeThreadId;

class ThreadManager {
 public:
  ThreadType type_;
  std::unique_ptr<ThreadStatic> thread_static_;
  std::mutex lock_;

  ThreadManager() : type_(ThreadType::kPthread) {}

  void SetThreadType(ThreadType type) {
    type_ = type;
  }

  ThreadStatic* GetThreadStatic() {
    if (!thread_static_) {
      lock_.lock();
      if (!thread_static_) {
        thread_static_ = ThreadStaticFactory::Get(type_);
      }
      lock_.unlock();
    }
    return thread_static_.get();
  }
};

union NodeThreadId {
  struct {
    uint32_t tid_;
    uint32_t pid_;
  } bits_;
  uint64_t as_int_;

  NodeThreadId() {
    bits_.tid_ = HSHM_THREAD_MANAGER->GetThreadStatic()->GetTid();
    bits_.pid_ = HERMES_SYSTEM_INFO->pid_;
  }

  uint32_t hash() {
    return as_int_;
  }
};

}  // namespace hshm

#endif  // HERMES_THREAD_THREAD_MANAGER_H_
