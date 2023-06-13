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

#include "hermes_shm/thread/thread_model_manager.h"
#include "hermes_shm/util/logging.h"

namespace hshm {

/** Set the threading model of this application */
void ThreadModelManager::SetThreadModel(ThreadType type) {
  static std::mutex lock_;
  lock_.lock();
  if (type_ == type) {
    lock_.unlock();
    return;
  }
  type_ = type;
  thread_static_ = thread_model::ThreadFactory::Get(type);
  if (thread_static_ == nullptr) {
    HELOG(kFatal, "Could not load the threading model");
  }
  lock_.unlock();
}

/** Sleep for a period of time (microseconds) */
void ThreadModelManager::SleepForUs(size_t us) {
  thread_static_->SleepForUs(us);
}

/** Call Yield */
void ThreadModelManager::Yield() {
  thread_static_->Yield();
}

/** Call GetTid */
tid_t ThreadModelManager::GetTid() {
  return thread_static_->GetTid();
}

}  // namespace hshm
