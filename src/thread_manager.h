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

#ifndef HERMES_SRC_THREAD_MANAGER_H_
#define HERMES_SRC_THREAD_MANAGER_H_

#include "hermes_types.h"
#include <thallium.hpp>
#include <future>

namespace hermes {

class ThreadManager {
 public:
  ABT_xstream execution_stream_;      /**< Argobots execution stream */
  std::atomic<bool> kill_requested_;  /**< Terminate threads spawned here */

 public:
  /** Default constructor */
  ThreadManager() : kill_requested_(false) {
    ABT_xstream_create(ABT_SCHED_NULL, &execution_stream_);
  }

  /** Spawn a for handling a function or lambda */
  template<typename FuncT, typename ParamsT = void>
  void Spawn(FuncT &&func, ParamsT *params = nullptr) {
    ABT_thread_create_on_xstream(execution_stream_,
                                 func, (void*)params,
                                 ABT_THREAD_ATTR_NULL, NULL);
  }

  /** Begin terminating threads */
  void Join() {
    kill_requested_.store(true);
    ABT_xstream_join(execution_stream_);
    ABT_xstream_free(&execution_stream_);
  }

  /** Whether the threads in this thread manager should still be executing */
  bool Alive() {
    return !kill_requested_.load();
  }
};

}  // namespace hermes

#define HERMES_THREAD_MANAGER \
  hshm::EasySingleton<hermes::ThreadManager>::GetInstance()

#endif  // HERMES_SRC_THREAD_MANAGER_H_
