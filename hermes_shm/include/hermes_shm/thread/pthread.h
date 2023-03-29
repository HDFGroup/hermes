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

#ifndef HERMES_THREAD_PTHREAD_H_
#define HERMES_THREAD_PTHREAD_H_

#include "thread.h"
#include <errno.h>
#include <hermes_shm/util/errors.h>
#include <omp.h>

namespace hshm {

template<typename BIND> class Pthread;

template<typename BIND>
struct PthreadParams {
  BIND bind_;
  Pthread<BIND> *pthread_;

  PthreadParams(Pthread<BIND> *pthread, BIND bind) :
    bind_(bind),
    pthread_(pthread) {}
};

template<typename BIND>
class Pthread : public Thread {
 private:
  pthread_t pthread_;

 public:
  bool started_;

 public:
  Pthread() = default;
  explicit Pthread(BIND bind) : started_(false), pthread_(-1) {
    PthreadParams<BIND> params(this, bind);
    int ret = pthread_create(&pthread_, nullptr,
                             DoWork,
                             &params);
    if (ret != 0) {
      throw PTHREAD_CREATE_FAILED.format();
    }
    while (!started_) {}
  }

  void Pause() override {}

  void Resume() override {}

  void Join() override {
    void *ret;
    pthread_join(pthread_, &ret);
  }

  void SetAffinity(int cpu_start, int count) override {
    /*cpu_set_t cpus[n_cpu_];
    CPU_ZERO(cpus);
    CPU_SET(cpu_id, cpus);
    pthread_setaffinity_np_safe(n_cpu_, cpus);*/
  }

 private:
  static void* DoWork(void *void_params) {
    auto params = (*reinterpret_cast<PthreadParams<BIND>*>(void_params));
    params.pthread_->started_ = true;
    params.bind_();
    return nullptr;
  }

  inline void pthread_setaffinity_np_safe(int n_cpu, cpu_set_t *cpus) {
    int ret = pthread_setaffinity_np(pthread_, n_cpu, cpus);
    if (ret != 0) {
      throw INVALID_AFFINITY.format(strerror(ret));
    }
  }
};

class PthreadStatic : public ThreadStatic {
 public:
  void Yield() override {
    sched_yield();
  }

  tid_t GetTid() override {
    return omp_get_thread_num();
    // return static_cast<tid_t>(pthread_self());
  }
};

}  // namespace hshm

#endif  // HERMES_THREAD_PTHREAD_H_
