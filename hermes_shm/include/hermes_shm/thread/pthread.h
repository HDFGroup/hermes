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

#ifndef HERMES_SHM_THREAD_PTHREAD_H_
#define HERMES_SHM_THREAD_PTHREAD_H_

#include "thread.h"
#include <errno.h>
#include <hermes_shm/util/errors.h>

namespace hermes_shm {

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
    pthread_yield();
  }

  tid_t GetTid() override {
    return static_cast<tid_t>(pthread_self());
  }
};

}  // namespace hermes_shm

#endif  // HERMES_SHM_THREAD_PTHREAD_H_
