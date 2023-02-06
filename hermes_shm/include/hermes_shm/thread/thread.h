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

#ifndef HERMES_SHM_THREAD_THREAD_H_
#define HERMES_SHM_THREAD_THREAD_H_

#include <vector>
#include <cstdint>
#include <memory>
#include <atomic>

namespace hermes_shm {

typedef uint32_t tid_t;

enum class ThreadType {
  kPthread
};

class Thread {
 public:
  virtual void Pause() = 0;
  virtual void Resume() = 0;
  virtual void Join() = 0;
  void SetAffinity(int cpu_id) { SetAffinity(cpu_id, 1); }
  virtual void SetAffinity(int cpu_start, int count) = 0;
};

class ThreadStatic {
 public:
  virtual ~ThreadStatic() = default;
  virtual void Yield() = 0;
  virtual tid_t GetTid() = 0;
};

}  // namespace hermes_shm

#endif  // HERMES_SHM_THREAD_THREAD_H_
