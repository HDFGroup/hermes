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

#ifndef HERMES_THREAD_THREAD_H_
#define HERMES_THREAD_THREAD_H_

#include <vector>
#include <cstdint>
#include <memory>
#include <atomic>
#include "hermes_shm/types/bitfield.h"

namespace hshm {

/** Available threads that are mapped */
enum class ThreadType {
  kNone,
  kPthread,
  kArgobots
};

/** Used to represent tid */
typedef uint64_t tid_t;

}  // namespace hshm

namespace hshm::thread_model {
/** Represents the generic operations of a thread */
class ThreadModel {
 public:
  /** Sleep thread for a period of time */
  virtual void SleepForUs(size_t us) = 0;

  /** Yield thread time slice */
  virtual void Yield() = 0;

  /** Get the TID of the current thread */
  virtual tid_t GetTid() = 0;
};

}  // namespace hshm::thread_model

#endif  // HERMES_THREAD_THREAD_H_
