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

#ifndef HERMES_SHM_DEBUG_H
#define HERMES_SHM_DEBUG_H

#if defined(HERMES_SHM_DEBUG) && defined(__cplusplus)
#define AUTO_TRACE(...) \
  hermes_shm::AutoTrace auto_tracer(false, __PRETTY_FUNCTION__, __VA_ARGS__);
#define TRACEPOINT(...) \
  hermes_shm::AutoTrace(true, __PRETTY_FUNCTION__, __VA_ARGS__);
#elif defined(KERNEL_BUILD) && defined(DEBUG)
#define AUTO_TRACE(...) pr_info(__VA_ARGS__);
#define TRACEPOINT(...) pr_info(__VA_ARGS__);
#else
#define AUTO_TRACE(...)
#define TRACEPOINT(...)
#endif

#ifdef __cplusplus

#include "stdio.h"
#include "hermes_shm/util/timer.h"
#include "hermes_shm/util/formatter.h"
#include <sched.h>
#include <unistd.h>

namespace hermes_shm {

class AutoTrace {
 private:
  std::string base_text_;
  bool tracepoint_;
  hermes_shm::HighResCpuTimer t_cpu_;
  hermes_shm::HighResMonotonicTimer t_total_;

 public:
  template<typename ...Args>
  AutoTrace(bool tracepoint, Args&& ...args) : tracepoint_(tracepoint) {
    // TODO(llogan): Redo with good argpack
    std::stringstream ss;
    base_text_ = ss.str();
    t_cpu_.Resume();
    t_total_.Resume();
    if (!tracepoint) {
      printf("%s\n", (base_text_ + "start").c_str());
    } else {
      printf("%d;%s\n", gettid(), (base_text_).c_str());
    }
  }

  ~AutoTrace() {
    if (!tracepoint_) {
      t_cpu_.Pause();
      t_total_.Pause();
      base_text_ += "cpu-time-us;" +
          std::to_string(t_cpu_.GetUsec()) + ";total-time-us;" +
          std::to_string(t_total_.GetUsec()) + ";";
      printf("%s\n", (base_text_ + "end").c_str());
    }
  }
};

}  // namespace hermes_shm

#endif

#endif  // HERMES_SHM_DEBUG_H
