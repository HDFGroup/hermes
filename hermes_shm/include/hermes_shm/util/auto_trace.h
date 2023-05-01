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

#ifndef HERMES_INCLUDE_HERMES_UTIL_AUTO_TRACE_H_
#define HERMES_INCLUDE_HERMES_UTIL_AUTO_TRACE_H_

#include "formatter.h"
#include "timer.h"
#include "logging.h"
#include <iostream>

namespace hshm {

#ifdef HERMES_ENABLE_PROFILING
#define AUTO_TRACE(LOG_LEVEL) \
  hshm::AutoTrace<LOG_LEVEL> hshm_tracer_(__func__);
#define TIMER_START(NAME) \
  hshm_tracer_.StartTimer(NAME);
#define TIMER_END()  \
  hshm_tracer_.EndTimer();
#else
#define AUTO_TRACE(LOG_LEVEL)
#define TIMER_START(NAME)
#define TIMER_END()
#endif

/** Trace function execution times */
template<int LOG_LEVEL>
class AutoTrace {
 private:
  HighResMonotonicTimer timer_;
  HighResMonotonicTimer timer2_;
  std::string fname_;
  std::string internal_name_;

 public:
  template<typename ...Args>
  explicit AutoTrace(const char *fname) : fname_(fname) {
    _StartTimer(timer_);
  }

  ~AutoTrace() {
    _EndTimer(timer_);
  }

  void StartTimer(const char *internal_name) {
    internal_name_ = "/" + std::string(internal_name);
    _StartTimer(timer2_);
  }

  void EndTimer() {
    _EndTimer(timer2_);
  }

 private:
  template<typename ...Args>
  void _StartTimer(HighResMonotonicTimer &timer) {
#ifdef HERMES_ENABLE_PROFILING
    if constexpr(LOG_LEVEL <= HERMES_LOG_VERBOSITY) {
      timer.Resume();
      HILOG(LOG_LEVEL, "{}{}",
           fname_,
           internal_name_)
    }
#endif
  }

  void _EndTimer(HighResMonotonicTimer &timer) {
#ifdef HERMES_ENABLE_PROFILING
    if constexpr(LOG_LEVEL <= HERMES_LOG_VERBOSITY) {
      timer.Pause();
      HILOG(LOG_LEVEL, "{}{} {}ns",
           fname_,
           internal_name_,
           timer.GetNsec())
      timer.Reset();
      internal_name_.clear();
    }
#endif
  }
};

}  // namespace hshm

#endif  // HERMES_INCLUDE_HERMES_UTIL_AUTO_TRACE_H_
