//
// Created by lukemartinlogan on 1/11/23.
//

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_UTIL_AUTO_TRACE_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_UTIL_AUTO_TRACE_H_

#include "formatter.h"
#include "timer.h"

namespace hermes_shm {

#define AUTO_TRACE() AutoTrace(__PRETTY_FUNCTION__);

/** An in-memory log of trace times */
class AutoTraceLog {
 public:
  std::stringstream ss_;
  void emplace_back(const std::string &fname, Timer timer) {
    ss_ << fname << "," << timer.GetUsec() << "," << std::endl;
  }
};

/** Trace function execution times */
class AutoTrace {
 private:
  HighResMonotonicTimer timer_;
  std::string fname_;
  static AutoTraceLog log_;

 public:
  AutoTrace(const std::string &fname) : fname_(fname) {
    timer_.Resume();
  }

  ~AutoTrace() {
    timer_.Pause();
    log_.emplace_back(fname_, timer_);
  }
};

}  // namespace hermes_shm

#endif //HERMES_SHM_INCLUDE_HERMES_SHM_UTIL_AUTO_TRACE_H_
