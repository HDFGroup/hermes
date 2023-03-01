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
  explicit AutoTrace(const std::string &fname) : fname_(fname) {
    timer_.Resume();
  }

  ~AutoTrace() {
    timer_.Pause();
    log_.emplace_back(fname_, timer_);
  }
};

}  // namespace hermes_shm

#endif  // HERMES_INCLUDE_HERMES_UTIL_AUTO_TRACE_H_
