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

#ifndef HERMES_TIMER_H
#define HERMES_TIMER_H

#include <chrono>
#include <vector>
#include <functional>

namespace hermes_shm {

template<typename T>
class TimerBase {
 private:
  std::chrono::time_point<T> start_, end_;
  double time_ns_;

 public:
  TimerBase() : time_ns_(0) {}

  void Resume() {
    start_ = T::now();
  }
  double Pause() {
    time_ns_ += GetNsecFromStart();
    return time_ns_;
  }
  double Pause(double &dt) {
    dt = GetNsecFromStart();
    time_ns_ += dt;
    return time_ns_;
  }
  void Reset() {
    time_ns_ = 0;
  }

  double GetNsecFromStart() {
    end_ = T::now();
    double elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
        end_ - start_).count();
    return elapsed;
  }
  double GetUsecFromStart() {
    end_ = T::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(
        end_ - start_).count();
  }
  double GetMsecFromStart() {
    end_ = T::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        end_ - start_).count();
  }
  double GetSecFromStart() {
    end_ = T::now();
    return std::chrono::duration_cast<std::chrono::seconds>(
        end_ - start_).count();
  }

  double GetNsec() const {
    return time_ns_;
  }
  double GetUsec() const {
    return time_ns_/1000;
  }
  double GetMsec() const {
    return time_ns_/1000000;
  }
  double GetSec() const {
    return time_ns_/1000000000;
  }

  double GetUsFromEpoch() const {
    std::chrono::time_point<std::chrono::system_clock> point =
        std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(
        point.time_since_epoch()).count();
  }
};

typedef TimerBase<std::chrono::high_resolution_clock> HighResCpuTimer;
typedef TimerBase<std::chrono::steady_clock> HighResMonotonicTimer;
typedef HighResMonotonicTimer Timer;

}  // namespace hermes_shm

#endif  // HERMES_TIMER_H
