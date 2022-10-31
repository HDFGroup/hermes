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

namespace hermes {

template<typename T>
class Timer {
 private:
  std::chrono::time_point<T> start_, end_;
  double time_ns_;

 public:
  Timer() : time_ns_(0) {}

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

template<typename T>
class ThreadedTimer {
 private:
  std::vector<Timer<T>> timers_;

 public:
  ThreadedTimer() = default;

  void Resume(int tid) {
    MinimumTID(tid);
    timers_[tid].Resume();
  }
  double Pause(int tid) {
    MinimumTID(tid);
    return timers_[tid].Pause();
  }
  double Pause(int tid, double &dt) {
    MinimumTID(tid);
    return timers_[tid].Pause(dt);
  }
  void Reset(int tid) {
    MinimumTID(tid);
    timers_[tid].Reset();
  }

  double GetMsecFromStart(int tid) {
    MinimumTID(tid);
    return timers_[tid].GetMsecFromStart();
  }

  double GetNsec(int tid) {
    MinimumTID(tid);
    return timers_[tid].GetNsec();
  }
  double GetUsec(int tid) {
    MinimumTID(tid);
    return timers_[tid].GetUsec();
  }
  double GetMsec(int tid) {
    MinimumTID(tid);
    return timers_[tid].GetMsec();
  }
  double GetSec(int tid) {
    MinimumTID(tid);
    return timers_[tid].GetSec();
  }
  double GetUsFromEpoch(int tid) {
    MinimumTID(tid);
    return timers_[tid].GetUsFromEpoch();
  }

  double GetMsecFromStart() const {
    auto iter = std::max_element(timers_.begin(), timers_.end(),
                   [] (Timer<T> const& lhs, Timer<T> const& rhs) {
                     return lhs.GetMsecFromStart() < rhs.GetMsecFromStart();
                   });
    return iter->GetMsecFromStart();
  }
  double GetNsec() const {
    auto iter = std::max_element(timers_.begin(), timers_.end(),
                   [] (Timer<T> const& lhs, Timer<T> const& rhs) {
                     return lhs.GetNsec() < rhs.GetNsec();
                   });
    return iter->GetNsec();
  }
  double GetUsec() const {
    auto iter = std::max_element(timers_.begin(), timers_.end(),
                   [] (Timer<T> const& lhs, Timer<T> const& rhs) {
                     return lhs.GetUsec() < rhs.GetUsec();
                   });
    return iter->GetUsec();
  }
  double GetMsec() const {
    auto iter = std::max_element(timers_.begin(), timers_.end(),
                   [] (Timer<T> const& lhs, Timer<T> const& rhs) {
                     return lhs.GetMsec() < rhs.GetMsec();
                   });
    return iter->GetMsec();
  }
  double GetSec() const {
    auto iter = std::max_element(timers_.begin(), timers_.end(),
                   [] (Timer<T> const& lhs, Timer<T> const& rhs) {
                     return lhs.GetSec() < rhs.GetSec();
                   });
    return iter->GetSec();
  }
  double GetUsFromEpoch() const {
    auto iter = std::max_element(timers_.begin(), timers_.end(),
                   [] (Timer<T> const& lhs, Timer<T> const& rhs) {
                     return lhs.GetUsFromEpoch() < rhs.GetUsFromEpoch();
                   });
    return iter->GetUsFromEpoch();
  }

 private:
  void MinimumTID(int tid) {
    if ((size_t)tid >= timers_.size()) {
      timers_.resize(tid+1);
    }
  }
};

typedef Timer<std::chrono::high_resolution_clock> HighResCpuTimer;
typedef Timer<std::chrono::steady_clock> HighResMonotonicTimer;
typedef ThreadedTimer<std::chrono::high_resolution_clock>
    ThreadedHighResCpuTimer;
typedef ThreadedTimer<std::chrono::steady_clock>
    ThreadedHighResMonotonicTimer;

}  // namespace labstor

#endif  // HERMES_TIMER_H
