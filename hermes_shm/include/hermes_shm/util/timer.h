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

#ifndef HERMES_SHM_TIMER_H
#define HERMES_SHM_TIMER_H

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

#endif  // HERMES_SHM_TIMER_H
