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

#ifndef HERMES_INCLUDE_HERMES_SCORE_HISTOGRAM_H_
#define HERMES_INCLUDE_HERMES_SCORE_HISTOGRAM_H_

#include <vector>
#include <atomic>
#include <hermes_shm/types/atomic.h>

namespace hermes {

struct HistEntry {
  std::atomic<int> x_;

  /** Default constructor */
  HistEntry() : x_(0) {}

  /** Constructor */
  HistEntry(int x) : x_(x) {}

  /** Copy constructor */
  HistEntry(const HistEntry &other) : x_(other.x_.load()) {}

  /** Copy operator */
  HistEntry &operator=(const HistEntry &other) {
    x_.store(other.x_.load());
    return *this;
  }

  /** Move constructor */
  HistEntry(HistEntry &&other) noexcept : x_(other.x_.load()) {}

  /** Move operator */
  HistEntry &operator=(HistEntry &&other) noexcept {
    x_.store(other.x_.load());
    return *this;
  }

  void increment() {
    x_.fetch_add(1);
  }
};

class Histogram {
 public:
  std::vector<HistEntry> histogram_;
  std::atomic<u32> count_;

 public:
  /** Default constructor */
  Histogram() : histogram_(), count_(0) {}

  /** Copy constructor */
  Histogram(const Histogram &other) : histogram_(other.histogram_),
                                      count_(other.count_.load()) {}

  /** Copy operator */
  Histogram &operator=(const Histogram &other) {
    histogram_ = other.histogram_;
    count_.store(other.count_.load());
    return *this;
  }

  /** Move constructor */
  Histogram(Histogram &&other) noexcept : histogram_(other.histogram_),
                                          count_(other.count_.load()) {}

  /** Move operator */
  Histogram &operator=(Histogram &&other) noexcept {
    histogram_ = other.histogram_;
    count_.store(other.count_.load());
    return *this;
  }

  /** Resize the histogram */
  void Resize(int num_bins) {
    histogram_.resize(num_bins);
  }

  /** Increment histogram */
  void Increment(float score) {
    int bin = (int)(1.0/score - 1.0);
    histogram_[bin].increment();
    count_.fetch_add(1);
  }

  /** Decrement histogram */
  void Decrement(float score) {
    int bin = (int)(1.0/score - 1.0);
    histogram_[bin].x_.fetch_sub(1);
    count_.fetch_sub(1);
  }

  /**
   * Determine if a blob should be elevated (1),
   * stationary (0), or demoted (-1)
   * */
  u16 GetPercentile(float score) {
    int bin = (int)(1.0/score - 1.0);
    u32 count = 0;
    for (u32 i = 0; i <= bin; ++i) {
      count += histogram_[i].x_.load();
    }
    return count * 100 / count_;
  }
};

}  // namespace hermes

#endif  // HERMES_INCLUDE_HERMES_SCORE_HISTOGRAM_H_
