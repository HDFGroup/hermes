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
  std::atomic<u32> x_;

  /** Default constructor */
  HistEntry() : x_(0) {}

  /** Constructor */
  explicit HistEntry(int x) : x_(x) {}

  /** Copy constructor */
  explicit HistEntry(const HistEntry &other) : x_(other.x_.load()) {}

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

  /** Get the bin score belongs to */
  u32 GetBin(float score) {
    u32 bin = score * histogram_.size();
    if (bin >= histogram_.size()) {
      bin = histogram_.size() - 1;
    }
    return bin;
  }

  /** Increment histogram */
  void Increment(float score) {
    u32 bin = GetBin(score);
    histogram_[bin].increment();
    count_.fetch_add(1);
  }

  /** Decrement histogram */
  void Decrement(float score) {
    u32 bin = GetBin(score);
    histogram_[bin].x_.fetch_sub(1);
    count_.fetch_sub(1);
  }

  /**
   * Determine if a blob should be elevated (1),
   * stationary (0), or demoted (-1)
   *
   * @input score a number between 0 and 1
   * @return Percentile (a number between 0 and 100)
   * */
  template<bool LESS_THAN_EQUAL>
  u32 GetPercentileBase(float score) {
    if (score == 0) {
      return 0;
    }
    if (count_ == 0) {
      return 100;
    }
    u32 bin = GetBin(score);
    u32 count = 0;
    if (LESS_THAN_EQUAL) {
      for (u32 i = 0; i <= bin; ++i) {
        count += histogram_[i].x_.load();
      }
    } else {
      for (u32 i = 0; i < bin; ++i) {
        count += histogram_[i].x_.load();
      }
    }
    return count * 100 / count_;
  }
  u32 GetPercentile(float score) {
    return GetPercentileBase<true>(score);
  }
  u32 GetPercentileLT(float score) {
    return GetPercentileBase<false>(score);
  }

  /**
   * Get quantile.
   * @input percentile is a number between 0 and 100
   * */
  float GetQuantile(u32 percentile) {
    u32 count = 0;
    if (count_ == 0) {
      return 0.0;
    }
    for (u32 i = 0; i < histogram_.size(); ++i) {
      count += histogram_[i].x_.load();
      if (count * 100 / count_ >= percentile && count > 0) {
        return (i + 1) / histogram_.size();
      }
    }
    return 0.0;
  }
};

}  // namespace hermes

#endif  // HERMES_INCLUDE_HERMES_SCORE_HISTOGRAM_H_
