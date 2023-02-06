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

#ifndef HERMES_SHM_BASICS_H
#define HERMES_SHM_BASICS_H

#define MODULE_KEY_SIZE 32

#include <cstdint>
using std::size_t;

#include <stdint.h>
#include <string>
#include <cstring>
#include <unordered_map>
#include <limits>

namespace hermes_shm {

/**
 * decimal + (numerator/65536)
 * */
struct RealNumber {
  uint64_t decimal_;
  uint32_t numerator_;
  static const uint32_t precision = 65536;

  RealNumber() =  default;

  /**
   * Converts numerator / denomintor ->
   * decimal + (numerator / 65536);
   *
   * For example,
   * 4/5 = (4 * 65536 / 5) / 65536
   * */
  explicit RealNumber(uint64_t numerator, uint64_t denominator) {
    decimal_ = numerator / denominator;
    uint64_t rem = numerator % denominator;
    numerator_ = rem * precision / denominator;
  }

  /**
   * (d1 + n1/p) * d2 =
   * d1 * d2 + d2 * n1 / p
   * */
  RealNumber operator*(size_t other) const {
    RealNumber res;
    res.decimal_ = other * decimal_;
    uint64_t frac = other * numerator_;
    res.decimal_ += frac / precision;
    res.numerator_ = frac % precision;
    return res;
  }

  /**
   * (d1 + n1/p) * (d2 + n2/p) =
   * (d1 * d2) + (d1 * n2)/p + (d2 * n1) / p + (n1 * n2 / p) / p =
   * (d1 * d2) + [(d1 * n2) + (d2 * n1) + (n1 * n2)/p] / p
   * */
  RealNumber operator*(const RealNumber &other) const {
    RealNumber res;
    // d1 * d2
    res.decimal_ = other.decimal_ * decimal_;
    uint64_t frac =
      (decimal_ * other.numerator_) + // d1 * n2
      (other.decimal_ * numerator_) + // d2 * n1
      (numerator_ * other.numerator_) / precision; // n1 * n2 / p
    res.decimal_ += frac / precision;
    res.numerator_ = frac % precision;
    return res;
  }

  RealNumber operator*=(size_t other) {
    (*this) = (*this) * other;
    return *this;
  }

  RealNumber operator*=(const RealNumber &other) {
    (*this) = (*this) * other;
    return *this;
  }

  size_t as_int() const {
    return decimal_ + numerator_ / precision;
  }
};

struct id {
  char key_[MODULE_KEY_SIZE];
  id() = default;
  ~id() = default;
  explicit id(const std::string &key_str) {
    snprintf(key_, MODULE_KEY_SIZE, "%s", key_str.c_str());
  }
  explicit id(const char* key_str) {
    snprintf(key_, MODULE_KEY_SIZE, "%s", key_str);
  }
  bool operator==(const id &other) const {
    return strncmp(key_, other.key_, MODULE_KEY_SIZE) == 0;
  }
  void copy(const std::string &str) {
    memcpy(key_, str.c_str(), str.size());
    key_[str.size()] = 0;
  }
  const char& operator[](int i) {
    return key_[i];
  }
};

typedef int32_t off_t;

}  // namespace hermes_shm

namespace std {
template<>
struct hash<hermes_shm::id> {
  size_t operator()(const hermes_shm::id &id) const {
    size_t sum = 0;
    int len = strnlen(id.key_, MODULE_KEY_SIZE);
    for (int i = 0; i < len; ++i) {
      if (id.key_[i] == 0) { break; }
      sum += id.key_[i] << (i % 8);
    }
    return sum;
  }
};
}  // namespace std

#endif  // HERMES_SHM_BASICS_H