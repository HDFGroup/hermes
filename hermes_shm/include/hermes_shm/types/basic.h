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

#ifndef HERMES_BASICS_H
#define HERMES_BASICS_H

#define MODULE_KEY_SIZE 32

#include <cstdint>
using std::size_t;

#ifdef KERNEL_BUILD
#include <linux/types.h>
#elif __cplusplus
#include <stdint.h>
#include <string>
#include <cstring>
#include <unordered_map>
#include <limits>
#endif

typedef uint32_t labstor_runtime_id_t;
typedef int32_t labstor_off_t;

struct labstor_id {
  char key_[MODULE_KEY_SIZE];
};

struct labstor_credentials {
  int pid_;
  int uid_;
  int gid_;
  int priority_;
};

#ifdef __cplusplus

namespace hermes_shm {

typedef labstor_credentials UserCredentials;

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
      (decimal_ * other.numerator_) +  // d1 * n2
      (other.decimal_ * numerator_) +  // d2 * n1
      (numerator_ * other.numerator_) / precision;  // n1 * n2 / p
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

#endif



#endif  // HERMES_BASICS_H
