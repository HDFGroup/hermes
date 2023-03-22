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

#ifndef HERMES_ERROR_SERIALIZER_H
#define HERMES_ERROR_SERIALIZER_H

#include <memory>
#include <vector>
#include <list>
#include <string>
#include <type_traits>
#include <cstring>
#include <sstream>
#include <hermes_shm/types/argpack.h>

#define NUMBER_SERIAL(type) \
    return std::to_string(num_.type);

namespace hermes_shm {

class Formattable {
 public:
  virtual std::string ToString() = 0;
};

class SizeType : public Formattable {
 public:
  double num_;
  size_t unit_;

  static const size_t
      BYTES = 1,
      KB = (1ul << 10),
      MB = (1ul << 20),
      GB = (1ul << 30),
      TB = (1ul << 40);

  std::string unit_to_str(size_t unit) {
    switch (unit) {
      case BYTES: return "BYTES";
      case KB: return "KB";
      case MB: return "MB";
      case GB: return "GB";
      case TB: return "TB";
    }
    return "";
  }

  SizeType() : num_(0), unit_(0) {}
  SizeType(const SizeType &old_obj) {
    num_ = old_obj.num_;
    unit_ = old_obj.unit_;
  }

  SizeType(int8_t bytes, size_t unit) :
  num_(((double)bytes)/unit), unit_(unit) {}
  SizeType(int16_t bytes, size_t unit) :
  num_(((double)bytes)/unit), unit_(unit) {}
  SizeType(int32_t bytes, size_t unit) :
  num_(((double)bytes)/unit), unit_(unit) {}
  SizeType(int64_t bytes, size_t unit) :
  num_(((double)bytes)/unit), unit_(unit) {}
  SizeType(uint8_t bytes, size_t unit) :
  num_(((double)bytes)/unit), unit_(unit) {}
  SizeType(uint16_t bytes, size_t unit) :
  num_(((double)bytes)/unit), unit_(unit) {}
  SizeType(uint32_t bytes, size_t unit) :
  num_(((double)bytes)/unit), unit_(unit) {}
  SizeType(uint64_t bytes, size_t unit) :
  num_(((double)bytes)/unit), unit_(unit) {}
  SizeType(float bytes, size_t unit) :
  num_(((double)bytes)/unit), unit_(unit) {}
  SizeType(double bytes, size_t unit) :
  num_(((double)bytes)/unit), unit_(unit) {}

  std::string ToString() {
    return std::to_string(num_) + unit_to_str(unit_);
  }
};

class Formatter {
 public:
  template<typename ...Args>
  static std::string format(std::string fmt, Args&& ...args) {
    std::stringstream ss;
    std::vector<std::pair<size_t, size_t>> offsets = tokenize(fmt);
    size_t packlen =
      make_argpack(std::forward<Args>(args)...).Size();
    if (offsets.size() != packlen + 1) {
      return fmt;
    }
    auto lambda = [&ss, &fmt, &offsets](auto i, auto &&arg) {
      if (i.Get() >= offsets.size()) { return; }
      auto &sub = offsets[i.Get()];
      ss << fmt.substr(sub.first, sub.second);
      ss << arg;
    };
    ForwardIterateArgpack::Apply(
      make_argpack(std::forward<Args>(args)...), lambda);
    if (offsets.back().second > 0) {
      auto &sub = offsets.back();
      ss << fmt.substr(sub.first, sub.second);
    }
    return ss.str();
  }

  static std::vector<std::pair<size_t, size_t>>
    tokenize(const std::string &fmt) {
    std::vector<std::pair<size_t, size_t>> offsets;
    size_t i = 0;
    offsets.emplace_back(std::pair<size_t, size_t>(0, fmt.size()));
    while (i < fmt.size()) {
      if (fmt[i] == '{') {
        // Set the size of the prior substring
        // E.g., "hello{}there".
        // prior.first is 0
        // i = 5 for the '{'
        // The substring's length is i - 0 = 5.
        auto &prior = offsets.back();
        prior.second = i - prior.first;

        // The token after the closing '}'
        // i = 5 for the '{'
        // i = 7 for the 't'
        // The total size is 12
        // The remaining size is: 12 - 7 = 5 (length of "there").
        i += 2;
        offsets.emplace_back(std::pair<size_t, size_t>(i, fmt.size() - i));
      }
      ++i;
    }
    return offsets;
  }
};

}  // namespace hermes_shm

#endif  //HERMES_ERROR_SERIALIZER_H
