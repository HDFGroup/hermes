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

namespace hshm {

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
        continue;
      }
      ++i;
    }
    return offsets;
  }
};

}  // namespace hshm

#endif  //HERMES_ERROR_SERIALIZER_H
