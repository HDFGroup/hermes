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

#ifndef HERMES_SHM_ERROR_SERIALIZER_H
#define HERMES_SHM_ERROR_SERIALIZER_H

#include <memory>
#include <vector>
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
    // make_argpack(std::forward<Args>(args)...);
    return fmt;
  }
};

}  // namespace hermes_shm

#endif  //HERMES_SHM_ERROR_SERIALIZER_H
