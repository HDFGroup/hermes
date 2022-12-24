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

#ifndef HERMES_STATUS_H_
#define HERMES_STATUS_H_

#include <iostream>

/** \file hermes_status.h */

namespace hermes::api {

class Status {
 private:
  static int code_counter_;
  int code_;
  const char *msg_;

 public:
  Status() : code_(0) {}

  explicit Status(const char *msg) : msg_(msg) {
    code_ = code_counter_;
    code_counter_ += 1;
  }

  Status(const Status &other) {
    code_ = other.code_;
    msg_ = other.msg_;
  }

  const char* Msg() {
    return msg_;
  }

  bool Success() {
    return code_ == 0;
  }

  bool Fail() {
    return code_ != 0;
  }
};

}  // hermes
#endif  // HERMES_STATUS_H_
