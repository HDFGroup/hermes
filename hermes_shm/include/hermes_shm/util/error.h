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

#ifndef HERMES_ERROR_H
#define HERMES_ERROR_H

// #ifdef __cplusplus

#include <iostream>
#include <string>
#include <cstdlib>
#include <memory>
#include <hermes_shm/util/formatter.h>

#define HERMES_ERROR_TYPE std::shared_ptr<hshm::Error>
#define HERMES_ERROR_HANDLE_START() try {
#define HERMES_ERROR_HANDLE_END() \
  } catch(HERMES_ERROR_TYPE &err) { err->print(); exit(-1024); }
#define HERMES_ERROR_HANDLE_TRY try
#define HERMES_ERROR_PTR err
#define HERMES_ERROR_HANDLE_CATCH catch(HERMES_ERROR_TYPE &HERMES_ERROR_PTR)
#define HERMES_ERROR_IS(err, check) (err->get_code() == check.get_code())

namespace hshm {

class Error : std::exception {
 private:
  std::string fmt_;
  std::string msg_;
 public:
  Error() : fmt_() {}
  explicit Error(std::string fmt) : fmt_(std::move(fmt)) {}
  ~Error() override = default;

  template<typename ...Args>
  Error format(Args&& ...args) const {
    Error err = Error(fmt_);
    err.msg_ = Formatter::format(fmt_, std::forward<Args>(args)...);
    return err;
  }

  const char* what() const throw() override{
    return msg_.c_str();
  }

  void print() {
    std::cout << what() << std::endl;
  }
};

}  // namespace hshm

// #endif

#endif  // HERMES_ERROR_H
