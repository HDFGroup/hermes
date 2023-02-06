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

#ifndef HERMES_SHM_ERROR_H
#define HERMES_SHM_ERROR_H

// #ifdef __cplusplus

#include <iostream>
#include <string>
#include <cstdlib>
#include <memory>
#include <hermes_shm/util/formatter.h>

#define HERMES_SHM_ERROR_TYPE std::shared_ptr<hermes_shm::Error>
#define HERMES_SHM_ERROR_HANDLE_START() try {
#define HERMES_SHM_ERROR_HANDLE_END() \
  } catch(HERMES_SHM_ERROR_TYPE &err) { err->print(); exit(-1024); }
#define HERMES_SHM_ERROR_HANDLE_TRY try
#define HERMES_SHM_ERROR_PTR err
#define HERMES_SHM_ERROR_HANDLE_CATCH catch(HERMES_SHM_ERROR_TYPE &HERMES_SHM_ERROR_PTR)
#define HERMES_SHM_ERROR_IS(err, check) (err->get_code() == check.get_code())

namespace hermes_shm {

class Error {
 private:
  std::string fmt_;
  std::string msg_;
 public:
  Error() : fmt_() {}
  explicit Error(std::string fmt) : fmt_(std::move(fmt)) {}
  ~Error() = default;

  template<typename ...Args>
  std::shared_ptr<Error> format(Args&& ...args) const {
    std::shared_ptr<Error> err = std::make_shared<Error>(fmt_);
    err->msg_ = Formatter::format(fmt_, std::forward<Args>(args)...);
    return err;
  }

  void print() {
    std::cout << msg_ << std::endl;
  }
};

}  // namespace hermes_shm

// #endif

#endif  // HERMES_SHM_ERROR_H
