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
