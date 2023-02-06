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

#ifndef HERMES_SHM_TEST_UNIT_BASIC_TEST_H_
#define HERMES_SHM_TEST_UNIT_BASIC_TEST_H_

#define CATCH_CONFIG_RUNNER
#include <catch2/catch_all.hpp>

namespace cl = Catch::Clara;
cl::Parser define_options();

#include <iostream>
#include <cstdlib>

static bool VerifyBuffer(char *ptr, size_t size, char nonce) {
  for (size_t i = 0; i < size; ++i) {
    if (ptr[i] != nonce) {
      std::cout << (int)ptr[i] << std::endl;
      return false;
    }
  }
  return true;
}

/** var = TYPE(val) */
#define SET_VAR_TO_INT_OR_STRING(TYPE, VAR, VAL)\
  if constexpr(std::is_same_v<TYPE, lipc::string>) {\
    VAR = lipc::string(std::to_string(VAL));\
  } else if constexpr(std::is_same_v<TYPE, std::string>) {\
    VAR = std::string(std::to_string(VAL));\
  } else {\
    VAR = VAL;\
  }

/** TYPE VAR = TYPE(VAL) */
#define CREATE_SET_VAR_TO_INT_OR_STRING(TYPE, VAR, VAL)\
  TYPE VAR;\
  SET_VAR_TO_INT_OR_STRING(TYPE, VAR, VAL);

/** RET = int(TYPE(VAR)); */
#define GET_INT_FROM_VAR(TYPE, RET, VAR)\
  if constexpr(std::is_same_v<TYPE, lipc::string>) {\
    RET = atoi((VAR).str().c_str());\
  } else if constexpr(std::is_same_v<TYPE, std::string>) {\
    RET = atoi((VAR).c_str());\
  } else {\
    RET = VAR;\
  }

/** int RET = int(TYPE(VAR)); */
#define CREATE_GET_INT_FROM_VAR(TYPE, RET, VAR)\
  int RET;\
  GET_INT_FROM_VAR(TYPE, RET, VAR)

void MainPretest();
void MainPosttest();

#endif  // HERMES_SHM_TEST_UNIT_BASIC_TEST_H_
