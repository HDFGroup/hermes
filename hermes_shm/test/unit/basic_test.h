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

#ifndef HERMES_TEST_UNIT_BASIC_TEST_H_
#define HERMES_TEST_UNIT_BASIC_TEST_H_

#define CATCH_CONFIG_RUNNER
#include <catch2/catch_all.hpp>

namespace cl = Catch::Clara;
cl::Parser define_options();

#include <iostream>
#include <cstdlib>

static inline bool VerifyBuffer(char *ptr, size_t size, char nonce) {
  for (size_t i = 0; i < size; ++i) {
    if (ptr[i] != nonce) {
      std::cout << (int)ptr[i] << std::endl;
      return false;
    }
  }
  return true;
}

/** var = TYPE(val) */
#define _CREATE_SET_VAR_TO_INT_OR_STRING(TYPE, VAR, TMP_VAR, VAL)\
  if constexpr(std::is_same_v<TYPE, hipc::string>) {\
    TMP_VAR = hipc::make_uptr<hipc::string>(std::to_string(VAL));\
  } else if constexpr(std::is_same_v<TYPE, std::string>) {\
    TMP_VAR = hipc::make_uptr<std::string>(std::to_string(VAL));\
  } else {\
    TMP_VAR = hipc::make_uptr<int>(VAL);\
  }\
  TYPE &VAR = *TMP_VAR;

/** TYPE VAR = TYPE(VAL) */
#define CREATE_SET_VAR_TO_INT_OR_STRING(TYPE, VAR, VAL)\
  hipc::uptr<TYPE> VAR##_tmp;\
  _CREATE_SET_VAR_TO_INT_OR_STRING(TYPE, VAR, VAR##_tmp, VAL);

/** RET = int(TYPE(VAR)); */
#define GET_INT_FROM_VAR(TYPE, RET, VAR)\
  if constexpr(std::is_same_v<TYPE, hipc::string>) {\
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

#define PAGE_DIVIDE(TEXT)

#endif  // HERMES_TEST_UNIT_BASIC_TEST_H_
