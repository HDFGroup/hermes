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

void MainPretest();
void MainPosttest();

#endif  // HERMES_SHM_TEST_UNIT_BASIC_TEST_H_
