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

#ifndef LABSTOR_TEST_UNIT_BASIC_TEST_H_
#define LABSTOR_TEST_UNIT_BASIC_TEST_H_

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

static inline bool CompareBuffers(char *p1, size_t s1, char *p2, size_t s2,
                                  size_t off) {
  if (s1 != s2) {
    return false;
  }
  for (size_t i = off; i < s1; ++i) {
    if (p1[i] != p2[i]) {
      std::cout << "Mismatch at: " << (int)i << std::endl;
      return false;
    }
  }
  return true;
}

void MainPretest();
void MainPosttest();

#define PAGE_DIVIDE(TEXT)

#endif  // LABSTOR_TEST_UNIT_BASIC_TEST_H_
