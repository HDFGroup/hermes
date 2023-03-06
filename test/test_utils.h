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

#ifndef HERMES_TEST_TEST_UTILS_H_
#define HERMES_TEST_TEST_UTILS_H_

#include <iostream>
#include <cstdlib>

static bool VerifyBuffer(char *ptr, size_t size, char nonce) {
  for (size_t i = 0; i < size; ++i) {
    if (ptr[i] != nonce) {
      std::cout << "Mismatch at: " << (int)i << std::endl;
      return false;
    }
  }
  return true;
}

static bool CompareBuffers(char *p1, size_t s1, char *p2, size_t s2,
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

#endif  // HERMES_TEST_TEST_UTILS_H_
