//
// Created by lukemartinlogan on 3/3/23.
//

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
