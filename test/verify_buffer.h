//
// Created by lukemartinlogan on 12/27/22.
//

#ifndef HERMES_TEST_VERIFY_BUFFER_H_
#define HERMES_TEST_VERIFY_BUFFER_H_

#include <cstdlib>
#include <iostream>

static bool VerifyBuffer(char *ptr, size_t size, char nonce) {
  for (size_t i = 0; i < size; ++i) {
    if (ptr[i] != nonce) {
      std::cout << (int)ptr[i] << std::endl;
      return false;
    }
  }
  return true;
}

#endif  // HERMES_TEST_VERIFY_BUFFER_H_
