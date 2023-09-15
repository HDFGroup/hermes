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

#ifndef HERMES_ADAPTER_TEST_UTILS_H
#define HERMES_ADAPTER_TEST_UTILS_H
#include <cmath>
#include <cstdio>
#include <string>
#include <basic_test.h>

bool FilesystemSupportsTmpfile() {
  bool result = false;

#if O_TMPFILE
  // NOTE(chogan): Even if O_TMPFILE is defined, the underlying filesystem might
  // not support it.
  int tmp_fd = open("/tmp", O_WRONLY | O_TMPFILE, 0600);
  if (tmp_fd > 0) {
    result = true;
    close(tmp_fd);
  }
#endif

  return result;
}

size_t GetRandomOffset(size_t i, unsigned int offset_seed, size_t stride,
                       size_t total_size) {
  return abs((int)(((i * rand_r(&offset_seed)) % stride) % total_size));
}

std::string GenRandom(const int len) {
  std::string tmp_s;
  static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

  srand(100);

  tmp_s.reserve(len);

  for (int i = 0; i < len; ++i) {
    tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];
  }

  tmp_s[len - 1] = '\n';

  return tmp_s;
}

#endif  // HERMES_ADAPTER_TEST_UTILS_H
