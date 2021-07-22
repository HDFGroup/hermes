#ifndef HERMES_ADAPTER_TEST_UTILS_H
#define HERMES_ADAPTER_TEST_UTILS_H
#include <cmath>
#include <cstdio>
#include <string>

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

  for (int i = 0; i < len; ++i)
    tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];

  return tmp_s;
}

#endif  // HERMES_ADAPTER_TEST_UTILS_H
