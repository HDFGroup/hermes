#ifndef HERMES_ADAPTER_TEST_UTILS_H
#define HERMES_ADAPTER_TEST_UTILS_H
#include <cmath>
#include <cstdio>
size_t GetRandomOffset(size_t i, unsigned int offset_seed, size_t stride,
                       size_t total_size) {
  return abs((int)(((i * rand_r(&offset_seed)) % stride) % total_size));
}
#endif  // HERMES_ADAPTER_TEST_UTILS_H
