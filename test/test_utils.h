#ifndef HERMES_TEST_UTILS_H_
#define HERMES_TEST_UTILS_H_

#include <stdio.h>
#include <stdlib.h>

#define KILOBYTES(n) ((n) * 1024)
#define MEGABYTES(n) ((n) * 1024 * 1024)

namespace hermes {
namespace testing {

void Assert(bool expr, int lineno, const char *message) {
  if (!expr) {
    fprintf(stderr, "Assertion failed on line %d: %s\n", lineno, message);
    exit(-1);
  }
}

}  // namespace testing
}  // namespace hermes

#define Assert(expr) hermes::testing::Assert((expr), __LINE__, #expr)

#endif  // HERMES_TEST_UTILS_H_
