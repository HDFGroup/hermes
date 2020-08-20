#ifndef HERMES_TEST_UTILS_H_
#define HERMES_TEST_UTILS_H_

#include <stdio.h>
#include <stdlib.h>

namespace hermes {
namespace testing {

void Assert(bool expr, const char *file, int lineno, const char *message) {
  if (!expr) {
    fprintf(stderr, "Assertion failed at %s: line %d: %s\n", file, lineno,
            message);
    exit(-1);
  }
}

}  // namespace testing
}  // namespace hermes

#define Assert(expr) hermes::testing::Assert((expr), __FILE__, __LINE__, #expr)

#endif  // HERMES_TEST_UTILS_H_
