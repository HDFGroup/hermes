#ifndef HERMES_TEST_UTILS_H_
#define HERMES_TEST_UTILS_H_

#include <stdio.h>
#include <stdlib.h>

#include <chrono>

namespace hermes {
namespace testing {

// TODO(chogan): Keep time in generic units and only convert the duration at the
// end
class Timer {
public:
  Timer():elapsed_time(0) {}
  void resumeTime() {
    t1 = std::chrono::high_resolution_clock::now();
  }
  double pauseTime() {
    auto t2 = std::chrono::high_resolution_clock::now();
    elapsed_time += std::chrono::duration<double>(t2 - t1).count();
    return elapsed_time;
  }
  double getElapsedTime() {
    return elapsed_time;
  }
private:
  std::chrono::high_resolution_clock::time_point t1;
  double elapsed_time;
};

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
