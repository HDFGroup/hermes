#ifndef HERMES_UTILS_H_
#define HERMES_UTILS_H_

#include <chrono>

/**
 * @file utils.h
 *
 * Utility classes and functions for Hermes.
 */

namespace hermes {

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

}  // namespace hermes
#endif  // HERMES_UTILS_H_
