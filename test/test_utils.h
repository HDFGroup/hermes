#ifndef HERMES_TEST_UTILS_H_
#define HERMES_TEST_UTILS_H_

#include <stdio.h>
#include <stdlib.h>

#include <chrono>
#include <vector>
#include <map>

#include "hermes_types.h"

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

struct TargetViewState {
  std::vector<hermes::u64> bytes_capacity;
  std::vector<hermes::u64> bytes_available;
  std::vector<hermes::f32> bandwidth;
  std::multimap<hermes::u64, size_t> ordered_cap;
  int num_devices;
};

}  // namespace testing
}  // namespace hermes

hermes::testing::TargetViewState InitDeviceState() {
  hermes::testing::TargetViewState result = {};
  result.num_devices = 4;

  result.bytes_available.push_back(MEGABYTES(5));
  result.bytes_available.push_back(MEGABYTES(20));
  result.bytes_available.push_back(MEGABYTES(50));
  result.bytes_available.push_back(MEGABYTES(200));

  result.bytes_capacity.push_back(MEGABYTES(5));
  result.bytes_capacity.push_back(MEGABYTES(20));
  result.bytes_capacity.push_back(MEGABYTES(50));
  result.bytes_capacity.push_back(MEGABYTES(200));

  result.bandwidth.push_back(6000);
  result.bandwidth.push_back(300);
  result.bandwidth.push_back(150);
  result.bandwidth.push_back(70);

  result.ordered_cap.insert(std::pair<hermes::u64, size_t>(MEGABYTES(5), 0));
  result.ordered_cap.insert(std::pair<hermes::u64, size_t>(MEGABYTES(20), 1));
  result.ordered_cap.insert(std::pair<hermes::u64, size_t>(MEGABYTES(50), 2));
  result.ordered_cap.insert(std::pair<hermes::u64, size_t>(MEGABYTES(200), 3));

  return result;
}

#define Assert(expr) hermes::testing::Assert((expr), __FILE__, __LINE__, #expr)

#endif  // HERMES_TEST_UTILS_H_
