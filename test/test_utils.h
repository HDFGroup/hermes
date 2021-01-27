#ifndef HERMES_TEST_UTILS_H_
#define HERMES_TEST_UTILS_H_

#include <stdio.h>
#include <stdlib.h>

#include <chrono>
#include <iostream>
#include <vector>
#include <map>

#include "hermes_types.h"
#include "bucket.h"

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

#define Assert(expr) hermes::testing::Assert((expr), __FILE__, __LINE__, #expr)

struct TargetViewState {
  std::vector<hermes::u64> bytes_capacity;
  std::vector<hermes::u64> bytes_available;
  std::vector<hermes::f32> bandwidth;
  std::multimap<hermes::u64, TargetID> ordered_cap;
  int num_devices;
};

TargetID DefaultRamTargetId() {
  TargetID result = {};
  result.bits.node_id = 1;
  result.bits.device_id = 0;
  result.bits.index = 0;

  return result;
}

TargetID DefaultFileTargetId() {
  TargetID result = {};
  result.bits.node_id = 1;
  result.bits.device_id = 1;
  result.bits.index = 1;

  return result;
}

std::vector<TargetID> GetDefaultTargets(size_t n) {
  std::vector<TargetID> result(n);
  for (size_t i = 0; i < n; ++i) {
    TargetID id = {};
    id.bits.node_id = 1;
    id.bits.device_id = (DeviceID)i;
    id.bits.index = i;
    result[i] = id;
  }

  return result;
}

TargetViewState InitDeviceState() {
  TargetViewState result = {};
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

  using hermes::TargetID;
  std::vector<TargetID> targets = GetDefaultTargets(result.num_devices);
  result.ordered_cap.insert(std::pair<hermes::u64, TargetID>(MEGABYTES(5),
                                                             targets[0]));
  result.ordered_cap.insert(std::pair<hermes::u64, TargetID>(MEGABYTES(20),
                                                             targets[1]));
  result.ordered_cap.insert(std::pair<hermes::u64, TargetID>(MEGABYTES(50),
                                                             targets[2]));
  result.ordered_cap.insert(std::pair<hermes::u64, TargetID>(MEGABYTES(200),
                                                             targets[3]));

  return result;
}

u64 UpdateDeviceState(PlacementSchema &schema,
                      TargetViewState &node_state) {
  u64 result {0};
  node_state.ordered_cap.clear();

  for (auto [size, target] : schema) {
    result += size;
    node_state.bytes_available[target.bits.device_id] -= size;
    node_state.ordered_cap.insert(
        std::pair<u64, TargetID>(
            node_state.bytes_available[target.bits.device_id], target));
  }

  return result;
}

void PrintNodeState(TargetViewState &node_state) {
  for (int i {0}; i < node_state.num_devices; ++i) {
    std::cout << "capacity of device[" << i << "]: "
              << node_state.bytes_available[i]
              << '\n' << std::flush;
    std::cout << "available ratio of device["<< i << "]: "
              << static_cast<double>(node_state.bytes_available[i])/
                 node_state.bytes_capacity[i]
              << '\n' << std::flush;
  }
}

void GetAndVerifyBlob(api::Bucket &bucket, const std::string &blob_name,
                      const api::Blob &expected) {
  api::Context ctx;
  api::Blob retrieved_blob;
  size_t expected_size = expected.size();
  size_t retrieved_size = bucket.Get(blob_name, retrieved_blob, ctx);
  Assert(expected_size == retrieved_size);
  retrieved_blob.resize(retrieved_size);
  retrieved_size = bucket.Get(blob_name, retrieved_blob, ctx);
  Assert(expected_size == retrieved_size);
  Assert(retrieved_blob == expected);
}

}  // namespace testing
}  // namespace hermes

#endif  // HERMES_TEST_UTILS_H_