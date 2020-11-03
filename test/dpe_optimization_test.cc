#include <iostream>
#include <random>
#include <map>

#include "hermes.h"
#include "data_placement_engine.h"
#include "test_utils.h"

using namespace hermes;  // NOLINT(*)
namespace hapi = hermes::api;

namespace hermes {
namespace testing {
struct TargetViewState {
  std::vector<u64> bytes_capacity;
  std::vector<u64> bytes_available;
  std::vector<f32> bandwidth;
  std::multimap<u64, size_t> ordered_cap;
  int num_devices;
};
}  // namespace testing
}  // namespace hermes

testing::TargetViewState InitDeviceState() {
  testing::TargetViewState result = {};
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

  result.ordered_cap.insert(std::pair<u64, size_t>(MEGABYTES(5), 0));
  result.ordered_cap.insert(std::pair<u64, size_t>(MEGABYTES(20), 1));
  result.ordered_cap.insert(std::pair<u64, size_t>(MEGABYTES(50), 2));
  result.ordered_cap.insert(std::pair<u64, size_t>(MEGABYTES(200), 3));

  return result;
}

static hermes::testing::TargetViewState NodeViewState {InitDeviceState()};

hermes::testing::TargetViewState GetNodeViewState() {
  testing::TargetViewState result = {};

  result.num_devices = NodeViewState.num_devices;
  for (int i {0}; i < NodeViewState.num_devices; ++i) {
    result.bytes_available.push_back(NodeViewState.bytes_available[i]);
    result.bandwidth.push_back(NodeViewState.bandwidth[i]);
    result.ordered_cap.insert(std::pair<u64, size_t>
                              (NodeViewState.bytes_available[i], i));
  }

  return result;
}

u64 UpdateDeviceState(PlacementSchema schema) {
  u64 result {0};
  NodeViewState.ordered_cap.clear();

  for (auto [size, device] : schema) {
    result += size;
    NodeViewState.bytes_available[device] -= size;
    NodeViewState.ordered_cap.insert(std::pair<u64, size_t>
                              (NodeViewState.bytes_available[device], device));
  }

  return result;
}

int main() {
  std::vector<size_t> blob_sizes1(1, MEGABYTES(10));
  InitDeviceState();

  std::cout << "Device Initial State:\n";
  for (int i {0}; i < NodeViewState.num_devices; ++i) {
    std::cout << "capacity of device[" << i << "]: "
              << NodeViewState.bytes_available[i]
              << '\n' << std::flush;
    std::cout << "available ratio of device["<< i << "]: "
              << static_cast<double>(NodeViewState.bytes_available[i])/
                 NodeViewState.bytes_capacity[i]
              << '\n' << std::flush;
  }
  std::cout << "\nStart to place 10MB blob to targets\n" << std::flush;

  std::vector<PlacementSchema> schemas;
  Status result = MinimizeIoTimePlacement(blob_sizes1,
                                          NodeViewState.bytes_available,
                                          NodeViewState.bandwidth, schemas);
  if (result) {
    std::cout << "\nMinimizeIoTimePlacement failed\n" << std::flush;
    exit(1);
  }

  u64 placed_size {0};
  for (auto schema : schemas) {
    placed_size += UpdateDeviceState(schema);
  }

  std::cout << "\nUpdate Device State:\n";
  for (int i {0}; i < NodeViewState.num_devices; ++i) {
    std::cout << "capacity of device[" << i << "]: "
              << NodeViewState.bytes_available[i]
              << '\n' << std::flush;
    std::cout << "available ratio of device["<< i << "]: "
              << static_cast<double>(NodeViewState.bytes_available[i])/
                 NodeViewState.bytes_capacity[i]
              << '\n' << std::flush;
  }
  Assert(placed_size == MEGABYTES(10));

  std::cout << "\nMinimizeIoTimePlacement to place 1MB blob to targets\n"
            << std::flush;

  std::vector<size_t> blob_sizes2(1, MEGABYTES(1));
  schemas.clear();
  result = MinimizeIoTimePlacement(blob_sizes2, NodeViewState.bytes_available,
                                   NodeViewState.bandwidth, schemas);
  if (result) {
    std::cout << "\n\nSecond MinimizeIoTimePlacement failed\n" << std::flush;
    exit(1);
  }

  placed_size = 0;
  for (auto schema : schemas) {
    placed_size += UpdateDeviceState(schema);
  }

  std::cout << "\nUpdate Device State:\n";
  for (int i {0}; i < NodeViewState.num_devices; ++i) {
    std::cout << "capacity of device[" << i << "]: "
              << NodeViewState.bytes_available[i]
              << '\n' << std::flush;
    std::cout << "available ratio of device["<< i << "]: "
              << static_cast<double>(NodeViewState.bytes_available[i])/
                 NodeViewState.bytes_capacity[i]
              << '\n' << std::flush;
  }
  Assert(placed_size == MEGABYTES(1));

  return 0;
}
