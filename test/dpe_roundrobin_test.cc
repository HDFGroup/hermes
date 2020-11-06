#include <iostream>
#include <random>
#include <map>

#include "hermes.h"
#include "data_placement_engine.h"
#include "test_utils.h"

using namespace hermes;  // NOLINT(*)

static testing::TargetViewState node_state {InitDeviceState()};

u64 UpdateDeviceState(PlacementSchema schema) {
  u64 result {0};
  node_state.ordered_cap.clear();

  for (auto [size, device] : schema) {
    result += size;
    node_state.bytes_available[device] -= size;
    node_state.ordered_cap.insert(std::pair<u64, size_t>
                              (node_state.bytes_available[device], device));
  }

  return result;
}

void PrintNodeState(testing::TargetViewState &node_state) {
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

int main() {
  std::vector<size_t> blob_sizes1(1, MEGABYTES(10));

  Assert(node_state.num_devices==4);
  std::cout << "Device Initial State:\n";
  PrintNodeState(node_state);
  std::cout << "\nStart to place 10MB blob to targets\n" << std::flush;

  std::vector<PlacementSchema> schemas;
  Status result = RoundRobinPlacement(blob_sizes1,
                                      node_state.bytes_available,
                                      schemas);
  if (result) {
    std::cout << "\nFirst RoundRobinPlacement failed\n" << std::flush;
    exit(1);
  }

  u64 placed_size {0};
  for (auto schema : schemas) {
    placed_size += UpdateDeviceState(schema);
  }

  std::cout << "\nUpdate Device State:\n";
  PrintNodeState(node_state);
  Assert(placed_size == MEGABYTES(10));

  std::cout << "\nStart to place 1MB blob to targets\n" << std::flush;

  std::vector<size_t> blob_sizes2(1, MEGABYTES(1));
  schemas.clear();
  result = RoundRobinPlacement(blob_sizes2, node_state.bytes_available,
                               schemas);
  if (result) {
    std::cout << "\n\nSecond RoundRobinPlacement failed\n" << std::flush;
    exit(1);
  }

  placed_size = 0;
  for (auto schema : schemas) {
    placed_size += UpdateDeviceState(schema);
  }

  std::cout << "\nUpdate Device State:\n";
  PrintNodeState(node_state);
  Assert(placed_size == MEGABYTES(1));

  return 0;
}
