#include <iostream>
#include <random>
#include <map>

#include "hermes.h"
#include "data_placement_engine.h"
#include "test_utils.h"

using namespace hermes;  // NOLINT(*)

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
  Status result = RandomPlacement(blob_sizes1, NodeViewState.ordered_cap,
                                  schemas);
  if (result) {
    std::cout << "\nFirst RandomPlacement failed\n" << std::flush;
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

  std::cout << "\nStart to place 1MB blob to targets\n" << std::flush;

  std::vector<size_t> blob_sizes2(1, MEGABYTES(1));
  schemas.clear();
  result = RandomPlacement(blob_sizes2, NodeViewState.ordered_cap, schemas);
  if (result) {
    std::cout << "\n\nSecond RandomPlacement failed\n" << std::flush;
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
