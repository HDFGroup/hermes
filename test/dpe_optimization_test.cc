#include <iostream>
#include <random>
#include <map>

#include "hermes.h"
#include "data_placement_engine.h"
#include "test_utils.h"

using namespace hermes;  // NOLINT(*)

static hermes::testing::TargetViewState node_state {InitDeviceState()};

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

void MinimizeIoTimePlaceBlob(std::vector<size_t> &blob_sizes,
                             std::vector<PlacementSchema> &schemas) {
  std::cout << "\nMinimizeIoTimePlacement to place blob of size "
            << blob_sizes[0] << " to targets\n" << std::flush;
  Status result = MinimizeIoTimePlacement(blob_sizes,
                                          node_state.bytes_available,
                                          node_state.bandwidth, schemas);
  if (result) {
    std::cout << "\nMinimizeIoTimePlacement failed\n" << std::flush;
    exit(1);
  }

  u64 placed_size {0};
  for (auto schema : schemas) {
    placed_size += UpdateDeviceState(schema);
  }

  std::cout << "\nUpdate Device State:\n";
  PrintNodeState(node_state);
  u64 total_sizes = std::accumulate(blob_sizes.begin(), blob_sizes.end(), 0);
  Assert(placed_size == total_sizes);
}

int main() {
  Assert(node_state.num_devices == 4);
  std::cout << "Device Initial State:\n";
  PrintNodeState(node_state);

  std::vector<size_t> blob_sizes1(1, MEGABYTES(10));
  std::vector<PlacementSchema> schemas1;
  MinimizeIoTimePlaceBlob(blob_sizes1, schemas1);
  Assert(schemas1.size() == blob_sizes1.size());

  std::vector<size_t> blob_sizes2(1, MEGABYTES(1));
  std::vector<PlacementSchema> schemas2;
  MinimizeIoTimePlaceBlob(blob_sizes2, schemas2);
  Assert(schemas2.size() == blob_sizes2.size());

  return 0;
}
