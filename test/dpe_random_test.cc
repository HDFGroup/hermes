#include <iostream>
#include <random>
#include <map>

#include "ortools/linear_solver/linear_solver.h"

#include "hermes.h"
#include "data_placement_engine.h"

using namespace hermes;  // NOLINT(*)
namespace hapi = hermes::api;

namespace hermes {
namespace testing {
struct SystemViewState {
  u64 bytes_capacity[kMaxDevices];
  u64 bytes_available[kMaxDevices];
  u64 bandwidth[kMaxDevices];
  int num_devices;
};
}  // namespace testing
}  // namespace hermes

testing::SystemViewState InitSystemViewState() {
  testing::SystemViewState result = {};
  result.num_devices = 4;
  u64 one_mb = 1024 * 1024;

  result.bytes_available[0] = 5 * one_mb;
  result.bytes_available[1] = 20 * one_mb;
  result.bytes_available[2] = 50 * one_mb;
  result.bytes_available[3] = 200 * one_mb;

  result.bytes_capacity[0] = 5 * one_mb;
  result.bytes_capacity[1] = 20 * one_mb;
  result.bytes_capacity[2] = 50 * one_mb;
  result.bytes_capacity[3] = 200 * one_mb;

  result.bandwidth[0] = 6000;
  result.bandwidth[1] = 300;
  result.bandwidth[2] = 150;
  result.bandwidth[3] = 70;

  return result;
}

static testing::SystemViewState globalSystemViewState {InitSystemViewState()};

testing::SystemViewState GetSystemViewState() {
  testing::SystemViewState result = {};

  for (int i {0}; i < globalSystemViewState.num_devices; ++i) {
    result.num_devices = globalSystemViewState.num_devices;
    result.bytes_available[i] = globalSystemViewState.bytes_available[i];
    result.bandwidth[i] = globalSystemViewState.bandwidth[i];
  }

  return result;
}

void UpdateSystemViewState(PlacementSchema schema) {
  for (auto [size, device] : schema) {
    globalSystemViewState.bytes_available[device] -= size;
  }
}

std::vector<PlacementSchema> RandomPlacement(std::vector<hapi::Blob> blobs) {
  std::vector<PlacementSchema> result;
  // TODO(KIMMY): use kernel function of system view
  testing::SystemViewState state {GetSystemViewState()};
  std::vector<u64> node_state(state.num_devices);
  std::multimap<u64, size_t> ordered_cap;


  for (int j {0}; j < state.num_devices; ++j) {
    ordered_cap.insert(std::pair<u64, size_t>(state.bytes_available[j], j));
    node_state[j] = state.bytes_available[j];
  }

  for (size_t i {0}; i < blobs.size(); ++i) {
    // Split the blob or not
    std::random_device dev;
    std::mt19937 rng(dev());
    int number {0};

    // If size is greater than 64KB
    // Decision about split the blob or not
    if (blobs[i].size() > KILOBYTES(64)) {
      std::uniform_int_distribution<std::mt19937::result_type>
        distribution(0, 1);
      number = distribution(rng);
    }

    // Split the blob
    if (number) {
      std::cout << "blob size is " << blobs[i].size() << '\n' << std::flush;
      std::vector<int> split_choice = GetValidSplitChoices(blobs[i].size());

      // Random pickup a number from split_choice to split the blob
      std::uniform_int_distribution<std::mt19937::result_type>
        position(0, split_choice.size()-1);
      int split_num = split_choice[position(rng)];
      std::cout << "split blob into " << split_num << '\n' << std::flush;

      // Construct the vector for the splitted blob
      std::vector<size_t> new_blob_size;
      size_t blob_each_portion {blobs[i].size()/split_num};
      for (int j {0}; j < split_num - 1; ++j) {
        new_blob_size.push_back(blob_each_portion);
      }
      new_blob_size.push_back(blobs[i].size() -
                              blob_each_portion*(split_num-1));

      for (size_t k {0}; k < new_blob_size.size(); ++k) {
        AddRandomSchema(ordered_cap, new_blob_size[k], result, node_state);
      }
    } else {
      // Blob size is less than 64KB or do not split
      std::cout << "blob size is " << blobs[i].size() << '\n' << std::flush;
      AddRandomSchema(ordered_cap, blobs[i].size(), result, node_state);
    }
  }

  return result;
}

int main() {
  hermes::api::Blob p1(1024*1024, 255);
  hermes::api::Blob p2(1024*1024*2, 255);
  hermes::api::Blob p3(1024*1024*4, 255);
  hermes::api::Blob p4(1024*1024*7, 255);
  std::vector<hermes::api::Blob> input_blobs;
  input_blobs.push_back(p1);
  input_blobs.push_back(p2);
  input_blobs.push_back(p3);
  input_blobs.push_back(p4);

  InitSystemViewState();

  for (int i {0}; i < globalSystemViewState.num_devices; ++i) {
    std::cout << "device[" << i << "]: "
              << globalSystemViewState.bytes_available[i]
              << '\n' << std::flush;
    std::cout << "available ratio["<< i << "]: "
              << static_cast<double>(globalSystemViewState.bytes_available[i])/
                 globalSystemViewState.bytes_capacity[i]
              << '\n' << std::flush;
  }
  std::cout << '\n' << '\n' << std::flush;

  std::vector<PlacementSchema> schemas1 = RandomPlacement(input_blobs);
  for (auto schema : schemas1) {
    UpdateSystemViewState(schema);
  }

  for (int i {0}; i < globalSystemViewState.num_devices; ++i) {
    std::cout << "device[" << i << "]: "
              << globalSystemViewState.bytes_available[i]
              << '\n' << std::flush;
    std::cout << "available ratio["<< i << "]: "
              << static_cast<double>(globalSystemViewState.bytes_available[i])/
                 globalSystemViewState.bytes_capacity[i]
              << '\n' << std::flush;
  }
  std::cout << '\n' << '\n' << std::flush;

  hermes::api::Blob p5(1024*1024*0.5, 255);
  hermes::api::Blob p6(1024*1024*20, 255);
  hermes::api::Blob p7(1024*1024*4, 255);
  input_blobs.clear();
  input_blobs.push_back(p5);
  input_blobs.push_back(p6);
  input_blobs.push_back(p7);
  std::vector<PlacementSchema> schemas2 = RandomPlacement(input_blobs);
  for (auto schema : schemas2) {
    UpdateSystemViewState(schema);
  }

  for (int i {0}; i < globalSystemViewState.num_devices; ++i) {
    std::cout << "device[" << i << "]: "
              << globalSystemViewState.bytes_available[i]
              << '\n' << std::flush;
    std::cout << "available ratio["<< i << "]: "
              << static_cast<double>(globalSystemViewState.bytes_available[i])/
                 globalSystemViewState.bytes_capacity[i]
              << '\n' << std::flush;
  }
  std::cout << '\n' << '\n' << std::flush;

  hermes::api::Blob p8(1024*1024*0.5, 255);
  hermes::api::Blob p9(1024*1024*20, 255);
  hermes::api::Blob p10(1024*1024*1, 255);
  hermes::api::Blob p11(1024*1024*4, 255);
  hermes::api::Blob p12(1024*1024*10, 255);
  input_blobs.clear();
  input_blobs.push_back(p8);
  input_blobs.push_back(p9);
  input_blobs.push_back(p10);
  input_blobs.push_back(p11);
  input_blobs.push_back(p12);
  std::vector<PlacementSchema> schemas3 = RandomPlacement(input_blobs);
  for (auto schema : schemas3) {
    UpdateSystemViewState(schema);
  }

  for (int i {0}; i < globalSystemViewState.num_devices; ++i) {
    std::cout << "device[" << i << "]: "
              << globalSystemViewState.bytes_available[i]
              << '\n' << std::flush;
    std::cout << "available ratio["<< i << "]: "
              << static_cast<double>(globalSystemViewState.bytes_available[i])/
                 globalSystemViewState.bytes_capacity[i]
              << '\n' << std::flush;
  }
  std::cout << '\n' << '\n' << std::flush;

  return 0;
}
