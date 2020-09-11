#include <iostream>
#include <random>
#include <map>

#include "ortools/linear_solver/linear_solver.h"

#include "hermes.h"

using namespace hermes;

namespace hermes {
namespace testing {
struct SystemViewState {
  u64 bytes_capacity[kMaxDevices];
  u64 bytes_available[kMaxDevices];
  u64 bandwidth[kMaxDevices];
  int num_devices;
};
}  // namespace hermes
}  // namespace testing

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

PlacementSchema RandomPlacement(std::vector<hermes::api::Blob> blobs) {
  PlacementSchema result;
  // TODO (KIMMY): use kernel function of system view
  testing::SystemViewState state {GetSystemViewState()};
  std::multimap<u64, int> ordered_cap;

  for (int j {0}; j < state.num_devices; ++j) {
    ordered_cap.insert(std::pair<u64, int>(state.bytes_available[j], j));
  }

  for (size_t i {0}; i < blobs.size(); ++i) {
    // Split the blob or not
    std::random_device dev;
    std::mt19937 rng(dev());
    int number {0};
    
    // If size is greater than 64KB
    // Decision about split the blob or not
    if (blobs[i].size() > KILOBYTES(64)) {
      std::uniform_int_distribution<std::mt19937::result_type> distribution(0,1);
      number = distribution(rng);
    }

    // Split the blob
    if (number) {
      int split_option {1};
      std::cout << "blob size is " << blobs[i].size() << '\n' << std::flush;
      // Not split the blob if size is less than 64KB
      if (blobs[i].size() > KILOBYTES(64) && blobs[i].size() <= KILOBYTES(256))
        split_option = 2;
      else if (blobs[i].size() > KILOBYTES(256) && blobs[i].size() <= MEGABYTES(1))
        split_option = 5;
      else if (blobs[i].size() > MEGABYTES(1) && blobs[i].size() <= MEGABYTES(4))
        split_option = 8;
      else
        split_option = 10;

      int split_range[] = { 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024 };
      std::vector<int> split_choice(split_range, split_range+split_option-1);

     // Random pickup a number from split_choice to split the blob
     std::uniform_int_distribution<std::mt19937::result_type>
       position(0, split_choice.size()-1);
     int split_num = split_choice[position(rng)];
     std::cout << "split blob into " << split_num << '\n' << std::flush;

     // Construct the vector for the splitted blob
     std::vector<size_t> new_blob_size;
     size_t blob_each_portion {blobs[i].size()/split_num};
     for (int j {0}; j<split_num-1; ++j) {
       new_blob_size.push_back(blob_each_portion);
     }
     new_blob_size.push_back(blobs[i].size() -
                             blob_each_portion*(split_num-1));

     for (size_t k {0}; k<new_blob_size.size(); ++k) {
       int dst {state.num_devices};
       auto itlow = ordered_cap.lower_bound (new_blob_size[k]);
       if (itlow == ordered_cap.end()) {
         std::cerr << "No target has enough capacity (max "
                   << ordered_cap.rbegin()->first
                   << ") for the blob with size " << new_blob_size[k] << ")\n"
                   << std::flush;
       }

       std::uniform_int_distribution<std::mt19937::result_type>
         dst_distribution((*itlow).second, state.num_devices-1);
       dst = dst_distribution(rng);
       result.push_back(std::make_pair(new_blob_size[k], dst));
       for (auto it=itlow; it!=ordered_cap.end(); ++it) {
         if ((*it).second == dst) {
           ordered_cap.insert(std::pair<size_t, size_t>(
                              (*it).first-new_blob_size[k], (*it).second));
           ordered_cap.erase(it);
           break;
         }
       }
     }
   }
   // Blob size is less than 64KB or do not split
   else {
     std::cout << "blob size is " << blobs[i].size() << '\n' << std::flush;
     int dst {state.num_devices};
     auto itlow = ordered_cap.lower_bound (blobs[i].size());
     if (itlow == ordered_cap.end()) {
       std::cerr << "No target has enough capacity (max "
                 << ordered_cap.rbegin()->first << " for the blob with size "
                 << blobs[i].size() << '\n' << std::flush;
     }

     std::uniform_int_distribution<std::mt19937::result_type>
       dst_distribution((*itlow).second, state.num_devices-1);
     dst = dst_distribution(rng);
     for (auto it=itlow; it!=ordered_cap.end(); ++it) {
       if ((*it).second == dst) {
         ordered_cap.insert(std::pair<size_t, size_t>(
                            (*it).first-blobs[i].size(), (*it).second));
         ordered_cap.erase(it);
         break;
       }
     }
     result.push_back(std::make_pair(blobs[i].size(), dst));
    }
  }

  return result;
}

int main()
{
  hermes::api::Blob p1 (1024*1024, 255);
  hermes::api::Blob p2 (1024*1024*2, 255);
  hermes::api::Blob p3 (1024*1024*4, 255);
  hermes::api::Blob p4 (1024*1024*7, 255);
  std::vector<hermes::api::Blob> input_blobs;
  input_blobs.push_back(p1);
  input_blobs.push_back(p2);
  input_blobs.push_back(p3);
  input_blobs.push_back(p4);

  InitSystemViewState();
  
  for(int i {0}; i < globalSystemViewState.num_devices; ++i) {
    std::cout << "device[" << i << "]: " << globalSystemViewState.bytes_available[i]
              << '\n' << std::flush;
    std::cout << "available ratio["<< i << "]: "
              << static_cast<double>(globalSystemViewState.bytes_available[i])/
                 globalSystemViewState.bytes_capacity[i]
              << '\n' << std::flush;
  }
  std::cout << '\n' << '\n' << std::flush;

  PlacementSchema schema1 = RandomPlacement(input_blobs);

  UpdateSystemViewState(schema1);
  for(int i {0}; i < globalSystemViewState.num_devices; ++i) {
    std::cout << "device[" << i << "]: " << globalSystemViewState.bytes_available[i]
              << '\n' << std::flush;
    std::cout << "available ratio["<< i << "]: "
              << static_cast<double>(globalSystemViewState.bytes_available[i])/
                 globalSystemViewState.bytes_capacity[i]
              << '\n' << std::flush;
  }
  std::cout << '\n' << '\n' << std::flush;

  hermes::api::Blob p5 (1024*1024*0.5, 255);
  hermes::api::Blob p6 (1024*1024*20, 255);
  hermes::api::Blob p7 (1024*1024*4, 255);
  input_blobs.clear();
  input_blobs.push_back(p5);
  input_blobs.push_back(p6);
  input_blobs.push_back(p7);
  PlacementSchema schema2 = RandomPlacement(input_blobs);

  UpdateSystemViewState(schema2);
  for(int i {0}; i < globalSystemViewState.num_devices; ++i) {
    std::cout << "device[" << i << "]: " << globalSystemViewState.bytes_available[i]
              << '\n' << std::flush;
    std::cout << "available ratio["<< i << "]: "
              << static_cast<double>(globalSystemViewState.bytes_available[i])/
                 globalSystemViewState.bytes_capacity[i]
              << '\n' << std::flush;
  }
  std::cout << '\n' << '\n' << std::flush;

  hermes::api::Blob p8 (1024*1024*0.5, 255);
  hermes::api::Blob p9 (1024*1024*20, 255);
  hermes::api::Blob p10 (1024*1024*1, 255);
  hermes::api::Blob p11 (1024*1024*4, 255);
  hermes::api::Blob p12 (1024*1024*10, 255);
  input_blobs.clear();
  input_blobs.push_back(p8);
  input_blobs.push_back(p9);
  input_blobs.push_back(p10);
  input_blobs.push_back(p11);
  input_blobs.push_back(p12);
  PlacementSchema schema3 = RandomPlacement(input_blobs);

  UpdateSystemViewState(schema3);
  for(int i {0}; i < globalSystemViewState.num_devices; ++i) {
    std::cout << "device[" << i << "]: " << globalSystemViewState.bytes_available[i]
              << '\n' << std::flush;
    std::cout << "available ratio["<< i << "]: "
              << static_cast<double>(globalSystemViewState.bytes_available[i])/
                 globalSystemViewState.bytes_capacity[i]
              << '\n' << std::flush;
  }
  std::cout << '\n' << '\n' << std::flush;

  return 0;
}
