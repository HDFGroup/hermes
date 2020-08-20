#include <iostream>

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

PlacementSchema PerfOrientedPlacement(std::vector<hermes::api::Blob> blobs) {
  using operations_research::MPSolver;
  using operations_research::MPVariable;
  using operations_research::MPConstraint;
  using operations_research::MPObjective;

  PlacementSchema result;
  // TODO (KIMMY): use kernel function of system view
  testing::SystemViewState state {GetSystemViewState()};

  std::vector<MPConstraint*> blob_constrt(blobs.size()+state.num_devices*3-1);
  std::vector<std::vector<MPVariable*>> blob_fraction (blobs.size());
  MPSolver solver("LinearOpt", MPSolver::GLOP_LINEAR_PROGRAMMING);
  int num_constrts {0};
  // TODO (KIMMY): placement ratio number will be from policy in the future
  const int placement_ratio {-10};

  u64 avail_cap {0};
  // Apply Remaining Capacity Change Threshold 20%
  for (int j {0}; j < state.num_devices; ++j) {
    avail_cap += static_cast<u64>(state.bytes_available[j]*0.2);
  }

  u64 total_blob_size {0};
  for (size_t i {0}; i < blobs.size(); ++i) {
    total_blob_size += blobs[i].size();
  }

  if(total_blob_size > avail_cap) {
    // TODO (KIMMY): @errorhandling 
    assert(!"Available capacity is not enough for data placement\n");
  }

  // Sum of fraction of each blob is 1
  for (size_t i {0}; i < blobs.size(); ++i) {
    blob_constrt[num_constrts+i] = solver.MakeRowConstraint(1, 1);
    blob_fraction[i].resize(state.num_devices);

    // TODO (KIMMY): consider remote nodes?
    for (int j {0}; j < state.num_devices; ++j) {
      std::string var_name {"blob_dst_" + std::to_string(i) + "_" +
                            std::to_string(j)};
      blob_fraction[i][j] = solver.MakeNumVar(0.0, 1, var_name);
      blob_constrt[num_constrts+i]->SetCoefficient(blob_fraction[i][j], 1);
    }
  }

  // Minimum Remaining Capacity Constraint
  num_constrts += blobs.size();
  for (int j {0}; j < state.num_devices; ++j) {
    blob_constrt[num_constrts+j] = solver.MakeRowConstraint(
      0, (static_cast<double>(state.bytes_available[j])-
      0.1*static_cast<double>(state.bytes_capacity[j])));
    for (size_t i {0}; i < blobs.size(); ++i) {
      blob_constrt[num_constrts+j]->SetCoefficient(
        blob_fraction[i][j], static_cast<double>(blobs[i].size()));
    }
  }

  // Remaining Capacity Change Threshold 20%
  num_constrts += state.num_devices;
  for (int j {0}; j < state.num_devices; ++j) {
    blob_constrt[num_constrts+j] =
      solver.MakeRowConstraint(0, 0.2*state.bytes_available[j]);
    for (size_t i {0}; i < blobs.size(); ++i) {
      blob_constrt[num_constrts+j]->SetCoefficient(
        blob_fraction[i][j], static_cast<double>(blobs[i].size()));
    }
  }

  // Placement Ratio
  num_constrts += state.num_devices;
  for (int j {0}; j < state.num_devices-1; ++j) {
    blob_constrt[num_constrts+j] =
      solver.MakeRowConstraint(0, solver.infinity());
    for (size_t i {0}; i < blobs.size(); ++i) {
      blob_constrt[num_constrts+j]->SetCoefficient(
        blob_fraction[i][j+1], static_cast<double>(blobs[i].size()));
      blob_constrt[num_constrts+j]->SetCoefficient(
        blob_fraction[i][j], 
        static_cast<double>(blobs[i].size())*placement_ratio);
    }
  }

  // Objective to minimize IO time
  MPObjective* const objective = solver.MutableObjective();
  for (size_t i {0}; i < blobs.size(); ++i) {
    for (int j {0}; j < state.num_devices; ++j) {
      objective->SetCoefficient(blob_fraction[i][j],
        static_cast<double>(blobs[i].size())/state.bandwidth[j]);
    }
  }
  objective->SetMinimization();

  const MPSolver::ResultStatus result_status = solver.Solve();
  // Check if the problem has an optimal solution.
  if (result_status != MPSolver::OPTIMAL) {
    std::cerr << "The problem does not have an optimal solution!\n";
  }

  for (size_t i {0}; i < blobs.size(); ++i) {
    int device_pos {0}; // to track the device with most data
    auto largest_bulk{blob_fraction[i][0]->solution_value()*blobs[i].size()};
    // NOTE: could be inefficient if there are hundreds of devices
    for (int j {1}; j < state.num_devices; ++j) {
      if (blob_fraction[i][j]->solution_value()*blobs[i].size() > largest_bulk)
        device_pos = j;
    }
    size_t blob_partial_sum {0};
    for (int j {0}; j < state.num_devices; ++j) {
      if (j == device_pos)
        continue;
      double check_frac_size {blob_fraction[i][j]->solution_value()*
                              blobs[i].size()}; // blob fraction size
      size_t frac_size_cast = static_cast<size_t>(check_frac_size);
      // If size to this destination is not 0, push to result
      if (frac_size_cast != 0) {
        result.push_back(std::make_pair(frac_size_cast, j));
        blob_partial_sum += frac_size_cast;
      }
    }
    // Push the rest data to device_pos
    result.push_back(std::make_pair(blobs[i].size()-blob_partial_sum, device_pos));
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

  PlacementSchema schema1 = PerfOrientedPlacement(input_blobs);

  UpdateSystemViewState(schema1);

  for (auto [size, device] : schema1) {
    std::cout << "placing " << size << " at device " << device << '\n' << std::flush;
  }
  for(int i {0}; i < globalSystemViewState.num_devices; ++i) {
    std::cout << "device[" << i << "]: " << globalSystemViewState.bytes_available[i]
              << '\n' << std::flush;
    std::cout << "available ratio["<< i << "]: "
              << static_cast<double>(globalSystemViewState.bytes_available[i])/
                 globalSystemViewState.bytes_capacity[i]
              << '\n' << std::flush;
  }
  std::cout << '\n' << '\n' << '\n'<< std::flush;

  hermes::api::Blob p5 (1024*1024*0.5, 255);
  hermes::api::Blob p6 (1024*1024*20, 255);
  hermes::api::Blob p7 (1024*1024*4, 255);
  input_blobs.clear();
  input_blobs.push_back(p5);
  input_blobs.push_back(p6);
  input_blobs.push_back(p7);
  PlacementSchema schema2 = PerfOrientedPlacement(input_blobs);

  UpdateSystemViewState(schema2);

  for (auto [size, device] : schema2) {
    std::cout << "placing " << size << " at device " << device << '\n' << std::flush;
  }
  for(int i {0}; i < globalSystemViewState.num_devices; ++i) {
    std::cout << "device[" << i << "]: " << globalSystemViewState.bytes_available[i]
              << '\n' << std::flush;
    std::cout << "available ratio["<< i << "]: "
              << static_cast<double>(globalSystemViewState.bytes_available[i])/
                 globalSystemViewState.bytes_capacity[i]
              << '\n' << std::flush;
  }
  std::cout << '\n' << '\n' << '\n'<< std::flush;

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
  PlacementSchema schema3 = PerfOrientedPlacement(input_blobs);

  UpdateSystemViewState(schema3);

  for (auto [size, device] : schema3) {
    std::cout << "placing " << size << " at device " << device << '\n' << std::flush;
  }
  for(int i {0}; i < globalSystemViewState.num_devices; ++i) {
    std::cout << "device[" << i << "]: " << globalSystemViewState.bytes_available[i]
              << '\n' << std::flush;
    std::cout << "available ratio["<< i << "]: "
              << static_cast<double>(globalSystemViewState.bytes_available[i])/
                 globalSystemViewState.bytes_capacity[i]
              << '\n' << std::flush;
  }
  std::cout << '\n' << '\n' << '\n'<< std::flush;

  return 0;
}
