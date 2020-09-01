#include "data_placement_engine.h"

#include <assert.h>
#include <math.h>

#include <utility>
#include <random>
#include <map>

#include "ortools/linear_solver/linear_solver.h"

#include "hermes.h"
#include "hermes_types.h"
#include "metadata_management.h"

namespace hermes {

using hermes::api::Status;

// TODO(chogan): Unfinished sketch
Status TopDownPlacement(SharedMemoryContext *context, RpcContext *rpc,
                        std::vector<size_t> blob_sizes,
                        std::vector<PlacementSchema> &output) {
  HERMES_NOT_IMPLEMENTED_YET;

  Status result = 0;
  std::vector<u64> node_state = GetRemainingNodeCapacities(context);

  for (auto &blob_size : blob_sizes) {
    PlacementSchema schema;
    size_t size_left = blob_size;
    DeviceID current_device = 0;

    while (size_left > 0 && current_device < node_state.size()) {
      size_t bytes_used = 0;
      if (node_state[current_device] > size_left) {
        bytes_used = size_left;
      } else {
        bytes_used = node_state[current_device];
        current_device++;
      }

      if (bytes_used) {
        size_left -= bytes_used;
        schema.push_back(std::make_pair(current_device, bytes_used));
      }
    }

    if (size_left > 0) {
      // TODO(chogan): Trigger BufferOrganizer
      // EvictBuffers(eviction_schema);
      schema.clear();
    }

    output.push_back(schema);
  }

  return result;
}

Status RoundRobinPlacement(SharedMemoryContext *context, RpcContext *rpc,
                        std::vector<size_t> blob_sizes,
                        std::vector<PlacementSchema> &output) {
  std::vector<u64> global_state = GetGlobalDeviceCapacities(context, rpc);
  Status result = 0;

  for (size_t i {0}; i < blob_sizes.size(); ++i) {
    std::random_device dev;
    std::mt19937 rng(dev());
    int number {0};
    PlacementSchema schema;

    // If size is greater than 64KB
    // Split the blob or not
    if (blob_sizes[i] > KILOBYTES(64)) {
      std::uniform_int_distribution<std::mt19937::result_type> distribution(0,1);
      number = distribution(rng);
    }

    // Split the blob
    if (number) {
      int split_option {1};
      // Split the blob if size is greater than 64KB
      if (blob_sizes[i] > KILOBYTES(64) && blob_sizes[i] <= KILOBYTES(256))
        split_option = 2;
      else if (blob_sizes[i] > KILOBYTES(256) && blob_sizes[i] <= MEGABYTES(1))
        split_option = 5;
      else if (blob_sizes[i] > MEGABYTES(1) && blob_sizes[i] <= MEGABYTES(4))
        split_option = 8;
      else
        split_option = 10;

      int split_range[] = { 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024 };
      std::vector<int> split_choice(split_range, split_range+split_option-1);

      // Random pickup a number from split_choice to split the blob
      std::uniform_int_distribution<std::mt19937::result_type>
        position(0, split_choice.size()-1);
      int split_num = split_choice[position(rng)];

      // Construct the vector for the splitted blob
      std::vector<size_t> new_blob_size;
      size_t blob_each_portion {blob_sizes[i]/split_num};
      for (int j {0}; j<split_num-1; ++j) {
        new_blob_size.push_back(blob_each_portion);
      }
      new_blob_size.push_back(blob_sizes[i] -
                              blob_each_portion*(split_num-1));

      for (size_t k {0}; k<new_blob_size.size(); ++k) {
        size_t dst {global_state.size()};
        DataPlacementEngine dpe;
        size_t device_pos {dpe.getCountDevice()};
        for (size_t j {0}; j < global_state.size(); ++j) {
          size_t adjust_pos {(j+device_pos)%global_state.size()};
          if (global_state[adjust_pos] >= new_blob_size[k]) {
            dpe.setCountDevice((j+device_pos+1)%global_state.size());
            dst = adjust_pos;
            global_state[adjust_pos] -= new_blob_size[k];
            schema.push_back(std::make_pair(new_blob_size[k], dst));
            break;
          }
        }
        if (dst == global_state.size()) {
          HERMES_NOT_IMPLEMENTED_YET;
          std::cerr << "Buffer device not found for splitted blob! Go to PFS.\n";
        }
      }
      output.push_back(schema);
    }
    // Blob size is less than 64KB or do not split
    else {
      size_t dst {global_state.size()};
      DataPlacementEngine dpe;
      size_t device_pos {dpe.getCountDevice()};
      for (size_t j {0}; j < global_state.size(); ++j) {
        size_t adjust_pos {(j+device_pos)%global_state.size()};
        if (global_state[adjust_pos] >= blob_sizes[i]) {
          dpe.setCountDevice((j+device_pos+1)%global_state.size());
          dst = adjust_pos;
          global_state[adjust_pos] -= blob_sizes[i];
          schema.push_back(std::make_pair(blob_sizes[i], dst));
          output.push_back(schema);
          break;
        }
      }
      if (dst == global_state.size()) {
        HERMES_NOT_IMPLEMENTED_YET;
        std::cerr << "Buffer device not found for the blob! Go to PFS.\n";
      }
    }
  }

  return result;
}

Status RandomPlacement(SharedMemoryContext *context, RpcContext *rpc,
                       std::vector<size_t> &blob_sizes,
                       std::vector<PlacementSchema> &output) {
  // TODO(chogan): For now we just look at the node level. Eventually we will
  // need the ability to escalate to neighborhoods, and the entire cluster.
  std::vector<u64> node_state = GetRemainingNodeCapacities(context);
  std::multimap<u64, size_t> ordered_cap;
  Status result = 0;

  for (size_t i {0}; i < blob_sizes.size(); ++i) {
    std::random_device dev;
    std::mt19937 rng(dev());
    int number {0};
    PlacementSchema schema;

    // If size is greater than 64KB
    // Split the blob or not
    if (blob_sizes[i] > KILOBYTES(64)) {
      std::uniform_int_distribution<std::mt19937::result_type>
        distribution(0, 1);
      number = distribution(rng);
    }

    // Split the blob
    if (number) {
      int split_option {1};
      // Split the blob if size is greater than 64KB
      if (blob_sizes[i] > KILOBYTES(64) && blob_sizes[i] <= KILOBYTES(256))
        split_option = 2;
      else if (blob_sizes[i] > KILOBYTES(256) && blob_sizes[i] <= MEGABYTES(1))
        split_option = 5;
      else if (blob_sizes[i] > MEGABYTES(1) && blob_sizes[i] <= MEGABYTES(4))
        split_option = 8;
      else
        split_option = 10;

      int split_range[] = { 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024 };
      std::vector<int> split_choice(split_range, split_range+split_option-1);

      // Random pickup a number from split_choice to split the blob
      std::uniform_int_distribution<std::mt19937::result_type>
        position(0, split_choice.size()-1);
      int split_num = split_choice[position(rng)];

      // Construct the vector for the splitted blob
      std::vector<size_t> new_blob_size;
      size_t blob_each_portion {blob_sizes[i]/split_num};
      for (int j {0}; j < split_num - 1; ++j) {
        new_blob_size.push_back(blob_each_portion);
      }
      new_blob_size.push_back(blob_sizes[i] -
                              blob_each_portion*(split_num-1));

      for (size_t k {0}; k < new_blob_size.size(); ++k) {
        size_t dst {node_state.size()};
        auto itlow = ordered_cap.lower_bound(new_blob_size[k]);
        if (itlow == ordered_cap.end()) {
          HERMES_NOT_IMPLEMENTED_YET;
          assert(!"No buffer device has enough capacity! Go to PFS.\n");
        }

        std::uniform_int_distribution<std::mt19937::result_type>
          dst_distribution((*itlow).second, node_state.size()-1);
        dst = dst_distribution(rng);
        schema.push_back(std::make_pair(new_blob_size[k], dst));

        for (auto it = itlow; it != ordered_cap.end(); ++it) {
          if ((*it).second == dst) {
            ordered_cap.insert(std::pair<u64, size_t>(
                               (*it).first-new_blob_size[k], (*it).second));
            ordered_cap.erase(it);
            break;
          }
        }
      }
      output.push_back(schema);
    } else {
      // Blob size is less than 64KB or do not split
      PlacementSchema schema;
      size_t dst {node_state.size()};
      auto itlow = ordered_cap.lower_bound(blob_sizes[i]);
      if (itlow == ordered_cap.end()) {
        HERMES_NOT_IMPLEMENTED_YET;
        assert(!"No buffer device has enough capacity! Go to PFS.\n");
      }

      std::uniform_int_distribution<std::mt19937::result_type>
        dst_distribution((*itlow).second, node_state.size()-1);
      dst = dst_distribution(rng);
      for (auto it = itlow; it != ordered_cap.end(); ++it) {
        if ((*it).second == dst) {
          ordered_cap.insert(std::pair<u64, size_t>(
                             (*it).first-blob_sizes[i], (*it).second));
          ordered_cap.erase(it);
          break;
        }
      }
      schema.push_back(std::make_pair(blob_sizes[i], dst));
      output.push_back(schema);
    }
  }

  return result;
}

Status MinimizeIoTimePlacement(SharedMemoryContext *context, RpcContext *rpc,
                            std::vector<size_t> &blob_sizes,
                            std::vector<PlacementSchema> &output) {
  using operations_research::MPSolver;
  using operations_research::MPVariable;
  using operations_research::MPConstraint;
  using operations_research::MPObjective;

  Status result = 0;
  // TODO(chogan): For now we just look at the node level targets. Eventually we
  // will need the ability to escalate to neighborhoods, and the entire cluster.
  std::vector<u64> node_state = GetRemainingNodeCapacities(context);
  std::vector<f32> bandwidths = GetBandwidths(context);
  // TODO(KIMMY): size of constraints should be from context
  std::vector<MPConstraint*> blob_constrt(blob_sizes.size() +
                                          node_state.size()*3-1);
  std::vector<std::vector<MPVariable*>> blob_fraction(blob_sizes.size());
  MPSolver solver("LinearOpt", MPSolver::GLOP_LINEAR_PROGRAMMING);
  int num_constrts {0};

  // Sum of fraction of each blob is 1
  for (size_t i {0}; i < blob_sizes.size(); ++i) {
    blob_constrt[num_constrts+i] = solver.MakeRowConstraint(1, 1);
    blob_fraction[i].resize(node_state.size());

    // TODO(KIMMY): consider remote nodes?
    for (size_t j {0}; j < node_state.size(); ++j) {
      std::string var_name {"blob_dst_" + std::to_string(i) + "_" +
                            std::to_string(j)};
      blob_fraction[i][j] = solver.MakeNumVar(0.0, 1, var_name);
      blob_constrt[num_constrts+i]->SetCoefficient(blob_fraction[i][j], 1);
    }
  }

  // Minimum Remaining Capacity Constraint
  num_constrts += blob_sizes.size();
  for (size_t j {0}; j < node_state.size(); ++j) {
    blob_constrt[num_constrts+j] = solver.MakeRowConstraint(
      0, (static_cast<double>(node_state[j])-
      0.1*static_cast<double>(node_state[j])));
    for (size_t i {0}; i < blob_sizes.size(); ++i) {
      blob_constrt[num_constrts+j]->SetCoefficient(
        blob_fraction[i][j], static_cast<double>(blob_sizes[i]));
    }
  }

  // Remaining Capacity Change Threshold 20%
  num_constrts += node_state.size();
  for (size_t j {0}; j < node_state.size(); ++j) {
    blob_constrt[num_constrts+j] =
      solver.MakeRowConstraint(0, 0.2*node_state[j]);
    for (size_t i {0}; i < blob_sizes.size(); ++i) {
      blob_constrt[num_constrts+j]->SetCoefficient(
        blob_fraction[i][j], static_cast<double>(blob_sizes[i]));
    }
  }

  // Placement Ratio
  num_constrts += node_state.size();
  for (size_t j {0}; j < node_state.size()-1; ++j) {
    blob_constrt[num_constrts+j] =
      solver.MakeRowConstraint(0, solver.infinity());
    for (size_t i {0}; i < blob_sizes.size(); ++i) {
      blob_constrt[num_constrts+j]->SetCoefficient(
        blob_fraction[i][j+1], static_cast<double>(blob_sizes[i]));
      double placement_ratio = static_cast<double>(node_state[j+1])/
                                                   node_state[j];
      blob_constrt[num_constrts+j]->SetCoefficient(
        blob_fraction[i][j],
        static_cast<double>(blob_sizes[i])*(0-placement_ratio));
    }
  }

  // Objective to minimize IO time
  MPObjective* const objective = solver.MutableObjective();
  for (size_t i {0}; i < blob_sizes.size(); ++i) {
    for (size_t j {0}; j < node_state.size(); ++j) {
      objective->SetCoefficient(blob_fraction[i][j],
        static_cast<double>(blob_sizes[i])/bandwidths[j]);
    }
  }
  objective->SetMinimization();

  const MPSolver::ResultStatus result_status = solver.Solve();
  // Check if the problem has an optimal solution.
  if (result_status != MPSolver::OPTIMAL) {
    std::cerr << "The problem does not have an optimal solution!\n";
  }

  for (size_t i {0}; i < blob_sizes.size(); ++i) {
    PlacementSchema schema;
    size_t device_pos {0};  // to track the device with most data
    auto largest_bulk{blob_fraction[i][0]->solution_value()*blob_sizes[i]};
    // NOTE: could be inefficient if there are hundreds of devices
    for (size_t j {1}; j < node_state.size(); ++j) {
      if (blob_fraction[i][j]->solution_value()*blob_sizes[i] > largest_bulk)
        device_pos = j;
    }
    size_t blob_partial_sum {0};
    for (size_t j {0}; j < node_state.size(); ++j) {
      if (j == device_pos)
        continue;
      double check_frac_size {blob_fraction[i][j]->solution_value()*
                              blob_sizes[i]};  // blob fraction size
      size_t frac_size_cast = static_cast<size_t>(check_frac_size);
      // If size to this destination is not 0, push to result
      if (frac_size_cast != 0) {
        schema.push_back(std::make_pair(frac_size_cast, j));
        blob_partial_sum += frac_size_cast;
      }
    }
    // Push the rest data to device device_pos
    schema.push_back(std::make_pair(blob_sizes[i]-blob_partial_sum,
                                    device_pos));
    output.push_back(schema);
  }

  return result;
}

Status CalculatePlacement(SharedMemoryContext *context, RpcContext *rpc,
                          std::vector<size_t> &blob_sizes,
                          std::vector<PlacementSchema> &output,
                          const api::Context &api_context) {
  (void)api_context;
  Status result = 0;

  // TODO(chogan): Return a PlacementSchema that minimizes a cost function F
  // given a set of N Devices and a blob, while satisfying a policy P.

  switch (api_context.policy) {
    // TODO(KIMMY): check device capacity against blob size
    case api::PlacementPolicy::kRandom: {
      result = RandomPlacement(context, rpc, blob_sizes, output);
      break;
    }
    case api::PlacementPolicy::kRoundRobin: {
      result = RoundRobinPlacement(context, rpc, blob_sizes, output);
      break;
    }
    case api::PlacementPolicy::kTopDown: {
      result = TopDownPlacement(context, rpc, blob_sizes, output);
      break;
    }
    case api::PlacementPolicy::kMinimizeIoTime: {
      result = MinimizeIoTimePlacement(context, rpc, blob_sizes, output);
      break;
    }
  }

  return result;
}

}  // namespace hermes
