/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Distributed under BSD 3-Clause license.                                   *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Illinois Institute of Technology.                        *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Hermes. The full Hermes copyright notice, including  *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the top directory. If you do not  *
 * have access to the file, you may request a copy from help@hdfgroup.org.   *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

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
#include "metadata_management_internal.h"
#include "metadata_storage.h"

namespace hermes {

using hermes::api::Status;

RoundRobinState::RoundRobinState() : device_index_mutex_() {
  device_index_mutex_.lock();
}

RoundRobinState::~RoundRobinState() {
  device_index_mutex_.unlock();
}

size_t RoundRobinState::GetNumDevices() const {
  return devices_.size();
}

DeviceID RoundRobinState::GetDeviceByIndex(int i) const {
  return devices_[i];
}

int RoundRobinState::GetCurrentDeviceIndex() const {
  return current_device_index_;
}

void RoundRobinState::SetCurrentDeviceIndex(int new_device_index) {
  current_device_index_ = new_device_index;
}

std::vector<int> GetValidSplitChoices(size_t blob_size) {
  int split_option = 10;
  // Split the blob if size is greater than 64KB
  if (blob_size > KILOBYTES(64) && blob_size <= KILOBYTES(256)) {
    split_option = 2;
  } else if (blob_size > KILOBYTES(256) && blob_size <= MEGABYTES(1)) {
    split_option = 5;
  } else if (blob_size > MEGABYTES(1) && blob_size <= MEGABYTES(4)) {
    split_option = 8;
  }

  constexpr int split_range[] = { 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024 };
  std::vector<int> result(split_range, split_range + split_option - 1);

  return result;
}

Status AddRoundRobinSchema(size_t index, std::vector<u64> &node_state,
                           const std::vector<size_t> &blob_sizes,
                           const std::vector<TargetID> &targets,
                           PlacementSchema &output) {
  Status result;
  TargetID dst = {};
  RoundRobinState dpe;
  size_t num_targets = targets.size();
  int current_device_index {dpe.GetCurrentDeviceIndex()};
  size_t num_devices = dpe.GetNumDevices();

  // NOTE(chogan): Starting with current_device, loop through all devices until
  // we either 1) find a matching Target or 2) end up back at the starting
  // device.
  for (size_t i = 0; i < num_devices; ++i) {
    int next_index = (current_device_index + i) % num_devices;
    DeviceID dev_id = dpe.GetDeviceByIndex(next_index);

    for (size_t j = 0; j < num_targets; ++j) {
      if (node_state[j] >= blob_sizes[index]) {
        if (targets[j].bits.device_id == dev_id) {
          dst = targets[j];
          output.push_back(std::make_pair(blob_sizes[index], dst));
          node_state[j] -= blob_sizes[index];
          dpe.SetCurrentDeviceIndex((next_index + 1) % num_devices);
          break;
        }
      }
    }

    if (!IsNullTargetId(dst)) {
      break;
    }
  }

  if (IsNullTargetId(dst)) {
    result = DPE_RR_FIND_TGT_FAILED;
    LOG(ERROR) << result.Msg();
  }

  return result;
}

bool SplitBlob(size_t blob_size) {
  bool result = false;
  std::random_device dev;
  std::mt19937 rng(dev());

  if (blob_size > KILOBYTES(64)) {
    std::uniform_int_distribution<std::mt19937::result_type> distribution(0, 1);
    if (distribution(rng) == 1) {
      result = true;
    }
  }

  return result;
}

void GetSplitSizes(size_t blob_size, std::vector<size_t> &output) {
  std::random_device dev;
  std::mt19937 rng(dev());

  std::vector<int> split_choice = GetValidSplitChoices(blob_size);

  // Random pickup a number from split_choice to split the blob
  std::uniform_int_distribution<std::mt19937::result_type>
    position(0, split_choice.size()-1);
  int split_num = split_choice[position(rng)];

  size_t blob_each_portion {blob_size/split_num};
  for (int j {0}; j < split_num - 1; ++j) {
    output.push_back(blob_each_portion);
  }
  output.push_back(blob_size - blob_each_portion*(split_num-1));
}

Status RoundRobinPlacement(const std::vector<size_t> &blob_sizes,
                           std::vector<u64> &node_state,
                           std::vector<PlacementSchema> &output,
                           const std::vector<TargetID> &targets,
                           bool split) {
  Status result;
  std::vector<u64> ns_local(node_state.begin(), node_state.end());

  for (size_t i {0}; i < blob_sizes.size(); ++i) {
    PlacementSchema schema;

    if (split) {
      // Construct the vector for the splitted blob
      std::vector<size_t> new_blob_size;
      GetSplitSizes(blob_sizes[i], new_blob_size);

      for (size_t k {0}; k < new_blob_size.size(); ++k) {
        result = AddRoundRobinSchema(k, ns_local, new_blob_size, targets,
                                     schema);
        if (!result.Succeeded()) {
          break;
        }
      }
    } else {
      result = AddRoundRobinSchema(i, ns_local, blob_sizes, targets, schema);
      if (!result.Succeeded()) {
        return result;
      }
    }
    output.push_back(schema);
  }

  return result;
}

Status AddRandomSchema(std::multimap<u64, TargetID> &ordered_cap,
                       size_t blob_size, PlacementSchema &schema) {
  std::random_device rd;
  std::mt19937 gen(rd());
  Status result;

  auto itlow = ordered_cap.lower_bound(blob_size);
  if (itlow == ordered_cap.end()) {
    result = DPE_RANDOM_FOUND_NO_TGT;
    LOG(ERROR) << result.Msg();
  } else {
    // distance from lower bound to the end
    std::uniform_int_distribution<>
      dst_dist(1, std::distance(itlow, ordered_cap.end()));
    size_t dst_relative = dst_dist(gen);
    std::advance(itlow, dst_relative-1);
    ordered_cap.insert(std::pair<u64, TargetID>((*itlow).first-blob_size,
                                                (*itlow).second));

    schema.push_back(std::make_pair(blob_size, (*itlow).second));
    ordered_cap.erase(itlow);
  }

  return result;
}

Status RandomPlacement(const std::vector<size_t> &blob_sizes,
                       std::multimap<u64, TargetID> &ordered_cap,
                       std::vector<PlacementSchema> &output) {
  Status result;

  for (size_t i {0}; i < blob_sizes.size(); ++i) {
    PlacementSchema schema;

    // Split the blob
    if (SplitBlob(blob_sizes[i])) {
      // Construct the vector for the splitted blob
      std::vector<size_t> new_blob_size;
      GetSplitSizes(blob_sizes[i], new_blob_size);

      for (size_t k {0}; k < new_blob_size.size(); ++k) {
        result = AddRandomSchema(ordered_cap, new_blob_size[k], schema);

        if (!result.Succeeded()) {
          break;
        }
      }
    } else {
      // Blob size is less than 64KB or do not split
      result = AddRandomSchema(ordered_cap, blob_sizes[i], schema);
      if (!result.Succeeded()) {
        return result;
      }
    }
    output.push_back(schema);
  }

  return result;
}

Status MinimizeIoTimePlacement(const std::vector<size_t> &blob_sizes,
                               const std::vector<u64> &node_state,
                               const std::vector<f32> &bandwidths,
                               const std::vector<TargetID> &targets,
                               std::vector<PlacementSchema> &output,
                               const api::Context &ctx) {
  using operations_research::MPSolver;
  using operations_research::MPVariable;
  using operations_research::MPConstraint;
  using operations_research::MPObjective;

  Status result;
  const size_t num_targets = targets.size();
  const size_t num_blobs = blob_sizes.size();

  const double minimum_remaining_capacity =
    ctx.minimize_io_time_options.minimum_remaining_capacity;
  const double capacity_change_threshold =
    ctx.minimize_io_time_options.capacity_change_threshold;

  size_t constraints_per_target = 1;
  if (minimum_remaining_capacity != 0) {
    constraints_per_target++;
  }
  if (capacity_change_threshold != 0) {
    constraints_per_target++;
  }

  const size_t total_constraints =
    num_blobs + (num_targets * constraints_per_target) - 1;
  std::vector<MPConstraint*> blob_constrt(total_constraints);
  std::vector<std::vector<MPVariable*>> blob_fraction(num_blobs);
  MPSolver solver("LinearOpt", MPSolver::GLOP_LINEAR_PROGRAMMING);
  int num_constrts {0};

  // Constraint #1: Sum of fraction of each blob is 1
  for (size_t i {0}; i < num_blobs; ++i) {
    blob_constrt[num_constrts+i] = solver.MakeRowConstraint(1, 1);
    blob_fraction[i].resize(num_targets);

    // TODO(KIMMY): consider remote nodes?
    for (size_t j {0}; j < num_targets; ++j) {
      std::string var_name {"blob_dst_" + std::to_string(i) + "_" +
                            std::to_string(j)};
      blob_fraction[i][j] = solver.MakeNumVar(0.0, 1, var_name);
      blob_constrt[num_constrts+i]->SetCoefficient(blob_fraction[i][j], 1);
    }
  }
  num_constrts += num_blobs;

  // Constraint #2: Minimum Remaining Capacity Constraint
  if (minimum_remaining_capacity != 0) {
    for (size_t j {0}; j < num_targets; ++j) {
      double remaining_capacity_threshold =
        static_cast<double>(node_state[j]) * minimum_remaining_capacity;
      blob_constrt[num_constrts+j] = solver.MakeRowConstraint(
        0, static_cast<double>(node_state[j]) - remaining_capacity_threshold);
      for (size_t i {0}; i < num_blobs; ++i) {
        blob_constrt[num_constrts+j]->SetCoefficient(
          blob_fraction[i][j], static_cast<double>(blob_sizes[i]));
      }
    }
    num_constrts += num_targets;
  }

  // Constraint #3: Remaining Capacity Change Threshold
  if (capacity_change_threshold != 0) {
    for (size_t j {0}; j < num_targets; ++j) {
      blob_constrt[num_constrts+j] =
        solver.MakeRowConstraint(0, capacity_change_threshold * node_state[j]);
      for (size_t i {0}; i < num_blobs; ++i) {
        blob_constrt[num_constrts+j]->SetCoefficient(
          blob_fraction[i][j], static_cast<double>(blob_sizes[i]));
      }
    }
    num_constrts += num_targets;
  }

  // Placement Ratio
  for (size_t j {0}; j < num_targets-1; ++j) {
    blob_constrt[num_constrts+j] =
      solver.MakeRowConstraint(0, solver.infinity());
    for (size_t i {0}; i < num_blobs; ++i) {
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
  for (size_t i {0}; i < num_blobs; ++i) {
    for (size_t j {0}; j < num_targets; ++j) {
      objective->SetCoefficient(blob_fraction[i][j],
        static_cast<double>(blob_sizes[i])/bandwidths[j]);
    }
  }
  objective->SetMinimization();

  const MPSolver::ResultStatus result_status = solver.Solve();
  // Check if the problem has an optimal solution.
  if (result_status != MPSolver::OPTIMAL) {
    result = DPE_ORTOOLS_NO_SOLUTION;
    LOG(ERROR) << result.Msg();
    return result;
  }

  for (size_t i {0}; i < num_blobs; ++i) {
    PlacementSchema schema;
    size_t target_pos {0};  // to track the target with most data
    auto largest_bulk{blob_fraction[i][0]->solution_value()*blob_sizes[i]};
    // NOTE: could be inefficient if there are hundreds of targets
    for (size_t j {1}; j < num_targets; ++j) {
      if (blob_fraction[i][j]->solution_value()*blob_sizes[i] > largest_bulk)
        target_pos = j;
    }
    size_t blob_partial_sum {0};
    for (size_t j {0}; j < num_targets; ++j) {
      if (j == target_pos) {
        continue;
      }
      double check_frac_size {blob_fraction[i][j]->solution_value()*
                              blob_sizes[i]};  // blob fraction size
      size_t frac_size_cast = static_cast<size_t>(check_frac_size);
      // If size to this destination is not 0, push to result
      if (frac_size_cast != 0) {
        schema.push_back(std::make_pair(frac_size_cast, targets[j]));
        blob_partial_sum += frac_size_cast;
      }
    }
    // Push the rest data to target at target_pos
    schema.push_back(std::make_pair(blob_sizes[i]-blob_partial_sum,
                                    targets[target_pos]));
    output.push_back(schema);
  }

  return result;
}

PlacementSchema AggregateBlobSchema(PlacementSchema &schema) {
  std::unordered_map<u64, u64> place_size;
  PlacementSchema result;

  for (auto [size, target] : schema) {
    place_size.insert({target.as_int, 0});
    place_size[target.as_int] += size;
  }
  for (auto [target, size] : place_size) {
    TargetID id = {};
    id.as_int = target;
    result.push_back(std::make_pair(size, id));
  }

  return result;
}

enum Topology {
  Topology_Local,
  Topology_Neighborhood,
  Topology_Global,

  Topology_Count
};

Status CalculatePlacement(SharedMemoryContext *context, RpcContext *rpc,
                          const std::vector<size_t> &blob_sizes,
                          std::vector<PlacementSchema> &output,
                          const api::Context &api_context) {
  std::vector<PlacementSchema> output_tmp;
  Status result;

  // NOTE(chogan): Start with local targets and gradually expand the target list
  // until the placement succeeds
  for (int i = 0; i < Topology_Count; ++i) {
    std::vector<TargetID> targets;

    switch (i) {
      case Topology_Local: {
        // TODO(chogan): @optimization We can avoid the copy here when getting
        // local targets by just getting a pointer and length.
        targets = LocalGetNodeTargets(context);
        break;
      }
      case Topology_Neighborhood: {
        targets = GetNeighborhoodTargets(context, rpc);
        break;
      }
      case Topology_Global: {
        // TODO(chogan): GetGlobalTargets(context, rpc);
        break;
      }
    }

    if (targets.size() == 0) {
      result = DPE_PLACEMENTSCHEMA_EMPTY;
      continue;
    }

    std::vector<u64> node_state =
      GetRemainingTargetCapacities(context, rpc, targets);

    switch (api_context.policy) {
      // TODO(KIMMY): check device capacity against blob size
      case api::PlacementPolicy::kRandom: {
        std::multimap<u64, TargetID> ordered_cap;
        for (size_t i = 0; i < node_state.size(); ++i) {
          ordered_cap.insert(std::pair<u64, TargetID>(node_state[i],
                                                      targets[i]));
        }

        result = RandomPlacement(blob_sizes, ordered_cap, output_tmp);
        break;
      }
      case api::PlacementPolicy::kRoundRobin: {
        result = RoundRobinPlacement(blob_sizes, node_state,
                                     output_tmp, targets, api_context.rr_split);
        break;
      }
      case api::PlacementPolicy::kMinimizeIoTime: {
        std::vector<f32> bandwidths = GetBandwidths(context, targets);

        result = MinimizeIoTimePlacement(blob_sizes, node_state, bandwidths,
                                         targets, output_tmp, api_context);
        break;
      }
    }

    if (!result.Failed()) {
      break;
    }
  }

  // Aggregate placement schemas from the same target
  if (result.Succeeded()) {
    for (auto it = output_tmp.begin(); it != output_tmp.end(); ++it) {
      PlacementSchema schema = AggregateBlobSchema((*it));
      if (schema.size() == 0) {
        result = DPE_PLACEMENTSCHEMA_EMPTY;
        LOG(ERROR) << result.Msg();
        return result;
      }
      output.push_back(schema);
    }
  }

  return result;
}

}  // namespace hermes
