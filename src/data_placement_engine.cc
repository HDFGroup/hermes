#include "data_placement_engine.h"

#include <assert.h>
#include <math.h>

#include <utility>

#include "ortools/linear_solver/linear_solver.h"

#include "hermes.h"
#include "metadata_management.h"

namespace hermes {

using hermes::api::Status;

enum class PlacementPolicy {
  kRandom,
  kTopDown,
  kPerformance,
};

// TODO(chogan): Unfinished sketch
Status TopDownPlacement(SharedMemoryContext *context, RpcContext *rpc,
                        std::vector<size_t> blob_sizes,
                        std::vector<TieredSchema> &output) {
  HERMES_NOT_IMPLEMENTED_YET;

  Status result = 0;
  std::vector<u64> global_state = GetGlobalTierCapacities(context, rpc);

  for (auto &blob_size : blob_sizes) {
    TieredSchema schema;
    size_t size_left = blob_size;
    TierID current_tier = 0;

    while (size_left > 0 && current_tier < global_state.size()) {
      size_t bytes_used = 0;
      if (global_state[current_tier] > size_left) {
        bytes_used = size_left;
      } else {
        bytes_used = global_state[current_tier];
        current_tier++;
      }

      if (bytes_used) {
        size_left -= bytes_used;
        schema.push_back(std::make_pair(current_tier, bytes_used));
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

Status RandomPlacement(SharedMemoryContext *context, RpcContext *rpc,
                       std::vector<size_t> &blob_sizes,
                       std::vector<TieredSchema> &output) {
  std::vector<u64> global_state = GetGlobalTierCapacities(context, rpc);
  Status result = 0;
  for (auto &blob_size : blob_sizes) {
    TieredSchema schema;
    TierID tier_id = rand() % global_state.size();

    if (global_state[tier_id] > blob_size) {
      schema.push_back(std::make_pair(blob_size, tier_id));
    } else {
      HERMES_NOT_IMPLEMENTED_YET;
      // TODO(chogan): Trigger BufferOrganizer
      // EvictBuffers(eviction_schema);
    }
    output.push_back(schema);
  }

  return result;
}

Status PerformancePlacement(SharedMemoryContext *context, RpcContext *rpc,
                            std::vector<size_t> &blob_sizes,
                            std::vector<TieredSchema> &output) {
  using operations_research::MPSolver;
  using operations_research::MPVariable;
  using operations_research::MPConstraint;
  using operations_research::MPObjective;

  Status result = 0;
  std::vector<u64> global_state = GetGlobalTierCapacities(context, rpc);
  std::vector<f32> bandwidths = GetBandwidths(context);
  std::vector<MPConstraint*> blob_constrt(blob_sizes.size() +
                                          global_state.size()*3-1);
  std::vector<std::vector<MPVariable*>> blob_fraction (blob_sizes.size());
  MPSolver solver("LinearOpt", MPSolver::GLOP_LINEAR_PROGRAMMING);
  int num_constrts {0};

  u64 avail_cap {0};
  // Apply Remaining Capacity Change Threshold 20%
  for (size_t j {0}; j < global_state.size(); ++j) {
    avail_cap += static_cast<u64>(global_state[j]*0.2);
  }

  u64 total_blob_size {0};
  for (size_t i {0}; i < blob_sizes.size(); ++i) {
    total_blob_size += blob_sizes[i];
  }

  if( total_blob_size > avail_cap) {
    HERMES_NOT_IMPLEMENTED_YET;
    assert(!"Available capacity is not enough for data placement\n");
  }

  // Sum of fraction of each blob is 1
  for (size_t i {0}; i < blob_sizes.size(); ++i) {
    blob_constrt[num_constrts+i] = solver.MakeRowConstraint(1, 1);
    blob_fraction[i].resize(global_state.size());

    // TODO (KIMMY): consider remote nodes?
    for (size_t j {0}; j < global_state.size(); ++j) {
      std::string var_name {"blob_dst_" + std::to_string(i) + "_" +
                            std::to_string(j)};
      blob_fraction[i][j] = solver.MakeNumVar(0.0, 1, var_name);
      blob_constrt[num_constrts+i]->SetCoefficient(blob_fraction[i][j], 1);
    }
  }

  // Minimum Remaining Capacity Constraint
  num_constrts += blob_sizes.size();
  for (size_t j {0}; j < global_state.size(); ++j) {
    blob_constrt[num_constrts+j] = solver.MakeRowConstraint(
      0, (static_cast<double>(global_state[j])-
      0.1*static_cast<double>(global_state[j])));
    for (size_t i {0}; i < blob_sizes.size(); ++i) {
      blob_constrt[num_constrts+j]->SetCoefficient(
        blob_fraction[i][j], static_cast<double>(blob_sizes[i]));
    }
  }

  // Remaining Capacity Change Threshold 20%
  num_constrts += global_state.size();
  for (size_t j {0}; j < global_state.size(); ++j) {
    blob_constrt[num_constrts+j] =
      solver.MakeRowConstraint(0, 0.2*global_state[j]);
    for (size_t i {0}; i < blob_sizes.size(); ++i) {
      blob_constrt[num_constrts+j]->SetCoefficient(
        blob_fraction[i][j], static_cast<double>(blob_sizes[i]));
    }
  }

  // Placement Ratio
  num_constrts += global_state.size();
  for (size_t j {0}; j < global_state.size()-1; ++j) {
    blob_constrt[num_constrts+j] =
      solver.MakeRowConstraint(0, solver.infinity());
    for (size_t i {0}; i < blob_sizes.size(); ++i) {
      blob_constrt[num_constrts+j]->SetCoefficient(
        blob_fraction[i][j+1], static_cast<double>(blob_sizes[i]));
      double placement_ratio = static_cast<double>(global_state[j+1])/
                                                   global_state[j];
      blob_constrt[num_constrts+j]->SetCoefficient(
        blob_fraction[i][j],
        static_cast<double>(blob_sizes[i])*(0-placement_ratio));
    }
  }

  // Objective to minimize IO time
  MPObjective* const objective = solver.MutableObjective();
  for (size_t i {0}; i < blob_sizes.size(); ++i) {
    for (size_t j {0}; j < global_state.size(); ++j) {
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
    TieredSchema schema;
    size_t tier_pos {0}; // to track the tier with most data
    auto largest_bulk{blob_fraction[i][0]->solution_value()*blob_sizes[i]};
    // NOTE: could be inefficient if there are hundreds of tiers
    for (size_t j {1}; j < global_state.size(); ++j) {
      if (blob_fraction[i][j]->solution_value()*blob_sizes[i] > largest_bulk)
        tier_pos = j;
    }
    size_t blob_partial_sum {0};
    for (size_t j {0}; j < global_state.size(); ++j) {
      if (j == tier_pos)
        continue;
      double check_frac_size {blob_fraction[i][j]->solution_value()*
                              blob_sizes[i]}; // blob fraction size
      size_t frac_size_cast = static_cast<size_t>(check_frac_size);
      // If size to this destination is not 0, push to result
      if (frac_size_cast != 0) {
        schema.push_back(std::make_pair(frac_size_cast, j));
        blob_partial_sum += frac_size_cast;
      }
    }
    // Push the rest data to tier tier_pos
    schema.push_back(std::make_pair(blob_sizes[i]-blob_partial_sum, tier_pos));
    output.push_back(schema);
  }

  return result;
}

Status CalculatePlacement(SharedMemoryContext *context, RpcContext *rpc,
                          std::vector<size_t> &blob_sizes,
                          std::vector<TieredSchema> &output,
                          const api::Context &api_context) {
  (void)api_context;
  Status result = 0;

  // TODO(chogan): Return a TieredSchema that minimizes a cost function F given
  // a set of N Tiers and a blob, while satisfying a policy P.

  // TODO(chogan): This should be part of the Context
  PlacementPolicy policy = PlacementPolicy::kRandom;

  switch (policy) {
    case PlacementPolicy::kRandom: {
      result = RandomPlacement(context, rpc, blob_sizes, output);
      break;
    }
    case PlacementPolicy::kTopDown: {
      result = TopDownPlacement(context, rpc, blob_sizes, output);
      break;
    }
    case PlacementPolicy::kPerformance: {
      result = PerformancePlacement(context, rpc, blob_sizes, output);
      break;
    }
  }

  return result;
}

}  // namespace hermes
