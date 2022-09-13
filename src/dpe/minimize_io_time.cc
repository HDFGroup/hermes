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

#include "minimize_io_time.h"
#include "linprog.h"
#include <assert.h>
#include <math.h>
#include <numeric>

namespace hermes {

Status MinimizeIoTime::Placement(const std::vector<size_t> &blob_sizes,
                                  const std::vector<u64> &node_state,
                                  const std::vector<TargetID> &targets,
                                  const api::Context &ctx,
                                  std::vector<PlacementSchema> &output) {
  LOG(INFO) << "Placement" << std::endl;

  Status result;
  const size_t num_targets = targets.size();
  const size_t num_blobs = blob_sizes.size();
  VERIFY_DPE_POLICY(ctx)

  const double minimum_remaining_capacity =
      ctx.minimize_io_time_options.minimum_remaining_capacity;
  const double capacity_change_threshold =
      ctx.minimize_io_time_options.capacity_change_threshold;
  GetPlacementRatios(node_state, ctx);

  size_t constraints_per_target = 2;
  VLOG(1) << "MinimizeIoTimePlacement()::minimum_remaining_capacity=" <<
      minimum_remaining_capacity;
  VLOG(1) << "MinimizeIoTimePlacement()::constraints_per_target=" <<
      constraints_per_target;
  const size_t total_constraints =
      num_blobs + (num_targets * constraints_per_target);
  Array2DIdx var(num_blobs, num_targets);
  hermes::LinearProgram lp("min_io");
  lp.DefineDimension(num_blobs * num_targets, total_constraints);

  // Constraint #1: Sum of fraction of each blob is 1.
  for (size_t i = 0; i < num_blobs; ++i) {
    // Ensure the entire row sums up to 1
    lp.AddConstraint("blob_row_", GLP_FX, 1.0, 1.0);

    // TODO(KIMMY): consider remote nodes?
    for (size_t j = 0; j < num_targets; ++j) {
      lp.SetConstraintCoeff(var.Get(i,j), 1);
      if (placement_ratios_[j] > 0) {
        lp.SetVariableBounds("blob_dst_", var.Get(i, j), GLP_DB, 0,
                             placement_ratios_[j]);
      } else {
        lp.SetVariableBounds("blob_dst_", var.Get(i, j), GLP_FX, 0, 0);
      }
    }
  }

  // Constraint #2: Capacity constraints
  for (size_t j = 0; j < num_targets; ++j) {
    double rem_cap_thresh =
        static_cast<double>(node_state[j]) -
        static_cast<double>(node_state[j]) * minimum_remaining_capacity;
    double est_rem_cap = capacity_change_threshold * node_state[j];
    double max_capacity = std::max({rem_cap_thresh, est_rem_cap});
    if (max_capacity > 0) {
      lp.AddConstraint("rem_cap_", GLP_DB, 0.0, max_capacity);
    } else {
      lp.AddConstraint("rem_gap_", GLP_FX, 0.0, 0.0);
    }
    for (size_t i = 0; i < num_blobs; ++i) {
      lp.SetConstraintCoeff(var.Get(i,j), static_cast<double>(blob_sizes[i]));
    }
  }

  // Objective to minimize IO time
  lp.SetObjective(GLP_MIN);
  for (size_t i = 0; i < num_blobs; ++i) {
    for (size_t j = 0; j < num_targets; ++j) {
      lp.SetObjectiveCoeff(var.Get(i,j),
                           static_cast<double>(blob_sizes[i])/bandwidths[j]);
    }
  }

  // Solve the problem
  lp.Solve();
  if (!lp.IsOptimal()) {
    result = DPE_ORTOOLS_NO_SOLUTION;
    LOG(ERROR) << result.Msg();
    return result;
  }
  std::vector<double> vars = lp.ToVector();

  // Create the placement schema
  for (size_t i = 0; i < num_blobs; ++i) {
    PlacementSchema schema;
    int row_start = var.Begin(i);
    int row_end = var.End(i);

    // Convert solution from blob fractions to bytes
    std::transform(vars.begin() + row_start,
                   vars.begin() + row_end,
                   vars.begin() + row_start,
                   std::bind(std::multiplies<double>(),
                       std::placeholders::_1, blob_sizes[i]));
    std::vector<size_t> vars_bytes(vars.begin() + row_start,
                                   vars.begin() + row_end);

    // Account for rounding error due to fractions
    auto iter = std::max_element(
        vars_bytes.begin(),
        vars_bytes.end());
    double io_size_frac = std::accumulate(vars_bytes.begin(),
                                          vars_bytes.end(),
                                          0.0);
    size_t io_size = static_cast<size_t>(io_size_frac);
    size_t io_diff = blob_sizes[i] - io_size;
    (*iter) += io_diff;

    // Push all non-zero schemas to target
    for (size_t j = 0; j < num_targets; ++j) {
      size_t io_to_target = vars_bytes[j];
      if (io_to_target != 0) {
        schema.push_back(std::make_pair(io_to_target, targets[j]));
      }
    }
    output.push_back(schema);
  }
  return result;
}

void MinimizeIoTime::GetPlacementRatios(const std::vector<u64> &node_state,
                                        const api::Context &ctx) {
  placement_ratios_.reserve(node_state.size());
  if (ctx.minimize_io_time_options.use_placement_ratio) {
    size_t total_bytes = std::accumulate(node_state.begin(), node_state.end(), (size_t)0);
    double total = static_cast<double>(total_bytes);
    for (size_t j = 0; j < node_state.size() - 1; ++j) {
      double target_cap = static_cast<double>(node_state[j]);
      double placement_ratio = target_cap / total;
      placement_ratios_.emplace_back(placement_ratio);
    }
    placement_ratios_.emplace_back(1.0);
  } else {
    for (size_t j = 0; j < node_state.size(); ++j) {
      placement_ratios_.emplace_back(1.0);
    }
  }
}

}  // namespace hermes
