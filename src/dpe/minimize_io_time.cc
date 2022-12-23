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
                                 const lipc::vector<TargetInfo> &targets,
                                 const api::Context &ctx,
                                 std::vector<PlacementSchema> &output) {
  Status result;
  const size_t num_targets = targets.size();
  const size_t num_blobs = blob_sizes.size();

  const double minimum_remaining_capacity =
      ctx.minimize_io_time_options.minimum_remaining_capacity;
  const double capacity_change_threshold =
      ctx.minimize_io_time_options.capacity_change_threshold;
  GetPlacementRatios(targets, ctx);

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
    for (size_t j = 0; j < num_targets; ++j) {
      lp.SetConstraintCoeff(var.Get(i, j), 1);
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
        static_cast<double>(targets[j].rem_cap_) * (1 - minimum_remaining_capacity);
    double est_rem_cap = capacity_change_threshold * targets[j].rem_cap_;
    double max_capacity = std::max({rem_cap_thresh, est_rem_cap});
    if (max_capacity > 0) {
      lp.AddConstraint("rem_cap_", GLP_DB, 0.0, max_capacity);
    } else {
      lp.AddConstraint("rem_gap_", GLP_FX, 0.0, 0.0);
    }
    for (size_t i = 0; i < num_blobs; ++i) {
      lp.SetConstraintCoeff(var.Get(i, j),
                            static_cast<double>(blob_sizes[i]));
    }
  }

  // Objective to minimize IO time
  lp.SetObjective(GLP_MIN);
  for (size_t i = 0; i < num_blobs; ++i) {
    for (size_t j = 0; j < num_targets; ++j) {
      lp.SetObjectiveCoeff(var.Get(i, j),
                           static_cast<double>(blob_sizes[i])/bandwidths[j]);
    }
  }

  // Solve the problem
  lp.Solve();
  if (!lp.IsOptimal()) {
    result = DPE_MIN_IO_TIME_NO_SOLUTION;
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
    std::vector<size_t> vars_bytes(vars.begin(), vars.end());

    // Account for rounding errors
    size_t est_io_size_u = std::accumulate(vars_bytes.begin(),
                                         vars_bytes.end(),
                                         0ul);
    ssize_t est_io_size = static_cast<ssize_t>(est_io_size_u);
    ssize_t true_io_size = static_cast<ssize_t>(blob_sizes[i]);
    ssize_t io_diff = true_io_size - est_io_size;
    PlaceBytes(0, io_diff, vars_bytes, targets);

    // Push all non-zero schemas to target
    for (size_t j = 0; j < num_targets; ++j) {
      size_t io_to_target = vars_bytes[j];
      if (io_to_target != 0) {
        schema.AddSubPlacement(io_to_target, targets[j].id_);
      }
    }
    output.emplace_back(std::move(schema));
  }
  return result;
}

void MinimizeIoTime::PlaceBytes(size_t j, ssize_t bytes,
                                std::vector<size_t> &vars_bytes,
                                const lipc::vector<TargetInfo> &targets) {
  if (vars_bytes[j] == 0) {
    PlaceBytes(j+1,  bytes, vars_bytes, targets);
    return;
  }
  ssize_t node_cap = static_cast<ssize_t>(targets[j].rem_cap_);
  ssize_t req_bytes = static_cast<ssize_t>(vars_bytes[j]);
  req_bytes += bytes;
  ssize_t io_diff = req_bytes - node_cap;
  if (io_diff <= 0) {
    vars_bytes[j] = static_cast<size_t>(req_bytes);
    return;
  }
  if (j == targets.size() - 1) {
    LOG(FATAL) << "No capacity left to buffer blob" << std::endl;
    return;
  }
  req_bytes -= io_diff;
  vars_bytes[j] = static_cast<size_t>(req_bytes);
  PlaceBytes(j+1, io_diff, vars_bytes, targets);
}

void MinimizeIoTime::GetPlacementRatios(const lipc::vector<TargetInfo> &targets,
                                        const api::Context &ctx) {
  placement_ratios_.reserve(targets.size());
  if (ctx.minimize_io_time_options.use_placement_ratio) {
    // Total number of bytes remaining across all targets
    size_t total_bytes = 0;
    for (auto iter = targets.cbegin(); iter != targets.cend(); ++iter) {
      total_bytes += (*iter).rem_cap_;
    }
    double total = static_cast<double>(total_bytes);
    // Get percentage of total capacity each target has
    for (size_t j = 0; j < targets.size() - 1; ++j) {
      double target_cap = static_cast<double>(targets[j].rem_cap_);
      double placement_ratio = target_cap / total;
      placement_ratios_.emplace_back(placement_ratio);
    }
    // Last tier always has a 1.0 to guarantee it's utilized
    placement_ratios_.emplace_back(1.0);
  } else {
    for (size_t j = 0; j < targets.size(); ++j) {
      placement_ratios_.emplace_back(1.0);
    }
  }
}

}  // namespace hermes
