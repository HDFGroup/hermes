//
// Created by lukemartinlogan on 9/9/22.
//

#include "minimize_io_time.h"
#include <assert.h>
#include <glpk.h>
#include <math.h>

namespace hermes {

Status MinimizeIoTime::Placement(const std::vector<size_t> &blob_sizes,
                               const std::vector<u64> &node_state,
                               const std::vector<f32> &bandwidths,
                               const std::vector<TargetID> &targets,
                               std::vector<PlacementSchema> &output,
                               const api::Context &ctx) {
  Status result;
  const size_t num_targets = targets.size();
  const size_t num_blobs = blob_sizes.size();

  const double minimum_remaining_capacity =
      ctx.minimize_io_time_options.minimum_remaining_capacity;
  const double capacity_change_threshold =
      ctx.minimize_io_time_options.capacity_change_threshold;

  size_t constraints_per_target = 1;
  VLOG(1) << "MinimizeIoTimePlacement()::minimum_remaining_capacity=" <<
      minimum_remaining_capacity;
  if (minimum_remaining_capacity != 0) {
    constraints_per_target++;
  }
  if (capacity_change_threshold != 0) {
    constraints_per_target++;
  }
  VLOG(1) << "MinimizeIoTimePlacement()::constraints_per_target=" <<
      constraints_per_target;
  const size_t total_constraints =
      num_blobs + (num_targets * constraints_per_target) - 1;
  glp_prob *lp = glp_create_prob();
  int ia[1+1000], ja[1+1000], last = 0;
  double ar[1+1000];
  // double x[num_blobs * num_targets];

  glp_set_prob_name(lp, "min_io");
  glp_set_obj_dir(lp, GLP_MIN);
  glp_add_rows(lp, total_constraints);
  glp_add_cols(lp, num_blobs * num_targets);

  int num_constrts {0};   // counter that increase to 1) blobs 2) targets X 3

  // Constraint #1: Sum of fraction of each blob is 1.
  for (size_t i {0}; i < num_blobs; ++i) {
    // Use GLP_FX for the fixed contraint of 1.
    std::string row_name {"blob_row_" + std::to_string(i)};
    glp_set_row_name(lp, i+1, row_name.c_str());
    glp_set_row_bnds(lp, i+1, GLP_FX, 1.0, 1.0);

    // TODO(KIMMY): consider remote nodes?
    for (size_t j {0}; j < num_targets; ++j) {
      int ij = i * num_targets + j + 1;
      std::string var_name {"blob_dst_" + std::to_string(i) + "_" +
                           std::to_string(j)};
      // total number of variables = number of blobs * number of targets.
      glp_set_col_name(lp, ij, var_name.c_str());
      glp_set_col_bnds(lp, ij, GLP_DB, 0.0, 1.0);
      ia[ij] = i+1, ja[ij] = j+1, ar[ij] = 1.0;  // var[i][j] = 1.0
      last = ij;
    }
  }
  num_constrts += num_blobs;

  // Constraint #2: Minimum Remaining Capacity Constraint
  int last2 = 0;
  if (minimum_remaining_capacity != 0) {
    for (size_t j{0}; j < num_targets; ++j) {
      double remaining_capacity_threshold =
          static_cast<double>(node_state[j]) * minimum_remaining_capacity;
      std::string row_name{"mrc_row_" + std::to_string(j)};
      glp_set_row_name(lp, num_constrts + j + 1, row_name.c_str());
      glp_set_row_bnds(
          lp, num_constrts + j + 1, GLP_DB, 0.0,
          static_cast<double>(node_state[j]) - remaining_capacity_threshold);

      for (size_t i{0}; i < num_blobs; ++i) {
        // Starting row of contraint array is (blob * target)*num_constrts.
        int ij = j * num_blobs + i + 1 + last;
        ia[ij] = num_constrts + j + 1, ja[ij] = j + 1,
        ar[ij] = static_cast<double>(blob_sizes[i]);
        last2 = ij;
      }
    }
    num_constrts += num_targets;
  } else {
    last2 = last;
  }

  // Constraint #3: Remaining Capacity Change Threshold
  int last3 = 0;
  if (capacity_change_threshold != 0) {
    for (size_t j {0}; j < num_targets; ++j) {
      std::string row_name {"rcct_row_" + std::to_string(j)};
      glp_set_row_name(lp, num_constrts+j+1, row_name.c_str());
      glp_set_row_bnds(lp, num_constrts+j+1, GLP_DB, 0.0,
                       capacity_change_threshold * node_state[j]);
      for (size_t i {0}; i < num_blobs; ++i) {
        int ij = j * num_blobs + i + 1 + last2;
        ia[ij] = num_constrts+j+1, ja[ij] = j+1,
        ar[ij] = static_cast<double>(blob_sizes[i]);
        last3 = ij;
      }
    }
    num_constrts += num_targets;
  } else {
    last3 = last2;
  }

  int last4 = 0;

  // Placement Ratio
  if (ctx.minimize_io_time_options.use_placement_ratio) {
    for (size_t j {0}; j < num_targets-1; ++j) {
      std::string row_name {"pr_row_" + std::to_string(j)};
      glp_set_row_name(lp, num_constrts+j+1, row_name.c_str());
      glp_set_row_bnds(lp, num_constrts+j+1, GLP_LO, 0.0, 0.0);

      for (size_t i {0}; i < num_blobs; ++i) {
        int ij = j * num_blobs + i + 1 + last3 + j;
        ia[ij] = num_constrts+j+1, ja[ij] = j+2,
        ar[ij] = static_cast<double>(blob_sizes[i]);

        double placement_ratio = static_cast<double>(node_state[j+1])/
                                 node_state[j];
        ij = ij + 1;
        ia[ij] = num_constrts+j+1, ja[ij] = j+1,
        ar[ij] = static_cast<double>(blob_sizes[i])*(0-placement_ratio);
        last4 = ij;
      }
    }
  } else {
    last4 = last3;
  }

  // Objective to minimize IO time
  for (size_t i {0}; i < num_blobs; ++i) {
    for (size_t j {0}; j < num_targets; ++j) {
      int ij = i * num_targets + j + 1;
      glp_set_obj_coef(lp, ij,
                       static_cast<double>(blob_sizes[i])/bandwidths[j]);
    }
  }
  VLOG(1) << "MinimizeIoTimePlacement()::last4=" << last4;

  glp_load_matrix(lp, last4, ia, ja, ar);
  glp_smcp parm;
  glp_init_smcp(&parm);
  parm.msg_lev = GLP_MSG_OFF;
  glp_simplex(lp, &parm);

  // Check if the problem has an optimal solution.
  if (glp_get_status(lp) != GLP_OPT) {
    result = DPE_ORTOOLS_NO_SOLUTION;
    LOG(ERROR) << result.Msg();
    glp_delete_prob(lp);
    glp_free_env();
    return result;
  }
  glp_get_obj_val(lp);

  for (size_t i {0}; i < num_blobs; ++i) {
    PlacementSchema schema;
    size_t target_pos {0};  // to track the target with most data
    auto largest_bulk{glp_get_col_prim(lp, i*num_targets+1) *blob_sizes[i]};
    // NOTE: could be inefficient if there are hundreds of targets
    for (size_t j {1}; j < num_targets; ++j) {
      if (glp_get_col_prim(lp, i*num_targets+j+1)*blob_sizes[i] > largest_bulk)
        target_pos = j;
    }

    size_t blob_partial_sum {0};

    for (size_t j {0}; j < num_targets; ++j) {
      if (j == target_pos) {
        continue;
      }
      double check_frac_size {glp_get_col_prim(lp, i*num_targets+j+1)*
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
  glp_delete_prob(lp);
  glp_free_env();
  return result;
}

}