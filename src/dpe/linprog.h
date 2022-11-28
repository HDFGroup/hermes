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

#ifndef HERMES_SRC_DPE_LINPROG_H_
#define HERMES_SRC_DPE_LINPROG_H_

#include <glpk.h>
#include <math.h>

#include <string>
#include <vector>

namespace hermes {

/**
   A structure to represent 2D array index
*/
struct Array2DIdx {
  int nrow_; /**< number of rows */
  int ncol_; /**< number of columns */

  /** Initialize structure with \a nrow and \a ncol. */
  Array2DIdx(int nrow, int ncol) : nrow_(nrow), ncol_(ncol) {}

  /** Get the index of row \a i and column \a j. */
  int Get(int i, int j) { return i * nrow_ + j; }

  /** Get the beginning index of row \a i.  */
  int Begin(int i) { return Get(i, 0); }

  /** Get the last index of row \a i.  */
  int End(int i) { return Get(i, ncol_); }
};

/**
 * GLPK documentation: http://most.ccib.rutgers.edu/glpk.pdf
 * */

const size_t kDefaultCoeffs = 1000 + 1;

/**
   A structure to represent linear program
*/
class LinearProgram {
 private:
  int num_vars_;           /**< number of variables */
  int num_constraints_;    /**< number of constraints */
  glp_prob *lp_;           /**< pointer to GLPK problem solver */
  std::vector<int> ia_;    /**< The "i" in A[i][j] */
  std::vector<int> ja_;    /**< The "j" in A[i][j] */
  std::vector<double> ar_; /**< values for A[i][j] */
  size_t cur_constraint_;  /**< current constraint */
  size_t result_;          /**< result of solver */

 public:
  /** Initialize linear program solver with \a name. */
  explicit LinearProgram(const char *name) {
    lp_ = glp_create_prob();
    glp_set_prob_name(lp_, name);
    cur_constraint_ = 0;
  }

  /** Clean up resources used by the GLPK solver. */
  ~LinearProgram() {
    glp_delete_prob(lp_);
    glp_free_env();
  }

  /** Define the problem size using \a num_vars and \a num_constraints. */
  void DefineDimension(int num_vars, int num_constraints) {
    // NOTE(llogan): GLPK requires arrays start from "1" instead of "0"
    glp_add_rows(lp_, num_constraints);
    glp_add_cols(lp_, num_vars);
    ia_.reserve(kDefaultCoeffs);
    ia_.emplace_back(0);
    ja_.reserve(kDefaultCoeffs);
    ja_.emplace_back(0);
    ar_.reserve(kDefaultCoeffs);
    ar_.emplace_back(0.0);
    num_vars_ = num_vars;
    num_constraints_ = num_constraints;
  }

  /** Add a constraint to the GLPK solver. */
  void AddConstraint(const std::string &base_name, int op_type, double lb,
                     double ub) {
    cur_constraint_ += 1;
    std::string name = base_name + std::to_string(cur_constraint_);
    glp_set_row_name(lp_, cur_constraint_, name.c_str());
    glp_set_row_bnds(lp_, cur_constraint_, op_type, lb, ub);
  }

  /** Set coefficient value as \a val for the \a var variable. */
  void SetConstraintCoeff(int var, double val) {
    var += 1;
    ia_.emplace_back(cur_constraint_);
    ja_.emplace_back(var);
    ar_.emplace_back(val);
  }

  /** Set the upper and lower bound for \a var variable. */
  void SetVariableBounds(const std::string &base_name, int var, int op_type,
                         double lb, double ub) {
    var += 1;
    std::string name = base_name + std::to_string(var);
    glp_set_col_name(lp_, var, name.c_str());
    glp_set_col_bnds(lp_, var, op_type, lb, ub);
  }

  /** Set the objective function to optimize. */
  void SetObjective(int objective) { glp_set_obj_dir(lp_, objective); }

  /** Set the coefficients of objective function. */
  void SetObjectiveCoeff(int var, double val) {
    var += 1;
    glp_set_obj_coef(lp_, var, val);
  }

  /** Solve the problem. */
  void Solve() {
    glp_load_matrix(lp_, ia_.size() - 1, ia_.data(), ja_.data(), ar_.data());
    glp_smcp parm;
    glp_init_smcp(&parm);
    parm.msg_lev = GLP_MSG_OFF;
    glp_simplex(lp_, &parm);
    result_ = glp_get_status(lp_);
  }

  /** Check if optimal solution exists. */
  bool IsOptimal() { return result_ == GLP_OPT; }

  /** Get solution.*/
  double GetSolution() { return glp_get_obj_val(lp_); }

  /** Get the values for optimal solution.*/
  double GetVariable(int var) {
    var += 1;
    return glp_get_col_prim(lp_, var);
  }

  /** Collect the values as a vector.*/
  std::vector<double> ToVector() {
    std::vector<double> v;
    v.reserve(num_vars_);
    for (int var = 0; var < num_vars_; ++var) {
      v.emplace_back(GetVariable(var));
    }
    return v;
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_DPE_LINPROG_H_
