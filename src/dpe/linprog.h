//
// Created by lukemartinlogan on 9/12/22.
//

#ifndef HERMES_SRC_DPE_LINPROG_H_
#define HERMES_SRC_DPE_LINPROG_H_

#include <glpk.h>
#include <string>
#include <math.h>
#include <vector>

namespace hermes {

struct Array2DIdx {
  int nrow_, ncol_;
  Array2DIdx(int nrow, int ncol) : nrow_(nrow), ncol_(ncol) {}
  int Get(int i, int j) {
    return i*nrow_ + j;
  }
  int Begin(int i) {
    return Get(i,0);
  }
  int End(int i) {
    return Get(i, ncol_);
  }
};

/**
 * GLPK documentation: http://most.ccib.rutgers.edu/glpk.pdf
 * */

class LinearProgram {
 private:
  int num_vars_, num_constraints_;
  glp_prob *lp_;
  std::vector<int> ia_; // The "i" in A[i][j]
  std::vector<int> ja_; // The "j" in A[i][j]
  std::vector<double> ar_;
  int off_;
  int cur_constraint_;
  int result_;

 public:
  LinearProgram(const char *name) {
    lp_ = glp_create_prob();
    glp_set_prob_name(lp_, name);
    cur_constraint_ = 0;
    off_ = 0;
  }
  ~LinearProgram() {
    glp_delete_prob(lp_);
    glp_free_env();
  }

  void DefineDimension(int num_vars, int num_constraints) {
    // NOTE(llogan): GLPK requires arrays start from "1" instead of "0"
    int total = num_vars * num_constraints + 1;
    glp_add_rows(lp_, num_constraints);
    glp_add_cols(lp_, num_vars);
    ia_.resize(total, 0);
    ja_.resize(total, 0);
    ar_.resize(total, 0);
    num_vars_ = num_vars;
    num_constraints_ = num_constraints;
  }

  void AddConstraint(const std::string &base_name,
                        int op_type, double lb, double ub) {
    cur_constraint_ += 1;
    std::string name = base_name + std::to_string(cur_constraint_);
    glp_set_row_name(lp_, cur_constraint_, name.c_str());
    glp_set_row_bnds(lp_, cur_constraint_, op_type, lb, ub);
  }

  void SetConstraintCoeff(int var, double val) {
    int off = GetOffset(var);
    ia_[off] = cur_constraint_;
    ja_[off] = var;
    ar_[off] = val;
  }

  void SetVariableBounds(const std::string &base_name, int var,
                         int op_type, double lb, double ub) {
    var += 1;
    std::string name = base_name + std::to_string(var);
    glp_set_col_name(lp_, var, name.c_str());
    glp_set_col_bnds(lp_, var, op_type, lb, ub);
  }

  void SetObjective(int objective) {
    glp_set_obj_dir(lp_, objective);
  }

  void SetObjectiveCoeff(int var, double val) {
    var += 1;
    glp_set_obj_coef(lp_, var, val);
  }

  void Solve() {
    glp_load_matrix(lp_, off_, ia_.data(), ja_.data(), ar_.data());
    glp_smcp parm;
    glp_init_smcp(&parm);
    parm.msg_lev = GLP_MSG_OFF;
    glp_simplex(lp_, &parm);
    result_ = glp_get_status(lp_);
  }

  bool IsOptimal() {
    return result_ == GLP_OPT;
  }

  double GetSolution() {
    return glp_get_obj_val(lp_);
  }

  double GetVariable(int var) {
    var += 1;
    return glp_get_col_prim(lp_, var);
  }

  std::vector<double> ToVector() {
    std::vector<double> v;
    v.reserve(num_vars_);
    for (int var = 0; var < num_vars_; ++var) {
      v.emplace_back(GetVariable(var));
    }
    return v;
  }

 protected:
  int GetOffset(int &var) {
    var += 1;
    return ++off_;
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_DPE_LINPROG_H_
