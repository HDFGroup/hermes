#ifndef HRUN_HERMES_DATA_OP_METHODS_H_
#define HRUN_HERMES_DATA_OP_METHODS_H_

/** The set of methods in the admin task */
struct Method : public TaskMethod {
  TASK_METHOD_T kRegisterOp = kLast + 0;
  TASK_METHOD_T kRegisterData = kLast + 1;
  TASK_METHOD_T kRunOp = kLast + 2;
};

#endif  // HRUN_HERMES_DATA_OP_METHODS_H_