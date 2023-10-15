#ifndef HRUN_HERMES_ADAPTERS_METHODS_H_
#define HRUN_HERMES_ADAPTERS_METHODS_H_

/** The set of methods in the admin task */
struct Method : public TaskMethod {
  TASK_METHOD_T kCustom = kLast + 0;
};

#endif  // HRUN_HERMES_ADAPTERS_METHODS_H_