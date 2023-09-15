#ifndef LABSTOR_LABSTOR_ADMIN_METHODS_H_
#define LABSTOR_LABSTOR_ADMIN_METHODS_H_

/** The set of methods in the admin task */
struct Method : public TaskMethod {
  TASK_METHOD_T kCreateTaskState = kLast + 0;
  TASK_METHOD_T kDestroyTaskState = kLast + 1;
  TASK_METHOD_T kRegisterTaskLib = kLast + 2;
  TASK_METHOD_T kDestroyTaskLib = kLast + 3;
  TASK_METHOD_T kGetOrCreateTaskStateId = kLast + 4;
  TASK_METHOD_T kGetTaskStateId = kLast + 5;
  TASK_METHOD_T kStopRuntime = kLast + 6;
  TASK_METHOD_T kSetWorkOrchQueuePolicy = kLast + 7;
  TASK_METHOD_T kSetWorkOrchProcPolicy = kLast + 8;
};

#endif  // LABSTOR_LABSTOR_ADMIN_METHODS_H_