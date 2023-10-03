#ifndef LABSTOR_DATA_STAGER_METHODS_H_
#define LABSTOR_DATA_STAGER_METHODS_H_

/** The set of methods in the admin task */
struct Method : public TaskMethod {
  TASK_METHOD_T kRegisterStager = kLast + 0;
  TASK_METHOD_T kUnregisterStager = kLast + 1;
  TASK_METHOD_T kStageIn = kLast + 2;
  TASK_METHOD_T kStageOut = kLast + 3;
};

#endif  // LABSTOR_DATA_STAGER_METHODS_H_