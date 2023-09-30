#ifndef LABSTOR_DATA_STAGER_METHODS_H_
#define LABSTOR_DATA_STAGER_METHODS_H_

/** The set of methods in the admin task */
struct Method : public TaskMethod {
  TASK_METHOD_T kRegisterStager = kLast + 0;
  TASK_METHOD_T kStageIn = kLast + 1;
  TASK_METHOD_T kStageOut = kLast + 2;
};

#endif  // LABSTOR_DATA_STAGER_METHODS_H_