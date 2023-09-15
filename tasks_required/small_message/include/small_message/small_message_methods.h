#ifndef LABSTOR_SMALL_MESSAGE_METHODS_H_
#define LABSTOR_SMALL_MESSAGE_METHODS_H_

/** The set of methods in the admin task */
struct Method : public TaskMethod {
  TASK_METHOD_T kMd = kLast + 0;
  TASK_METHOD_T kIo = kLast + 1;
  TASK_METHOD_T kMdPush = kLast + 2;
};

#endif  // LABSTOR_SMALL_MESSAGE_METHODS_H_