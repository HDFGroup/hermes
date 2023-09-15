#ifndef LABSTOR_PROC_QUEUE_METHODS_H_
#define LABSTOR_PROC_QUEUE_METHODS_H_

/** The set of methods in the admin task */
struct Method : public TaskMethod {
  TASK_METHOD_T kPush = kLast + 0;
};

#endif  // LABSTOR_PROC_QUEUE_METHODS_H_