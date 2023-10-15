#ifndef HRUN_REMOTE_QUEUE_METHODS_H_
#define HRUN_REMOTE_QUEUE_METHODS_H_

/** The set of methods in the admin task */
struct Method : public TaskMethod {
  TASK_METHOD_T kPush = kLast + 0;
  TASK_METHOD_T kDup = kLast + 1;
};

#endif  // HRUN_REMOTE_QUEUE_METHODS_H_