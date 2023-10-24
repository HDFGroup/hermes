#ifndef HRUN_BDEV_METHODS_H_
#define HRUN_BDEV_METHODS_H_

/** The set of methods in the admin task */
struct Method : public TaskMethod {
  TASK_METHOD_T kWrite = kLast + 0;
  TASK_METHOD_T kRead = kLast + 1;
  TASK_METHOD_T kAllocate = kLast + 2;
  TASK_METHOD_T kFree = kLast + 3;
  TASK_METHOD_T kStatBdev = kLast + 4;
  TASK_METHOD_T kUpdateScore = kLast + 5;
};

#endif  // HRUN_BDEV_METHODS_H_