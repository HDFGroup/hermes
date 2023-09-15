#ifndef LABSTOR_HERMES_BUCKET_MDM_METHODS_H_
#define LABSTOR_HERMES_BUCKET_MDM_METHODS_H_

/** The set of methods in the admin task */
struct Method : public TaskMethod {
  TASK_METHOD_T kGetOrCreateTag = kLast + 0;
  TASK_METHOD_T kGetTagId = kLast + 1;
  TASK_METHOD_T kGetTagName = kLast + 2;
  TASK_METHOD_T kRenameTag = kLast + 3;
  TASK_METHOD_T kDestroyTag = kLast + 4;
  TASK_METHOD_T kTagAddBlob = kLast + 5;
  TASK_METHOD_T kTagRemoveBlob = kLast + 6;
  TASK_METHOD_T kTagClearBlobs = kLast + 10;
  TASK_METHOD_T kUpdateSize = kLast + 11;
  TASK_METHOD_T kAppendBlobSchema = kLast + 12;
  TASK_METHOD_T kAppendBlob = kLast + 13;
  TASK_METHOD_T kGetSize = kLast + 14;
  TASK_METHOD_T kSetBlobMdm = kLast + 15;
};

#endif  // LABSTOR_HERMES_BUCKET_MDM_METHODS_H_