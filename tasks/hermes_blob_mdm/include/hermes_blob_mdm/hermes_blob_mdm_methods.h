#ifndef HRUN_HERMES_BLOB_MDM_METHODS_H_
#define HRUN_HERMES_BLOB_MDM_METHODS_H_

/** The set of methods in the admin task */
struct Method : public TaskMethod {
  TASK_METHOD_T kPutBlob = kLast + 0;
  TASK_METHOD_T kGetBlob = kLast + 1;
  TASK_METHOD_T kTruncateBlob = kLast + 2;
  TASK_METHOD_T kDestroyBlob = kLast + 3;
  TASK_METHOD_T kTagBlob = kLast + 4;
  TASK_METHOD_T kBlobHasTag = kLast + 6;
  TASK_METHOD_T kGetBlobId = kLast + 8;
  TASK_METHOD_T kGetOrCreateBlobId = kLast + 9;
  TASK_METHOD_T kGetBlobName = kLast + 10;
  TASK_METHOD_T kGetBlobSize = kLast + 11;
  TASK_METHOD_T kGetBlobScore = kLast + 12;
  TASK_METHOD_T kGetBlobBuffers = kLast + 13;
  TASK_METHOD_T kRenameBlob = kLast + 14;
  TASK_METHOD_T kReorganizeBlob = kLast + 15;
  TASK_METHOD_T kSetBucketMdm = kLast + 16;
  TASK_METHOD_T kFlushData = kLast + 17;
  TASK_METHOD_T kPollBlobMetadata = kLast + 18;
  TASK_METHOD_T kPollTargetMetadata = kLast + 19;
};

#endif  // HRUN_HERMES_BLOB_MDM_METHODS_H_