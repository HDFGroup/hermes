#ifndef HRUN_DATA_STAGER_LIB_EXEC_H_
#define HRUN_DATA_STAGER_LIB_EXEC_H_

/** Execute a task */
void Run(u32 method, Task *task, RunContext &rctx) override {
  switch (method) {
    case Method::kConstruct: {
      Construct(reinterpret_cast<ConstructTask *>(task), rctx);
      break;
    }
    case Method::kDestruct: {
      Destruct(reinterpret_cast<DestructTask *>(task), rctx);
      break;
    }
    case Method::kRegisterStager: {
      RegisterStager(reinterpret_cast<RegisterStagerTask *>(task), rctx);
      break;
    }
    case Method::kUnregisterStager: {
      UnregisterStager(reinterpret_cast<UnregisterStagerTask *>(task), rctx);
      break;
    }
    case Method::kStageIn: {
      StageIn(reinterpret_cast<StageInTask *>(task), rctx);
      break;
    }
    case Method::kStageOut: {
      StageOut(reinterpret_cast<StageOutTask *>(task), rctx);
      break;
    }
    case Method::kUpdateSize: {
      UpdateSize(reinterpret_cast<UpdateSizeTask *>(task), rctx);
      break;
    }
  }
}
/** Execute a task */
void Monitor(u32 mode, Task *task, RunContext &rctx) override {
  switch (task->method_) {
    case Method::kConstruct: {
      MonitorConstruct(mode, reinterpret_cast<ConstructTask *>(task), rctx);
      break;
    }
    case Method::kDestruct: {
      MonitorDestruct(mode, reinterpret_cast<DestructTask *>(task), rctx);
      break;
    }
    case Method::kRegisterStager: {
      MonitorRegisterStager(mode, reinterpret_cast<RegisterStagerTask *>(task), rctx);
      break;
    }
    case Method::kUnregisterStager: {
      MonitorUnregisterStager(mode, reinterpret_cast<UnregisterStagerTask *>(task), rctx);
      break;
    }
    case Method::kStageIn: {
      MonitorStageIn(mode, reinterpret_cast<StageInTask *>(task), rctx);
      break;
    }
    case Method::kStageOut: {
      MonitorStageOut(mode, reinterpret_cast<StageOutTask *>(task), rctx);
      break;
    }
    case Method::kUpdateSize: {
      MonitorUpdateSize(mode, reinterpret_cast<UpdateSizeTask *>(task), rctx);
      break;
    }
  }
}
/** Delete a task */
void Del(u32 method, Task *task) override {
  switch (method) {
    case Method::kConstruct: {
      HRUN_CLIENT->DelTask<ConstructTask>(reinterpret_cast<ConstructTask *>(task));
      break;
    }
    case Method::kDestruct: {
      HRUN_CLIENT->DelTask<DestructTask>(reinterpret_cast<DestructTask *>(task));
      break;
    }
    case Method::kRegisterStager: {
      HRUN_CLIENT->DelTask<RegisterStagerTask>(reinterpret_cast<RegisterStagerTask *>(task));
      break;
    }
    case Method::kUnregisterStager: {
      HRUN_CLIENT->DelTask<UnregisterStagerTask>(reinterpret_cast<UnregisterStagerTask *>(task));
      break;
    }
    case Method::kStageIn: {
      HRUN_CLIENT->DelTask<StageInTask>(reinterpret_cast<StageInTask *>(task));
      break;
    }
    case Method::kStageOut: {
      HRUN_CLIENT->DelTask<StageOutTask>(reinterpret_cast<StageOutTask *>(task));
      break;
    }
    case Method::kUpdateSize: {
      HRUN_CLIENT->DelTask<UpdateSizeTask>(reinterpret_cast<UpdateSizeTask *>(task));
      break;
    }
  }
}
/** Duplicate a task */
void Dup(u32 method, Task *orig_task, std::vector<LPointer<Task>> &dups) override {
  switch (method) {
    case Method::kConstruct: {
      hrun::CALL_DUPLICATE(reinterpret_cast<ConstructTask*>(orig_task), dups);
      break;
    }
    case Method::kDestruct: {
      hrun::CALL_DUPLICATE(reinterpret_cast<DestructTask*>(orig_task), dups);
      break;
    }
    case Method::kRegisterStager: {
      hrun::CALL_DUPLICATE(reinterpret_cast<RegisterStagerTask*>(orig_task), dups);
      break;
    }
    case Method::kUnregisterStager: {
      hrun::CALL_DUPLICATE(reinterpret_cast<UnregisterStagerTask*>(orig_task), dups);
      break;
    }
    case Method::kStageIn: {
      hrun::CALL_DUPLICATE(reinterpret_cast<StageInTask*>(orig_task), dups);
      break;
    }
    case Method::kStageOut: {
      hrun::CALL_DUPLICATE(reinterpret_cast<StageOutTask*>(orig_task), dups);
      break;
    }
    case Method::kUpdateSize: {
      hrun::CALL_DUPLICATE(reinterpret_cast<UpdateSizeTask*>(orig_task), dups);
      break;
    }
  }
}
/** Register the duplicate output with the origin task */
void DupEnd(u32 method, u32 replica, Task *orig_task, Task *dup_task) override {
  switch (method) {
    case Method::kConstruct: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<ConstructTask*>(orig_task), reinterpret_cast<ConstructTask*>(dup_task));
      break;
    }
    case Method::kDestruct: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<DestructTask*>(orig_task), reinterpret_cast<DestructTask*>(dup_task));
      break;
    }
    case Method::kRegisterStager: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<RegisterStagerTask*>(orig_task), reinterpret_cast<RegisterStagerTask*>(dup_task));
      break;
    }
    case Method::kUnregisterStager: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<UnregisterStagerTask*>(orig_task), reinterpret_cast<UnregisterStagerTask*>(dup_task));
      break;
    }
    case Method::kStageIn: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<StageInTask*>(orig_task), reinterpret_cast<StageInTask*>(dup_task));
      break;
    }
    case Method::kStageOut: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<StageOutTask*>(orig_task), reinterpret_cast<StageOutTask*>(dup_task));
      break;
    }
    case Method::kUpdateSize: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<UpdateSizeTask*>(orig_task), reinterpret_cast<UpdateSizeTask*>(dup_task));
      break;
    }
  }
}
/** Ensure there is space to store replicated outputs */
void ReplicateStart(u32 method, u32 count, Task *task) override {
  switch (method) {
    case Method::kConstruct: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<ConstructTask*>(task));
      break;
    }
    case Method::kDestruct: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<DestructTask*>(task));
      break;
    }
    case Method::kRegisterStager: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<RegisterStagerTask*>(task));
      break;
    }
    case Method::kUnregisterStager: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<UnregisterStagerTask*>(task));
      break;
    }
    case Method::kStageIn: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<StageInTask*>(task));
      break;
    }
    case Method::kStageOut: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<StageOutTask*>(task));
      break;
    }
    case Method::kUpdateSize: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<UpdateSizeTask*>(task));
      break;
    }
  }
}
/** Determine success and handle failures */
void ReplicateEnd(u32 method, Task *task) override {
  switch (method) {
    case Method::kConstruct: {
      hrun::CALL_REPLICA_END(reinterpret_cast<ConstructTask*>(task));
      break;
    }
    case Method::kDestruct: {
      hrun::CALL_REPLICA_END(reinterpret_cast<DestructTask*>(task));
      break;
    }
    case Method::kRegisterStager: {
      hrun::CALL_REPLICA_END(reinterpret_cast<RegisterStagerTask*>(task));
      break;
    }
    case Method::kUnregisterStager: {
      hrun::CALL_REPLICA_END(reinterpret_cast<UnregisterStagerTask*>(task));
      break;
    }
    case Method::kStageIn: {
      hrun::CALL_REPLICA_END(reinterpret_cast<StageInTask*>(task));
      break;
    }
    case Method::kStageOut: {
      hrun::CALL_REPLICA_END(reinterpret_cast<StageOutTask*>(task));
      break;
    }
    case Method::kUpdateSize: {
      hrun::CALL_REPLICA_END(reinterpret_cast<UpdateSizeTask*>(task));
      break;
    }
  }
}
/** Serialize a task when initially pushing into remote */
std::vector<DataTransfer> SaveStart(u32 method, BinaryOutputArchive<true> &ar, Task *task) override {
  switch (method) {
    case Method::kConstruct: {
      ar << *reinterpret_cast<ConstructTask*>(task);
      break;
    }
    case Method::kDestruct: {
      ar << *reinterpret_cast<DestructTask*>(task);
      break;
    }
    case Method::kRegisterStager: {
      ar << *reinterpret_cast<RegisterStagerTask*>(task);
      break;
    }
    case Method::kUnregisterStager: {
      ar << *reinterpret_cast<UnregisterStagerTask*>(task);
      break;
    }
    case Method::kStageIn: {
      ar << *reinterpret_cast<StageInTask*>(task);
      break;
    }
    case Method::kStageOut: {
      ar << *reinterpret_cast<StageOutTask*>(task);
      break;
    }
    case Method::kUpdateSize: {
      ar << *reinterpret_cast<UpdateSizeTask*>(task);
      break;
    }
  }
  return ar.Get();
}
/** Deserialize a task when popping from remote queue */
TaskPointer LoadStart(u32 method, BinaryInputArchive<true> &ar) override {
  TaskPointer task_ptr;
  switch (method) {
    case Method::kConstruct: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<ConstructTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<ConstructTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kDestruct: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<DestructTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<DestructTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kRegisterStager: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<RegisterStagerTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<RegisterStagerTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kUnregisterStager: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<UnregisterStagerTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<UnregisterStagerTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kStageIn: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<StageInTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<StageInTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kStageOut: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<StageOutTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<StageOutTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kUpdateSize: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<UpdateSizeTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<UpdateSizeTask*>(task_ptr.ptr_);
      break;
    }
  }
  return task_ptr;
}
/** Serialize a task when returning from remote queue */
std::vector<DataTransfer> SaveEnd(u32 method, BinaryOutputArchive<false> &ar, Task *task) override {
  switch (method) {
    case Method::kConstruct: {
      ar << *reinterpret_cast<ConstructTask*>(task);
      break;
    }
    case Method::kDestruct: {
      ar << *reinterpret_cast<DestructTask*>(task);
      break;
    }
    case Method::kRegisterStager: {
      ar << *reinterpret_cast<RegisterStagerTask*>(task);
      break;
    }
    case Method::kUnregisterStager: {
      ar << *reinterpret_cast<UnregisterStagerTask*>(task);
      break;
    }
    case Method::kStageIn: {
      ar << *reinterpret_cast<StageInTask*>(task);
      break;
    }
    case Method::kStageOut: {
      ar << *reinterpret_cast<StageOutTask*>(task);
      break;
    }
    case Method::kUpdateSize: {
      ar << *reinterpret_cast<UpdateSizeTask*>(task);
      break;
    }
  }
  return ar.Get();
}
/** Deserialize a task when returning from remote queue */
void LoadEnd(u32 replica, u32 method, BinaryInputArchive<false> &ar, Task *task) override {
  switch (method) {
    case Method::kConstruct: {
      ar.Deserialize(replica, *reinterpret_cast<ConstructTask*>(task));
      break;
    }
    case Method::kDestruct: {
      ar.Deserialize(replica, *reinterpret_cast<DestructTask*>(task));
      break;
    }
    case Method::kRegisterStager: {
      ar.Deserialize(replica, *reinterpret_cast<RegisterStagerTask*>(task));
      break;
    }
    case Method::kUnregisterStager: {
      ar.Deserialize(replica, *reinterpret_cast<UnregisterStagerTask*>(task));
      break;
    }
    case Method::kStageIn: {
      ar.Deserialize(replica, *reinterpret_cast<StageInTask*>(task));
      break;
    }
    case Method::kStageOut: {
      ar.Deserialize(replica, *reinterpret_cast<StageOutTask*>(task));
      break;
    }
    case Method::kUpdateSize: {
      ar.Deserialize(replica, *reinterpret_cast<UpdateSizeTask*>(task));
      break;
    }
  }
}
/** Get the grouping of the task */
u32 GetGroup(u32 method, Task *task, hshm::charbuf &group) override {
  switch (method) {
    case Method::kConstruct: {
      return reinterpret_cast<ConstructTask*>(task)->GetGroup(group);
    }
    case Method::kDestruct: {
      return reinterpret_cast<DestructTask*>(task)->GetGroup(group);
    }
    case Method::kRegisterStager: {
      return reinterpret_cast<RegisterStagerTask*>(task)->GetGroup(group);
    }
    case Method::kUnregisterStager: {
      return reinterpret_cast<UnregisterStagerTask*>(task)->GetGroup(group);
    }
    case Method::kStageIn: {
      return reinterpret_cast<StageInTask*>(task)->GetGroup(group);
    }
    case Method::kStageOut: {
      return reinterpret_cast<StageOutTask*>(task)->GetGroup(group);
    }
    case Method::kUpdateSize: {
      return reinterpret_cast<UpdateSizeTask*>(task)->GetGroup(group);
    }
  }
  return -1;
}

#endif  // HRUN_DATA_STAGER_METHODS_H_