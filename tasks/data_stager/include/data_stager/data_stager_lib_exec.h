#ifndef LABSTOR_DATA_STAGER_LIB_EXEC_H_
#define LABSTOR_DATA_STAGER_LIB_EXEC_H_

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
  }
}
/** Delete a task */
void Del(u32 method, Task *task) override {
  switch (method) {
    case Method::kConstruct: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<ConstructTask *>(task));
      break;
    }
    case Method::kDestruct: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<DestructTask *>(task));
      break;
    }
    case Method::kRegisterStager: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<RegisterStagerTask *>(task));
      break;
    }
    case Method::kUnregisterStager: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<UnregisterStagerTask *>(task));
      break;
    }
    case Method::kStageIn: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<StageInTask *>(task));
      break;
    }
    case Method::kStageOut: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<StageOutTask *>(task));
      break;
    }
  }
}
/** Ensure there is space to store replicated outputs */
void ReplicateStart(u32 method, u32 count, Task *task) override {
  switch (method) {
    case Method::kConstruct: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<ConstructTask*>(task));
      break;
    }
    case Method::kDestruct: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<DestructTask*>(task));
      break;
    }
    case Method::kRegisterStager: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<RegisterStagerTask*>(task));
      break;
    }
    case Method::kUnregisterStager: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<UnregisterStagerTask*>(task));
      break;
    }
    case Method::kStageIn: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<StageInTask*>(task));
      break;
    }
    case Method::kStageOut: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<StageOutTask*>(task));
      break;
    }
  }
}
/** Determine success and handle failures */
void ReplicateEnd(u32 method, Task *task) override {
  switch (method) {
    case Method::kConstruct: {
      labstor::CALL_REPLICA_END(reinterpret_cast<ConstructTask*>(task));
      break;
    }
    case Method::kDestruct: {
      labstor::CALL_REPLICA_END(reinterpret_cast<DestructTask*>(task));
      break;
    }
    case Method::kRegisterStager: {
      labstor::CALL_REPLICA_END(reinterpret_cast<RegisterStagerTask*>(task));
      break;
    }
    case Method::kUnregisterStager: {
      labstor::CALL_REPLICA_END(reinterpret_cast<UnregisterStagerTask*>(task));
      break;
    }
    case Method::kStageIn: {
      labstor::CALL_REPLICA_END(reinterpret_cast<StageInTask*>(task));
      break;
    }
    case Method::kStageOut: {
      labstor::CALL_REPLICA_END(reinterpret_cast<StageOutTask*>(task));
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
  }
  return ar.Get();
}
/** Deserialize a task when popping from remote queue */
TaskPointer LoadStart(u32 method, BinaryInputArchive<true> &ar) override {
  TaskPointer task_ptr;
  switch (method) {
    case Method::kConstruct: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<ConstructTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<ConstructTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kDestruct: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<DestructTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<DestructTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kRegisterStager: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<RegisterStagerTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<RegisterStagerTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kUnregisterStager: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<UnregisterStagerTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<UnregisterStagerTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kStageIn: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<StageInTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<StageInTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kStageOut: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<StageOutTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<StageOutTask*>(task_ptr.ptr_);
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
  }
  return -1;
}

#endif  // LABSTOR_DATA_STAGER_METHODS_H_