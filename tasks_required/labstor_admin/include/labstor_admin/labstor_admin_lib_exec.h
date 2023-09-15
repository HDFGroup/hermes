#ifndef LABSTOR_LABSTOR_ADMIN_LIB_EXEC_H_
#define LABSTOR_LABSTOR_ADMIN_LIB_EXEC_H_

/** Execute a task */
void Run(u32 method, Task *task) override {
  switch (method) {
    case Method::kCreateTaskState: {
      CreateTaskState(reinterpret_cast<CreateTaskStateTask *>(task));
      break;
    }
    case Method::kDestroyTaskState: {
      DestroyTaskState(reinterpret_cast<DestroyTaskStateTask *>(task));
      break;
    }
    case Method::kRegisterTaskLib: {
      RegisterTaskLib(reinterpret_cast<RegisterTaskLibTask *>(task));
      break;
    }
    case Method::kDestroyTaskLib: {
      DestroyTaskLib(reinterpret_cast<DestroyTaskLibTask *>(task));
      break;
    }
    case Method::kGetOrCreateTaskStateId: {
      GetOrCreateTaskStateId(reinterpret_cast<GetOrCreateTaskStateIdTask *>(task));
      break;
    }
    case Method::kGetTaskStateId: {
      GetTaskStateId(reinterpret_cast<GetTaskStateIdTask *>(task));
      break;
    }
    case Method::kStopRuntime: {
      StopRuntime(reinterpret_cast<StopRuntimeTask *>(task));
      break;
    }
    case Method::kSetWorkOrchQueuePolicy: {
      SetWorkOrchQueuePolicy(reinterpret_cast<SetWorkOrchQueuePolicyTask *>(task));
      break;
    }
    case Method::kSetWorkOrchProcPolicy: {
      SetWorkOrchProcPolicy(reinterpret_cast<SetWorkOrchProcPolicyTask *>(task));
      break;
    }
  }
}
/** Ensure there is space to store replicated outputs */
void ReplicateStart(u32 method, u32 count, Task *task) override {
  switch (method) {
    case Method::kCreateTaskState: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<CreateTaskStateTask*>(task));
      break;
    }
    case Method::kDestroyTaskState: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<DestroyTaskStateTask*>(task));
      break;
    }
    case Method::kRegisterTaskLib: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<RegisterTaskLibTask*>(task));
      break;
    }
    case Method::kDestroyTaskLib: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<DestroyTaskLibTask*>(task));
      break;
    }
    case Method::kGetOrCreateTaskStateId: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<GetOrCreateTaskStateIdTask*>(task));
      break;
    }
    case Method::kGetTaskStateId: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<GetTaskStateIdTask*>(task));
      break;
    }
    case Method::kStopRuntime: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<StopRuntimeTask*>(task));
      break;
    }
    case Method::kSetWorkOrchQueuePolicy: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<SetWorkOrchQueuePolicyTask*>(task));
      break;
    }
    case Method::kSetWorkOrchProcPolicy: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<SetWorkOrchProcPolicyTask*>(task));
      break;
    }
  }
}
/** Determine success and handle failures */
void ReplicateEnd(u32 method, Task *task) override {
  switch (method) {
    case Method::kCreateTaskState: {
      labstor::CALL_REPLICA_END(reinterpret_cast<CreateTaskStateTask*>(task));
      break;
    }
    case Method::kDestroyTaskState: {
      labstor::CALL_REPLICA_END(reinterpret_cast<DestroyTaskStateTask*>(task));
      break;
    }
    case Method::kRegisterTaskLib: {
      labstor::CALL_REPLICA_END(reinterpret_cast<RegisterTaskLibTask*>(task));
      break;
    }
    case Method::kDestroyTaskLib: {
      labstor::CALL_REPLICA_END(reinterpret_cast<DestroyTaskLibTask*>(task));
      break;
    }
    case Method::kGetOrCreateTaskStateId: {
      labstor::CALL_REPLICA_END(reinterpret_cast<GetOrCreateTaskStateIdTask*>(task));
      break;
    }
    case Method::kGetTaskStateId: {
      labstor::CALL_REPLICA_END(reinterpret_cast<GetTaskStateIdTask*>(task));
      break;
    }
    case Method::kStopRuntime: {
      labstor::CALL_REPLICA_END(reinterpret_cast<StopRuntimeTask*>(task));
      break;
    }
    case Method::kSetWorkOrchQueuePolicy: {
      labstor::CALL_REPLICA_END(reinterpret_cast<SetWorkOrchQueuePolicyTask*>(task));
      break;
    }
    case Method::kSetWorkOrchProcPolicy: {
      labstor::CALL_REPLICA_END(reinterpret_cast<SetWorkOrchProcPolicyTask*>(task));
      break;
    }
  }
}
/** Serialize a task when initially pushing into remote */
std::vector<DataTransfer> SaveStart(u32 method, BinaryOutputArchive<true> &ar, Task *task) override {
  switch (method) {
    case Method::kCreateTaskState: {
      ar << *reinterpret_cast<CreateTaskStateTask*>(task);
      break;
    }
    case Method::kDestroyTaskState: {
      ar << *reinterpret_cast<DestroyTaskStateTask*>(task);
      break;
    }
    case Method::kRegisterTaskLib: {
      ar << *reinterpret_cast<RegisterTaskLibTask*>(task);
      break;
    }
    case Method::kDestroyTaskLib: {
      ar << *reinterpret_cast<DestroyTaskLibTask*>(task);
      break;
    }
    case Method::kGetOrCreateTaskStateId: {
      ar << *reinterpret_cast<GetOrCreateTaskStateIdTask*>(task);
      break;
    }
    case Method::kGetTaskStateId: {
      ar << *reinterpret_cast<GetTaskStateIdTask*>(task);
      break;
    }
    case Method::kStopRuntime: {
      ar << *reinterpret_cast<StopRuntimeTask*>(task);
      break;
    }
    case Method::kSetWorkOrchQueuePolicy: {
      ar << *reinterpret_cast<SetWorkOrchQueuePolicyTask*>(task);
      break;
    }
    case Method::kSetWorkOrchProcPolicy: {
      ar << *reinterpret_cast<SetWorkOrchProcPolicyTask*>(task);
      break;
    }
  }
  return ar.Get();
}
/** Deserialize a task when popping from remote queue */
TaskPointer LoadStart(u32 method, BinaryInputArchive<true> &ar) override {
  TaskPointer task_ptr;
  switch (method) {
    case Method::kCreateTaskState: {
      task_ptr.task_ = LABSTOR_CLIENT->NewEmptyTask<CreateTaskStateTask>(task_ptr.p_);
      ar >> *reinterpret_cast<CreateTaskStateTask*>(task_ptr.task_);
      break;
    }
    case Method::kDestroyTaskState: {
      task_ptr.task_ = LABSTOR_CLIENT->NewEmptyTask<DestroyTaskStateTask>(task_ptr.p_);
      ar >> *reinterpret_cast<DestroyTaskStateTask*>(task_ptr.task_);
      break;
    }
    case Method::kRegisterTaskLib: {
      task_ptr.task_ = LABSTOR_CLIENT->NewEmptyTask<RegisterTaskLibTask>(task_ptr.p_);
      ar >> *reinterpret_cast<RegisterTaskLibTask*>(task_ptr.task_);
      break;
    }
    case Method::kDestroyTaskLib: {
      task_ptr.task_ = LABSTOR_CLIENT->NewEmptyTask<DestroyTaskLibTask>(task_ptr.p_);
      ar >> *reinterpret_cast<DestroyTaskLibTask*>(task_ptr.task_);
      break;
    }
    case Method::kGetOrCreateTaskStateId: {
      task_ptr.task_ = LABSTOR_CLIENT->NewEmptyTask<GetOrCreateTaskStateIdTask>(task_ptr.p_);
      ar >> *reinterpret_cast<GetOrCreateTaskStateIdTask*>(task_ptr.task_);
      break;
    }
    case Method::kGetTaskStateId: {
      task_ptr.task_ = LABSTOR_CLIENT->NewEmptyTask<GetTaskStateIdTask>(task_ptr.p_);
      ar >> *reinterpret_cast<GetTaskStateIdTask*>(task_ptr.task_);
      break;
    }
    case Method::kStopRuntime: {
      task_ptr.task_ = LABSTOR_CLIENT->NewEmptyTask<StopRuntimeTask>(task_ptr.p_);
      ar >> *reinterpret_cast<StopRuntimeTask*>(task_ptr.task_);
      break;
    }
    case Method::kSetWorkOrchQueuePolicy: {
      task_ptr.task_ = LABSTOR_CLIENT->NewEmptyTask<SetWorkOrchQueuePolicyTask>(task_ptr.p_);
      ar >> *reinterpret_cast<SetWorkOrchQueuePolicyTask*>(task_ptr.task_);
      break;
    }
    case Method::kSetWorkOrchProcPolicy: {
      task_ptr.task_ = LABSTOR_CLIENT->NewEmptyTask<SetWorkOrchProcPolicyTask>(task_ptr.p_);
      ar >> *reinterpret_cast<SetWorkOrchProcPolicyTask*>(task_ptr.task_);
      break;
    }
  }
  return task_ptr;
}
/** Serialize a task when returning from remote queue */
std::vector<DataTransfer> SaveEnd(u32 method, BinaryOutputArchive<false> &ar, Task *task) override {
  switch (method) {
    case Method::kCreateTaskState: {
      ar << *reinterpret_cast<CreateTaskStateTask*>(task);
      break;
    }
    case Method::kDestroyTaskState: {
      ar << *reinterpret_cast<DestroyTaskStateTask*>(task);
      break;
    }
    case Method::kRegisterTaskLib: {
      ar << *reinterpret_cast<RegisterTaskLibTask*>(task);
      break;
    }
    case Method::kDestroyTaskLib: {
      ar << *reinterpret_cast<DestroyTaskLibTask*>(task);
      break;
    }
    case Method::kGetOrCreateTaskStateId: {
      ar << *reinterpret_cast<GetOrCreateTaskStateIdTask*>(task);
      break;
    }
    case Method::kGetTaskStateId: {
      ar << *reinterpret_cast<GetTaskStateIdTask*>(task);
      break;
    }
    case Method::kStopRuntime: {
      ar << *reinterpret_cast<StopRuntimeTask*>(task);
      break;
    }
    case Method::kSetWorkOrchQueuePolicy: {
      ar << *reinterpret_cast<SetWorkOrchQueuePolicyTask*>(task);
      break;
    }
    case Method::kSetWorkOrchProcPolicy: {
      ar << *reinterpret_cast<SetWorkOrchProcPolicyTask*>(task);
      break;
    }
  }
  return ar.Get();
}
/** Deserialize a task when returning from remote queue */
void LoadEnd(u32 replica, u32 method, BinaryInputArchive<false> &ar, Task *task) override {
  switch (method) {
    case Method::kCreateTaskState: {
      ar.Deserialize(replica, *reinterpret_cast<CreateTaskStateTask*>(task));
      break;
    }
    case Method::kDestroyTaskState: {
      ar.Deserialize(replica, *reinterpret_cast<DestroyTaskStateTask*>(task));
      break;
    }
    case Method::kRegisterTaskLib: {
      ar.Deserialize(replica, *reinterpret_cast<RegisterTaskLibTask*>(task));
      break;
    }
    case Method::kDestroyTaskLib: {
      ar.Deserialize(replica, *reinterpret_cast<DestroyTaskLibTask*>(task));
      break;
    }
    case Method::kGetOrCreateTaskStateId: {
      ar.Deserialize(replica, *reinterpret_cast<GetOrCreateTaskStateIdTask*>(task));
      break;
    }
    case Method::kGetTaskStateId: {
      ar.Deserialize(replica, *reinterpret_cast<GetTaskStateIdTask*>(task));
      break;
    }
    case Method::kStopRuntime: {
      ar.Deserialize(replica, *reinterpret_cast<StopRuntimeTask*>(task));
      break;
    }
    case Method::kSetWorkOrchQueuePolicy: {
      ar.Deserialize(replica, *reinterpret_cast<SetWorkOrchQueuePolicyTask*>(task));
      break;
    }
    case Method::kSetWorkOrchProcPolicy: {
      ar.Deserialize(replica, *reinterpret_cast<SetWorkOrchProcPolicyTask*>(task));
      break;
    }
  }
}
/** Get the grouping of the task */
u32 GetGroup(u32 method, Task *task, hshm::charbuf &group) override {
  switch (method) {
    case Method::kCreateTaskState: {
      return reinterpret_cast<CreateTaskStateTask*>(task)->GetGroup(group);
    }
    case Method::kDestroyTaskState: {
      return reinterpret_cast<DestroyTaskStateTask*>(task)->GetGroup(group);
    }
    case Method::kRegisterTaskLib: {
      return reinterpret_cast<RegisterTaskLibTask*>(task)->GetGroup(group);
    }
    case Method::kDestroyTaskLib: {
      return reinterpret_cast<DestroyTaskLibTask*>(task)->GetGroup(group);
    }
    case Method::kGetOrCreateTaskStateId: {
      return reinterpret_cast<GetOrCreateTaskStateIdTask*>(task)->GetGroup(group);
    }
    case Method::kGetTaskStateId: {
      return reinterpret_cast<GetTaskStateIdTask*>(task)->GetGroup(group);
    }
    case Method::kStopRuntime: {
      return reinterpret_cast<StopRuntimeTask*>(task)->GetGroup(group);
    }
    case Method::kSetWorkOrchQueuePolicy: {
      return reinterpret_cast<SetWorkOrchQueuePolicyTask*>(task)->GetGroup(group);
    }
    case Method::kSetWorkOrchProcPolicy: {
      return reinterpret_cast<SetWorkOrchProcPolicyTask*>(task)->GetGroup(group);
    }
  }
  return -1;
}

#endif  // LABSTOR_LABSTOR_ADMIN_METHODS_H_