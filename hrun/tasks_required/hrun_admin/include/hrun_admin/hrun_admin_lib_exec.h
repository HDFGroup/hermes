#ifndef HRUN_HRUN_ADMIN_LIB_EXEC_H_
#define HRUN_HRUN_ADMIN_LIB_EXEC_H_

/** Execute a task */
void Run(u32 method, Task *task, RunContext &rctx) override {
  switch (method) {
    case Method::kCreateTaskState: {
      CreateTaskState(reinterpret_cast<CreateTaskStateTask *>(task), rctx);
      break;
    }
    case Method::kDestroyTaskState: {
      DestroyTaskState(reinterpret_cast<DestroyTaskStateTask *>(task), rctx);
      break;
    }
    case Method::kRegisterTaskLib: {
      RegisterTaskLib(reinterpret_cast<RegisterTaskLibTask *>(task), rctx);
      break;
    }
    case Method::kDestroyTaskLib: {
      DestroyTaskLib(reinterpret_cast<DestroyTaskLibTask *>(task), rctx);
      break;
    }
    case Method::kGetOrCreateTaskStateId: {
      GetOrCreateTaskStateId(reinterpret_cast<GetOrCreateTaskStateIdTask *>(task), rctx);
      break;
    }
    case Method::kGetTaskStateId: {
      GetTaskStateId(reinterpret_cast<GetTaskStateIdTask *>(task), rctx);
      break;
    }
    case Method::kStopRuntime: {
      StopRuntime(reinterpret_cast<StopRuntimeTask *>(task), rctx);
      break;
    }
    case Method::kSetWorkOrchQueuePolicy: {
      SetWorkOrchQueuePolicy(reinterpret_cast<SetWorkOrchQueuePolicyTask *>(task), rctx);
      break;
    }
    case Method::kSetWorkOrchProcPolicy: {
      SetWorkOrchProcPolicy(reinterpret_cast<SetWorkOrchProcPolicyTask *>(task), rctx);
      break;
    }
    case Method::kFlush: {
      Flush(reinterpret_cast<FlushTask *>(task), rctx);
      break;
    }
  }
}
/** Execute a task */
void Monitor(u32 mode, Task *task, RunContext &rctx) override {
  switch (task->method_) {
    case Method::kCreateTaskState: {
      MonitorCreateTaskState(mode, reinterpret_cast<CreateTaskStateTask *>(task), rctx);
      break;
    }
    case Method::kDestroyTaskState: {
      MonitorDestroyTaskState(mode, reinterpret_cast<DestroyTaskStateTask *>(task), rctx);
      break;
    }
    case Method::kRegisterTaskLib: {
      MonitorRegisterTaskLib(mode, reinterpret_cast<RegisterTaskLibTask *>(task), rctx);
      break;
    }
    case Method::kDestroyTaskLib: {
      MonitorDestroyTaskLib(mode, reinterpret_cast<DestroyTaskLibTask *>(task), rctx);
      break;
    }
    case Method::kGetOrCreateTaskStateId: {
      MonitorGetOrCreateTaskStateId(mode, reinterpret_cast<GetOrCreateTaskStateIdTask *>(task), rctx);
      break;
    }
    case Method::kGetTaskStateId: {
      MonitorGetTaskStateId(mode, reinterpret_cast<GetTaskStateIdTask *>(task), rctx);
      break;
    }
    case Method::kStopRuntime: {
      MonitorStopRuntime(mode, reinterpret_cast<StopRuntimeTask *>(task), rctx);
      break;
    }
    case Method::kSetWorkOrchQueuePolicy: {
      MonitorSetWorkOrchQueuePolicy(mode, reinterpret_cast<SetWorkOrchQueuePolicyTask *>(task), rctx);
      break;
    }
    case Method::kSetWorkOrchProcPolicy: {
      MonitorSetWorkOrchProcPolicy(mode, reinterpret_cast<SetWorkOrchProcPolicyTask *>(task), rctx);
      break;
    }
    case Method::kFlush: {
      MonitorFlush(mode, reinterpret_cast<FlushTask *>(task), rctx);
      break;
    }
  }
}
/** Delete a task */
void Del(u32 method, Task *task) override {
  switch (method) {
    case Method::kCreateTaskState: {
      HRUN_CLIENT->DelTask<CreateTaskStateTask>(reinterpret_cast<CreateTaskStateTask *>(task));
      break;
    }
    case Method::kDestroyTaskState: {
      HRUN_CLIENT->DelTask<DestroyTaskStateTask>(reinterpret_cast<DestroyTaskStateTask *>(task));
      break;
    }
    case Method::kRegisterTaskLib: {
      HRUN_CLIENT->DelTask<RegisterTaskLibTask>(reinterpret_cast<RegisterTaskLibTask *>(task));
      break;
    }
    case Method::kDestroyTaskLib: {
      HRUN_CLIENT->DelTask<DestroyTaskLibTask>(reinterpret_cast<DestroyTaskLibTask *>(task));
      break;
    }
    case Method::kGetOrCreateTaskStateId: {
      HRUN_CLIENT->DelTask<GetOrCreateTaskStateIdTask>(reinterpret_cast<GetOrCreateTaskStateIdTask *>(task));
      break;
    }
    case Method::kGetTaskStateId: {
      HRUN_CLIENT->DelTask<GetTaskStateIdTask>(reinterpret_cast<GetTaskStateIdTask *>(task));
      break;
    }
    case Method::kStopRuntime: {
      HRUN_CLIENT->DelTask<StopRuntimeTask>(reinterpret_cast<StopRuntimeTask *>(task));
      break;
    }
    case Method::kSetWorkOrchQueuePolicy: {
      HRUN_CLIENT->DelTask<SetWorkOrchQueuePolicyTask>(reinterpret_cast<SetWorkOrchQueuePolicyTask *>(task));
      break;
    }
    case Method::kSetWorkOrchProcPolicy: {
      HRUN_CLIENT->DelTask<SetWorkOrchProcPolicyTask>(reinterpret_cast<SetWorkOrchProcPolicyTask *>(task));
      break;
    }
    case Method::kFlush: {
      HRUN_CLIENT->DelTask<FlushTask>(reinterpret_cast<FlushTask *>(task));
      break;
    }
  }
}
/** Duplicate a task */
void Dup(u32 method, Task *orig_task, std::vector<LPointer<Task>> &dups) override {
  switch (method) {
    case Method::kCreateTaskState: {
      hrun::CALL_DUPLICATE(reinterpret_cast<CreateTaskStateTask*>(orig_task), dups);
      break;
    }
    case Method::kDestroyTaskState: {
      hrun::CALL_DUPLICATE(reinterpret_cast<DestroyTaskStateTask*>(orig_task), dups);
      break;
    }
    case Method::kRegisterTaskLib: {
      hrun::CALL_DUPLICATE(reinterpret_cast<RegisterTaskLibTask*>(orig_task), dups);
      break;
    }
    case Method::kDestroyTaskLib: {
      hrun::CALL_DUPLICATE(reinterpret_cast<DestroyTaskLibTask*>(orig_task), dups);
      break;
    }
    case Method::kGetOrCreateTaskStateId: {
      hrun::CALL_DUPLICATE(reinterpret_cast<GetOrCreateTaskStateIdTask*>(orig_task), dups);
      break;
    }
    case Method::kGetTaskStateId: {
      hrun::CALL_DUPLICATE(reinterpret_cast<GetTaskStateIdTask*>(orig_task), dups);
      break;
    }
    case Method::kStopRuntime: {
      hrun::CALL_DUPLICATE(reinterpret_cast<StopRuntimeTask*>(orig_task), dups);
      break;
    }
    case Method::kSetWorkOrchQueuePolicy: {
      hrun::CALL_DUPLICATE(reinterpret_cast<SetWorkOrchQueuePolicyTask*>(orig_task), dups);
      break;
    }
    case Method::kSetWorkOrchProcPolicy: {
      hrun::CALL_DUPLICATE(reinterpret_cast<SetWorkOrchProcPolicyTask*>(orig_task), dups);
      break;
    }
    case Method::kFlush: {
      hrun::CALL_DUPLICATE(reinterpret_cast<FlushTask*>(orig_task), dups);
      break;
    }
  }
}
/** Register the duplicate output with the origin task */
void DupEnd(u32 method, u32 replica, Task *orig_task, Task *dup_task) override {
  switch (method) {
    case Method::kCreateTaskState: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<CreateTaskStateTask*>(orig_task), reinterpret_cast<CreateTaskStateTask*>(dup_task));
      break;
    }
    case Method::kDestroyTaskState: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<DestroyTaskStateTask*>(orig_task), reinterpret_cast<DestroyTaskStateTask*>(dup_task));
      break;
    }
    case Method::kRegisterTaskLib: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<RegisterTaskLibTask*>(orig_task), reinterpret_cast<RegisterTaskLibTask*>(dup_task));
      break;
    }
    case Method::kDestroyTaskLib: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<DestroyTaskLibTask*>(orig_task), reinterpret_cast<DestroyTaskLibTask*>(dup_task));
      break;
    }
    case Method::kGetOrCreateTaskStateId: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<GetOrCreateTaskStateIdTask*>(orig_task), reinterpret_cast<GetOrCreateTaskStateIdTask*>(dup_task));
      break;
    }
    case Method::kGetTaskStateId: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<GetTaskStateIdTask*>(orig_task), reinterpret_cast<GetTaskStateIdTask*>(dup_task));
      break;
    }
    case Method::kStopRuntime: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<StopRuntimeTask*>(orig_task), reinterpret_cast<StopRuntimeTask*>(dup_task));
      break;
    }
    case Method::kSetWorkOrchQueuePolicy: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<SetWorkOrchQueuePolicyTask*>(orig_task), reinterpret_cast<SetWorkOrchQueuePolicyTask*>(dup_task));
      break;
    }
    case Method::kSetWorkOrchProcPolicy: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<SetWorkOrchProcPolicyTask*>(orig_task), reinterpret_cast<SetWorkOrchProcPolicyTask*>(dup_task));
      break;
    }
    case Method::kFlush: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<FlushTask*>(orig_task), reinterpret_cast<FlushTask*>(dup_task));
      break;
    }
  }
}
/** Ensure there is space to store replicated outputs */
void ReplicateStart(u32 method, u32 count, Task *task) override {
  switch (method) {
    case Method::kCreateTaskState: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<CreateTaskStateTask*>(task));
      break;
    }
    case Method::kDestroyTaskState: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<DestroyTaskStateTask*>(task));
      break;
    }
    case Method::kRegisterTaskLib: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<RegisterTaskLibTask*>(task));
      break;
    }
    case Method::kDestroyTaskLib: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<DestroyTaskLibTask*>(task));
      break;
    }
    case Method::kGetOrCreateTaskStateId: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<GetOrCreateTaskStateIdTask*>(task));
      break;
    }
    case Method::kGetTaskStateId: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<GetTaskStateIdTask*>(task));
      break;
    }
    case Method::kStopRuntime: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<StopRuntimeTask*>(task));
      break;
    }
    case Method::kSetWorkOrchQueuePolicy: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<SetWorkOrchQueuePolicyTask*>(task));
      break;
    }
    case Method::kSetWorkOrchProcPolicy: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<SetWorkOrchProcPolicyTask*>(task));
      break;
    }
    case Method::kFlush: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<FlushTask*>(task));
      break;
    }
  }
}
/** Determine success and handle failures */
void ReplicateEnd(u32 method, Task *task) override {
  switch (method) {
    case Method::kCreateTaskState: {
      hrun::CALL_REPLICA_END(reinterpret_cast<CreateTaskStateTask*>(task));
      break;
    }
    case Method::kDestroyTaskState: {
      hrun::CALL_REPLICA_END(reinterpret_cast<DestroyTaskStateTask*>(task));
      break;
    }
    case Method::kRegisterTaskLib: {
      hrun::CALL_REPLICA_END(reinterpret_cast<RegisterTaskLibTask*>(task));
      break;
    }
    case Method::kDestroyTaskLib: {
      hrun::CALL_REPLICA_END(reinterpret_cast<DestroyTaskLibTask*>(task));
      break;
    }
    case Method::kGetOrCreateTaskStateId: {
      hrun::CALL_REPLICA_END(reinterpret_cast<GetOrCreateTaskStateIdTask*>(task));
      break;
    }
    case Method::kGetTaskStateId: {
      hrun::CALL_REPLICA_END(reinterpret_cast<GetTaskStateIdTask*>(task));
      break;
    }
    case Method::kStopRuntime: {
      hrun::CALL_REPLICA_END(reinterpret_cast<StopRuntimeTask*>(task));
      break;
    }
    case Method::kSetWorkOrchQueuePolicy: {
      hrun::CALL_REPLICA_END(reinterpret_cast<SetWorkOrchQueuePolicyTask*>(task));
      break;
    }
    case Method::kSetWorkOrchProcPolicy: {
      hrun::CALL_REPLICA_END(reinterpret_cast<SetWorkOrchProcPolicyTask*>(task));
      break;
    }
    case Method::kFlush: {
      hrun::CALL_REPLICA_END(reinterpret_cast<FlushTask*>(task));
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
    case Method::kFlush: {
      ar << *reinterpret_cast<FlushTask*>(task);
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
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<CreateTaskStateTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<CreateTaskStateTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kDestroyTaskState: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<DestroyTaskStateTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<DestroyTaskStateTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kRegisterTaskLib: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<RegisterTaskLibTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<RegisterTaskLibTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kDestroyTaskLib: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<DestroyTaskLibTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<DestroyTaskLibTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kGetOrCreateTaskStateId: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<GetOrCreateTaskStateIdTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetOrCreateTaskStateIdTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kGetTaskStateId: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<GetTaskStateIdTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetTaskStateIdTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kStopRuntime: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<StopRuntimeTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<StopRuntimeTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kSetWorkOrchQueuePolicy: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<SetWorkOrchQueuePolicyTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<SetWorkOrchQueuePolicyTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kSetWorkOrchProcPolicy: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<SetWorkOrchProcPolicyTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<SetWorkOrchProcPolicyTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kFlush: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<FlushTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<FlushTask*>(task_ptr.ptr_);
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
    case Method::kFlush: {
      ar << *reinterpret_cast<FlushTask*>(task);
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
    case Method::kFlush: {
      ar.Deserialize(replica, *reinterpret_cast<FlushTask*>(task));
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
    case Method::kFlush: {
      return reinterpret_cast<FlushTask*>(task)->GetGroup(group);
    }
  }
  return -1;
}

#endif  // HRUN_HRUN_ADMIN_METHODS_H_