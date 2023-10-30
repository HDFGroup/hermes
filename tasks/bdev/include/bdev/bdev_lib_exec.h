#ifndef HRUN_BDEV_LIB_EXEC_H_
#define HRUN_BDEV_LIB_EXEC_H_

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
    case Method::kWrite: {
      Write(reinterpret_cast<WriteTask *>(task), rctx);
      break;
    }
    case Method::kRead: {
      Read(reinterpret_cast<ReadTask *>(task), rctx);
      break;
    }
    case Method::kAllocate: {
      Allocate(reinterpret_cast<AllocateTask *>(task), rctx);
      break;
    }
    case Method::kFree: {
      Free(reinterpret_cast<FreeTask *>(task), rctx);
      break;
    }
    case Method::kStatBdev: {
      StatBdev(reinterpret_cast<StatBdevTask *>(task), rctx);
      break;
    }
    case Method::kUpdateScore: {
      UpdateScore(reinterpret_cast<UpdateScoreTask *>(task), rctx);
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
    case Method::kWrite: {
      MonitorWrite(mode, reinterpret_cast<WriteTask *>(task), rctx);
      break;
    }
    case Method::kRead: {
      MonitorRead(mode, reinterpret_cast<ReadTask *>(task), rctx);
      break;
    }
    case Method::kAllocate: {
      MonitorAllocate(mode, reinterpret_cast<AllocateTask *>(task), rctx);
      break;
    }
    case Method::kFree: {
      MonitorFree(mode, reinterpret_cast<FreeTask *>(task), rctx);
      break;
    }
    case Method::kStatBdev: {
      MonitorStatBdev(mode, reinterpret_cast<StatBdevTask *>(task), rctx);
      break;
    }
    case Method::kUpdateScore: {
      MonitorUpdateScore(mode, reinterpret_cast<UpdateScoreTask *>(task), rctx);
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
    case Method::kWrite: {
      HRUN_CLIENT->DelTask<WriteTask>(reinterpret_cast<WriteTask *>(task));
      break;
    }
    case Method::kRead: {
      HRUN_CLIENT->DelTask<ReadTask>(reinterpret_cast<ReadTask *>(task));
      break;
    }
    case Method::kAllocate: {
      HRUN_CLIENT->DelTask<AllocateTask>(reinterpret_cast<AllocateTask *>(task));
      break;
    }
    case Method::kFree: {
      HRUN_CLIENT->DelTask<FreeTask>(reinterpret_cast<FreeTask *>(task));
      break;
    }
    case Method::kStatBdev: {
      HRUN_CLIENT->DelTask<StatBdevTask>(reinterpret_cast<StatBdevTask *>(task));
      break;
    }
    case Method::kUpdateScore: {
      HRUN_CLIENT->DelTask<UpdateScoreTask>(reinterpret_cast<UpdateScoreTask *>(task));
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
    case Method::kWrite: {
      hrun::CALL_DUPLICATE(reinterpret_cast<WriteTask*>(orig_task), dups);
      break;
    }
    case Method::kRead: {
      hrun::CALL_DUPLICATE(reinterpret_cast<ReadTask*>(orig_task), dups);
      break;
    }
    case Method::kAllocate: {
      hrun::CALL_DUPLICATE(reinterpret_cast<AllocateTask*>(orig_task), dups);
      break;
    }
    case Method::kFree: {
      hrun::CALL_DUPLICATE(reinterpret_cast<FreeTask*>(orig_task), dups);
      break;
    }
    case Method::kStatBdev: {
      hrun::CALL_DUPLICATE(reinterpret_cast<StatBdevTask*>(orig_task), dups);
      break;
    }
    case Method::kUpdateScore: {
      hrun::CALL_DUPLICATE(reinterpret_cast<UpdateScoreTask*>(orig_task), dups);
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
    case Method::kWrite: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<WriteTask*>(orig_task), reinterpret_cast<WriteTask*>(dup_task));
      break;
    }
    case Method::kRead: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<ReadTask*>(orig_task), reinterpret_cast<ReadTask*>(dup_task));
      break;
    }
    case Method::kAllocate: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<AllocateTask*>(orig_task), reinterpret_cast<AllocateTask*>(dup_task));
      break;
    }
    case Method::kFree: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<FreeTask*>(orig_task), reinterpret_cast<FreeTask*>(dup_task));
      break;
    }
    case Method::kStatBdev: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<StatBdevTask*>(orig_task), reinterpret_cast<StatBdevTask*>(dup_task));
      break;
    }
    case Method::kUpdateScore: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<UpdateScoreTask*>(orig_task), reinterpret_cast<UpdateScoreTask*>(dup_task));
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
    case Method::kWrite: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<WriteTask*>(task));
      break;
    }
    case Method::kRead: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<ReadTask*>(task));
      break;
    }
    case Method::kAllocate: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<AllocateTask*>(task));
      break;
    }
    case Method::kFree: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<FreeTask*>(task));
      break;
    }
    case Method::kStatBdev: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<StatBdevTask*>(task));
      break;
    }
    case Method::kUpdateScore: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<UpdateScoreTask*>(task));
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
    case Method::kWrite: {
      hrun::CALL_REPLICA_END(reinterpret_cast<WriteTask*>(task));
      break;
    }
    case Method::kRead: {
      hrun::CALL_REPLICA_END(reinterpret_cast<ReadTask*>(task));
      break;
    }
    case Method::kAllocate: {
      hrun::CALL_REPLICA_END(reinterpret_cast<AllocateTask*>(task));
      break;
    }
    case Method::kFree: {
      hrun::CALL_REPLICA_END(reinterpret_cast<FreeTask*>(task));
      break;
    }
    case Method::kStatBdev: {
      hrun::CALL_REPLICA_END(reinterpret_cast<StatBdevTask*>(task));
      break;
    }
    case Method::kUpdateScore: {
      hrun::CALL_REPLICA_END(reinterpret_cast<UpdateScoreTask*>(task));
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
    case Method::kWrite: {
      ar << *reinterpret_cast<WriteTask*>(task);
      break;
    }
    case Method::kRead: {
      ar << *reinterpret_cast<ReadTask*>(task);
      break;
    }
    case Method::kAllocate: {
      ar << *reinterpret_cast<AllocateTask*>(task);
      break;
    }
    case Method::kFree: {
      ar << *reinterpret_cast<FreeTask*>(task);
      break;
    }
    case Method::kStatBdev: {
      ar << *reinterpret_cast<StatBdevTask*>(task);
      break;
    }
    case Method::kUpdateScore: {
      ar << *reinterpret_cast<UpdateScoreTask*>(task);
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
    case Method::kWrite: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<WriteTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<WriteTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kRead: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<ReadTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<ReadTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kAllocate: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<AllocateTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<AllocateTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kFree: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<FreeTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<FreeTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kStatBdev: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<StatBdevTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<StatBdevTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kUpdateScore: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<UpdateScoreTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<UpdateScoreTask*>(task_ptr.ptr_);
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
    case Method::kWrite: {
      ar << *reinterpret_cast<WriteTask*>(task);
      break;
    }
    case Method::kRead: {
      ar << *reinterpret_cast<ReadTask*>(task);
      break;
    }
    case Method::kAllocate: {
      ar << *reinterpret_cast<AllocateTask*>(task);
      break;
    }
    case Method::kFree: {
      ar << *reinterpret_cast<FreeTask*>(task);
      break;
    }
    case Method::kStatBdev: {
      ar << *reinterpret_cast<StatBdevTask*>(task);
      break;
    }
    case Method::kUpdateScore: {
      ar << *reinterpret_cast<UpdateScoreTask*>(task);
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
    case Method::kWrite: {
      ar.Deserialize(replica, *reinterpret_cast<WriteTask*>(task));
      break;
    }
    case Method::kRead: {
      ar.Deserialize(replica, *reinterpret_cast<ReadTask*>(task));
      break;
    }
    case Method::kAllocate: {
      ar.Deserialize(replica, *reinterpret_cast<AllocateTask*>(task));
      break;
    }
    case Method::kFree: {
      ar.Deserialize(replica, *reinterpret_cast<FreeTask*>(task));
      break;
    }
    case Method::kStatBdev: {
      ar.Deserialize(replica, *reinterpret_cast<StatBdevTask*>(task));
      break;
    }
    case Method::kUpdateScore: {
      ar.Deserialize(replica, *reinterpret_cast<UpdateScoreTask*>(task));
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
    case Method::kWrite: {
      return reinterpret_cast<WriteTask*>(task)->GetGroup(group);
    }
    case Method::kRead: {
      return reinterpret_cast<ReadTask*>(task)->GetGroup(group);
    }
    case Method::kAllocate: {
      return reinterpret_cast<AllocateTask*>(task)->GetGroup(group);
    }
    case Method::kFree: {
      return reinterpret_cast<FreeTask*>(task)->GetGroup(group);
    }
    case Method::kStatBdev: {
      return reinterpret_cast<StatBdevTask*>(task)->GetGroup(group);
    }
    case Method::kUpdateScore: {
      return reinterpret_cast<UpdateScoreTask*>(task)->GetGroup(group);
    }
  }
  return -1;
}

#endif  // HRUN_BDEV_METHODS_H_