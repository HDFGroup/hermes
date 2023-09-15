#ifndef LABSTOR_BDEV_LIB_EXEC_H_
#define LABSTOR_BDEV_LIB_EXEC_H_

/** Execute a task */
void Run(u32 method, Task *task) override {
  switch (method) {
    case Method::kConstruct: {
      Construct(reinterpret_cast<ConstructTask *>(task));
      break;
    }
    case Method::kDestruct: {
      Destruct(reinterpret_cast<DestructTask *>(task));
      break;
    }
    case Method::kWrite: {
      Write(reinterpret_cast<WriteTask *>(task));
      break;
    }
    case Method::kRead: {
      Read(reinterpret_cast<ReadTask *>(task));
      break;
    }
    case Method::kAlloc: {
      Alloc(reinterpret_cast<AllocateTask *>(task));
      break;
    }
    case Method::kFree: {
      Free(reinterpret_cast<FreeTask *>(task));
      break;
    }
    case Method::kMonitor: {
      Monitor(reinterpret_cast<MonitorTask *>(task));
      break;
    }
    case Method::kUpdateCapacity: {
      UpdateCapacity(reinterpret_cast<UpdateCapacityTask *>(task));
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
    case Method::kWrite: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<WriteTask*>(task));
      break;
    }
    case Method::kRead: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<ReadTask*>(task));
      break;
    }
    case Method::kAlloc: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<AllocateTask*>(task));
      break;
    }
    case Method::kFree: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<FreeTask*>(task));
      break;
    }
    case Method::kMonitor: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<MonitorTask*>(task));
      break;
    }
    case Method::kUpdateCapacity: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<UpdateCapacityTask*>(task));
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
    case Method::kWrite: {
      labstor::CALL_REPLICA_END(reinterpret_cast<WriteTask*>(task));
      break;
    }
    case Method::kRead: {
      labstor::CALL_REPLICA_END(reinterpret_cast<ReadTask*>(task));
      break;
    }
    case Method::kAlloc: {
      labstor::CALL_REPLICA_END(reinterpret_cast<AllocateTask*>(task));
      break;
    }
    case Method::kFree: {
      labstor::CALL_REPLICA_END(reinterpret_cast<FreeTask*>(task));
      break;
    }
    case Method::kMonitor: {
      labstor::CALL_REPLICA_END(reinterpret_cast<MonitorTask*>(task));
      break;
    }
    case Method::kUpdateCapacity: {
      labstor::CALL_REPLICA_END(reinterpret_cast<UpdateCapacityTask*>(task));
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
    case Method::kAlloc: {
      ar << *reinterpret_cast<AllocateTask*>(task);
      break;
    }
    case Method::kFree: {
      ar << *reinterpret_cast<FreeTask*>(task);
      break;
    }
    case Method::kMonitor: {
      ar << *reinterpret_cast<MonitorTask*>(task);
      break;
    }
    case Method::kUpdateCapacity: {
      ar << *reinterpret_cast<UpdateCapacityTask*>(task);
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
      task_ptr.task_ = LABSTOR_CLIENT->NewEmptyTask<ConstructTask>(task_ptr.p_);
      ar >> *reinterpret_cast<ConstructTask*>(task_ptr.task_);
      break;
    }
    case Method::kDestruct: {
      task_ptr.task_ = LABSTOR_CLIENT->NewEmptyTask<DestructTask>(task_ptr.p_);
      ar >> *reinterpret_cast<DestructTask*>(task_ptr.task_);
      break;
    }
    case Method::kWrite: {
      task_ptr.task_ = LABSTOR_CLIENT->NewEmptyTask<WriteTask>(task_ptr.p_);
      ar >> *reinterpret_cast<WriteTask*>(task_ptr.task_);
      break;
    }
    case Method::kRead: {
      task_ptr.task_ = LABSTOR_CLIENT->NewEmptyTask<ReadTask>(task_ptr.p_);
      ar >> *reinterpret_cast<ReadTask*>(task_ptr.task_);
      break;
    }
    case Method::kAlloc: {
      task_ptr.task_ = LABSTOR_CLIENT->NewEmptyTask<AllocateTask>(task_ptr.p_);
      ar >> *reinterpret_cast<AllocateTask*>(task_ptr.task_);
      break;
    }
    case Method::kFree: {
      task_ptr.task_ = LABSTOR_CLIENT->NewEmptyTask<FreeTask>(task_ptr.p_);
      ar >> *reinterpret_cast<FreeTask*>(task_ptr.task_);
      break;
    }
    case Method::kMonitor: {
      task_ptr.task_ = LABSTOR_CLIENT->NewEmptyTask<MonitorTask>(task_ptr.p_);
      ar >> *reinterpret_cast<MonitorTask*>(task_ptr.task_);
      break;
    }
    case Method::kUpdateCapacity: {
      task_ptr.task_ = LABSTOR_CLIENT->NewEmptyTask<UpdateCapacityTask>(task_ptr.p_);
      ar >> *reinterpret_cast<UpdateCapacityTask*>(task_ptr.task_);
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
    case Method::kAlloc: {
      ar << *reinterpret_cast<AllocateTask*>(task);
      break;
    }
    case Method::kFree: {
      ar << *reinterpret_cast<FreeTask*>(task);
      break;
    }
    case Method::kMonitor: {
      ar << *reinterpret_cast<MonitorTask*>(task);
      break;
    }
    case Method::kUpdateCapacity: {
      ar << *reinterpret_cast<UpdateCapacityTask*>(task);
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
    case Method::kAlloc: {
      ar.Deserialize(replica, *reinterpret_cast<AllocateTask*>(task));
      break;
    }
    case Method::kFree: {
      ar.Deserialize(replica, *reinterpret_cast<FreeTask*>(task));
      break;
    }
    case Method::kMonitor: {
      ar.Deserialize(replica, *reinterpret_cast<MonitorTask*>(task));
      break;
    }
    case Method::kUpdateCapacity: {
      ar.Deserialize(replica, *reinterpret_cast<UpdateCapacityTask*>(task));
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
    case Method::kAlloc: {
      return reinterpret_cast<AllocateTask*>(task)->GetGroup(group);
    }
    case Method::kFree: {
      return reinterpret_cast<FreeTask*>(task)->GetGroup(group);
    }
    case Method::kMonitor: {
      return reinterpret_cast<MonitorTask*>(task)->GetGroup(group);
    }
    case Method::kUpdateCapacity: {
      return reinterpret_cast<UpdateCapacityTask*>(task)->GetGroup(group);
    }
  }
  return -1;
}

#endif  // LABSTOR_BDEV_METHODS_H_