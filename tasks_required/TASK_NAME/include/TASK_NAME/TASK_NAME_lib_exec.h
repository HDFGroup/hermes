#ifndef LABSTOR_TASK_NAME_LIB_EXEC_H_
#define LABSTOR_TASK_NAME_LIB_EXEC_H_

/** Execute a task */
void Run(u32 method, Task *task, RunContext &ctx) override {
  switch (method) {
    case Method::kConstruct: {
      Construct(reinterpret_cast<ConstructTask *>(task), ctx);
      break;
    }
    case Method::kDestruct: {
      Destruct(reinterpret_cast<DestructTask *>(task), ctx);
      break;
    }
    case Method::kCustom: {
      Custom(reinterpret_cast<CustomTask *>(task), ctx);
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
    case Method::kCustom: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<CustomTask*>(task));
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
    case Method::kCustom: {
      labstor::CALL_REPLICA_END(reinterpret_cast<CustomTask*>(task));
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
    case Method::kCustom: {
      ar << *reinterpret_cast<CustomTask*>(task);
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
    case Method::kCustom: {
      task_ptr.task_ = LABSTOR_CLIENT->NewEmptyTask<CustomTask>(task_ptr.p_);
      ar >> *reinterpret_cast<CustomTask*>(task_ptr.task_);
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
    case Method::kCustom: {
      ar << *reinterpret_cast<CustomTask*>(task);
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
    case Method::kCustom: {
      ar.Deserialize(replica, *reinterpret_cast<CustomTask*>(task));
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
    case Method::kCustom: {
      return reinterpret_cast<CustomTask*>(task)->GetGroup(group);
    }
  }
  return -1;
}

#endif  // LABSTOR_TASK_NAME_METHODS_H_