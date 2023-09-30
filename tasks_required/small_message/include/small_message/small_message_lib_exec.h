#ifndef LABSTOR_SMALL_MESSAGE_LIB_EXEC_H_
#define LABSTOR_SMALL_MESSAGE_LIB_EXEC_H_

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
    case Method::kMd: {
      Md(reinterpret_cast<MdTask *>(task), rctx);
      break;
    }
    case Method::kIo: {
      Io(reinterpret_cast<IoTask *>(task), rctx);
      break;
    }
    case Method::kMdPush: {
      MdPush(reinterpret_cast<MdPushTask *>(task), rctx);
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
    case Method::kMd: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<MdTask *>(task));
      break;
    }
    case Method::kIo: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<IoTask *>(task));
      break;
    }
    case Method::kMdPush: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<MdPushTask *>(task));
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
    case Method::kMd: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<MdTask*>(task));
      break;
    }
    case Method::kIo: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<IoTask*>(task));
      break;
    }
    case Method::kMdPush: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<MdPushTask*>(task));
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
    case Method::kMd: {
      labstor::CALL_REPLICA_END(reinterpret_cast<MdTask*>(task));
      break;
    }
    case Method::kIo: {
      labstor::CALL_REPLICA_END(reinterpret_cast<IoTask*>(task));
      break;
    }
    case Method::kMdPush: {
      labstor::CALL_REPLICA_END(reinterpret_cast<MdPushTask*>(task));
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
    case Method::kMd: {
      ar << *reinterpret_cast<MdTask*>(task);
      break;
    }
    case Method::kIo: {
      ar << *reinterpret_cast<IoTask*>(task);
      break;
    }
    case Method::kMdPush: {
      ar << *reinterpret_cast<MdPushTask*>(task);
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
    case Method::kMd: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<MdTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<MdTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kIo: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<IoTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<IoTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kMdPush: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<MdPushTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<MdPushTask*>(task_ptr.ptr_);
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
    case Method::kMd: {
      ar << *reinterpret_cast<MdTask*>(task);
      break;
    }
    case Method::kIo: {
      ar << *reinterpret_cast<IoTask*>(task);
      break;
    }
    case Method::kMdPush: {
      ar << *reinterpret_cast<MdPushTask*>(task);
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
    case Method::kMd: {
      ar.Deserialize(replica, *reinterpret_cast<MdTask*>(task));
      break;
    }
    case Method::kIo: {
      ar.Deserialize(replica, *reinterpret_cast<IoTask*>(task));
      break;
    }
    case Method::kMdPush: {
      ar.Deserialize(replica, *reinterpret_cast<MdPushTask*>(task));
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
    case Method::kMd: {
      return reinterpret_cast<MdTask*>(task)->GetGroup(group);
    }
    case Method::kIo: {
      return reinterpret_cast<IoTask*>(task)->GetGroup(group);
    }
    case Method::kMdPush: {
      return reinterpret_cast<MdPushTask*>(task)->GetGroup(group);
    }
  }
  return -1;
}

#endif  // LABSTOR_SMALL_MESSAGE_METHODS_H_