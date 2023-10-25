#ifndef HRUN_HERMES_BUCKET_MDM_LIB_EXEC_H_
#define HRUN_HERMES_BUCKET_MDM_LIB_EXEC_H_

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
    case Method::kGetOrCreateTag: {
      GetOrCreateTag(reinterpret_cast<GetOrCreateTagTask *>(task), rctx);
      break;
    }
    case Method::kGetTagId: {
      GetTagId(reinterpret_cast<GetTagIdTask *>(task), rctx);
      break;
    }
    case Method::kGetTagName: {
      GetTagName(reinterpret_cast<GetTagNameTask *>(task), rctx);
      break;
    }
    case Method::kRenameTag: {
      RenameTag(reinterpret_cast<RenameTagTask *>(task), rctx);
      break;
    }
    case Method::kDestroyTag: {
      DestroyTag(reinterpret_cast<DestroyTagTask *>(task), rctx);
      break;
    }
    case Method::kTagAddBlob: {
      TagAddBlob(reinterpret_cast<TagAddBlobTask *>(task), rctx);
      break;
    }
    case Method::kTagRemoveBlob: {
      TagRemoveBlob(reinterpret_cast<TagRemoveBlobTask *>(task), rctx);
      break;
    }
    case Method::kTagClearBlobs: {
      TagClearBlobs(reinterpret_cast<TagClearBlobsTask *>(task), rctx);
      break;
    }
    case Method::kUpdateSize: {
      UpdateSize(reinterpret_cast<UpdateSizeTask *>(task), rctx);
      break;
    }
    case Method::kAppendBlobSchema: {
      AppendBlobSchema(reinterpret_cast<AppendBlobSchemaTask *>(task), rctx);
      break;
    }
    case Method::kAppendBlob: {
      AppendBlob(reinterpret_cast<AppendBlobTask *>(task), rctx);
      break;
    }
    case Method::kGetSize: {
      GetSize(reinterpret_cast<GetSizeTask *>(task), rctx);
      break;
    }
    case Method::kSetBlobMdm: {
      SetBlobMdm(reinterpret_cast<SetBlobMdmTask *>(task), rctx);
      break;
    }
    case Method::kGetContainedBlobIds: {
      GetContainedBlobIds(reinterpret_cast<GetContainedBlobIdsTask *>(task), rctx);
      break;
    }
    case Method::kPollTagMetadata: {
      PollTagMetadata(reinterpret_cast<PollTagMetadataTask *>(task), rctx);
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
    case Method::kGetOrCreateTag: {
      MonitorGetOrCreateTag(mode, reinterpret_cast<GetOrCreateTagTask *>(task), rctx);
      break;
    }
    case Method::kGetTagId: {
      MonitorGetTagId(mode, reinterpret_cast<GetTagIdTask *>(task), rctx);
      break;
    }
    case Method::kGetTagName: {
      MonitorGetTagName(mode, reinterpret_cast<GetTagNameTask *>(task), rctx);
      break;
    }
    case Method::kRenameTag: {
      MonitorRenameTag(mode, reinterpret_cast<RenameTagTask *>(task), rctx);
      break;
    }
    case Method::kDestroyTag: {
      MonitorDestroyTag(mode, reinterpret_cast<DestroyTagTask *>(task), rctx);
      break;
    }
    case Method::kTagAddBlob: {
      MonitorTagAddBlob(mode, reinterpret_cast<TagAddBlobTask *>(task), rctx);
      break;
    }
    case Method::kTagRemoveBlob: {
      MonitorTagRemoveBlob(mode, reinterpret_cast<TagRemoveBlobTask *>(task), rctx);
      break;
    }
    case Method::kTagClearBlobs: {
      MonitorTagClearBlobs(mode, reinterpret_cast<TagClearBlobsTask *>(task), rctx);
      break;
    }
    case Method::kUpdateSize: {
      MonitorUpdateSize(mode, reinterpret_cast<UpdateSizeTask *>(task), rctx);
      break;
    }
    case Method::kAppendBlobSchema: {
      MonitorAppendBlobSchema(mode, reinterpret_cast<AppendBlobSchemaTask *>(task), rctx);
      break;
    }
    case Method::kAppendBlob: {
      MonitorAppendBlob(mode, reinterpret_cast<AppendBlobTask *>(task), rctx);
      break;
    }
    case Method::kGetSize: {
      MonitorGetSize(mode, reinterpret_cast<GetSizeTask *>(task), rctx);
      break;
    }
    case Method::kSetBlobMdm: {
      MonitorSetBlobMdm(mode, reinterpret_cast<SetBlobMdmTask *>(task), rctx);
      break;
    }
    case Method::kGetContainedBlobIds: {
      MonitorGetContainedBlobIds(mode, reinterpret_cast<GetContainedBlobIdsTask *>(task), rctx);
      break;
    }
    case Method::kPollTagMetadata: {
      MonitorPollTagMetadata(mode, reinterpret_cast<PollTagMetadataTask *>(task), rctx);
      break;
    }
  }
}
/** Delete a task */
void Del(u32 method, Task *task) override {
  switch (method) {
    case Method::kConstruct: {
      HRUN_CLIENT->DelTask(reinterpret_cast<ConstructTask *>(task));
      break;
    }
    case Method::kDestruct: {
      HRUN_CLIENT->DelTask(reinterpret_cast<DestructTask *>(task));
      break;
    }
    case Method::kGetOrCreateTag: {
      HRUN_CLIENT->DelTask(reinterpret_cast<GetOrCreateTagTask *>(task));
      break;
    }
    case Method::kGetTagId: {
      HRUN_CLIENT->DelTask(reinterpret_cast<GetTagIdTask *>(task));
      break;
    }
    case Method::kGetTagName: {
      HRUN_CLIENT->DelTask(reinterpret_cast<GetTagNameTask *>(task));
      break;
    }
    case Method::kRenameTag: {
      HRUN_CLIENT->DelTask(reinterpret_cast<RenameTagTask *>(task));
      break;
    }
    case Method::kDestroyTag: {
      HRUN_CLIENT->DelTask(reinterpret_cast<DestroyTagTask *>(task));
      break;
    }
    case Method::kTagAddBlob: {
      HRUN_CLIENT->DelTask(reinterpret_cast<TagAddBlobTask *>(task));
      break;
    }
    case Method::kTagRemoveBlob: {
      HRUN_CLIENT->DelTask(reinterpret_cast<TagRemoveBlobTask *>(task));
      break;
    }
    case Method::kTagClearBlobs: {
      HRUN_CLIENT->DelTask(reinterpret_cast<TagClearBlobsTask *>(task));
      break;
    }
    case Method::kUpdateSize: {
      HRUN_CLIENT->DelTask(reinterpret_cast<UpdateSizeTask *>(task));
      break;
    }
    case Method::kAppendBlobSchema: {
      HRUN_CLIENT->DelTask(reinterpret_cast<AppendBlobSchemaTask *>(task));
      break;
    }
    case Method::kAppendBlob: {
      HRUN_CLIENT->DelTask(reinterpret_cast<AppendBlobTask *>(task));
      break;
    }
    case Method::kGetSize: {
      HRUN_CLIENT->DelTask(reinterpret_cast<GetSizeTask *>(task));
      break;
    }
    case Method::kSetBlobMdm: {
      HRUN_CLIENT->DelTask(reinterpret_cast<SetBlobMdmTask *>(task));
      break;
    }
    case Method::kGetContainedBlobIds: {
      HRUN_CLIENT->DelTask(reinterpret_cast<GetContainedBlobIdsTask *>(task));
      break;
    }
    case Method::kPollTagMetadata: {
      HRUN_CLIENT->DelTask(reinterpret_cast<PollTagMetadataTask *>(task));
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
    case Method::kGetOrCreateTag: {
      hrun::CALL_DUPLICATE(reinterpret_cast<GetOrCreateTagTask*>(orig_task), dups);
      break;
    }
    case Method::kGetTagId: {
      hrun::CALL_DUPLICATE(reinterpret_cast<GetTagIdTask*>(orig_task), dups);
      break;
    }
    case Method::kGetTagName: {
      hrun::CALL_DUPLICATE(reinterpret_cast<GetTagNameTask*>(orig_task), dups);
      break;
    }
    case Method::kRenameTag: {
      hrun::CALL_DUPLICATE(reinterpret_cast<RenameTagTask*>(orig_task), dups);
      break;
    }
    case Method::kDestroyTag: {
      hrun::CALL_DUPLICATE(reinterpret_cast<DestroyTagTask*>(orig_task), dups);
      break;
    }
    case Method::kTagAddBlob: {
      hrun::CALL_DUPLICATE(reinterpret_cast<TagAddBlobTask*>(orig_task), dups);
      break;
    }
    case Method::kTagRemoveBlob: {
      hrun::CALL_DUPLICATE(reinterpret_cast<TagRemoveBlobTask*>(orig_task), dups);
      break;
    }
    case Method::kTagClearBlobs: {
      hrun::CALL_DUPLICATE(reinterpret_cast<TagClearBlobsTask*>(orig_task), dups);
      break;
    }
    case Method::kUpdateSize: {
      hrun::CALL_DUPLICATE(reinterpret_cast<UpdateSizeTask*>(orig_task), dups);
      break;
    }
    case Method::kAppendBlobSchema: {
      hrun::CALL_DUPLICATE(reinterpret_cast<AppendBlobSchemaTask*>(orig_task), dups);
      break;
    }
    case Method::kAppendBlob: {
      hrun::CALL_DUPLICATE(reinterpret_cast<AppendBlobTask*>(orig_task), dups);
      break;
    }
    case Method::kGetSize: {
      hrun::CALL_DUPLICATE(reinterpret_cast<GetSizeTask*>(orig_task), dups);
      break;
    }
    case Method::kSetBlobMdm: {
      hrun::CALL_DUPLICATE(reinterpret_cast<SetBlobMdmTask*>(orig_task), dups);
      break;
    }
    case Method::kGetContainedBlobIds: {
      hrun::CALL_DUPLICATE(reinterpret_cast<GetContainedBlobIdsTask*>(orig_task), dups);
      break;
    }
    case Method::kPollTagMetadata: {
      hrun::CALL_DUPLICATE(reinterpret_cast<PollTagMetadataTask*>(orig_task), dups);
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
    case Method::kGetOrCreateTag: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<GetOrCreateTagTask*>(orig_task), reinterpret_cast<GetOrCreateTagTask*>(dup_task));
      break;
    }
    case Method::kGetTagId: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<GetTagIdTask*>(orig_task), reinterpret_cast<GetTagIdTask*>(dup_task));
      break;
    }
    case Method::kGetTagName: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<GetTagNameTask*>(orig_task), reinterpret_cast<GetTagNameTask*>(dup_task));
      break;
    }
    case Method::kRenameTag: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<RenameTagTask*>(orig_task), reinterpret_cast<RenameTagTask*>(dup_task));
      break;
    }
    case Method::kDestroyTag: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<DestroyTagTask*>(orig_task), reinterpret_cast<DestroyTagTask*>(dup_task));
      break;
    }
    case Method::kTagAddBlob: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<TagAddBlobTask*>(orig_task), reinterpret_cast<TagAddBlobTask*>(dup_task));
      break;
    }
    case Method::kTagRemoveBlob: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<TagRemoveBlobTask*>(orig_task), reinterpret_cast<TagRemoveBlobTask*>(dup_task));
      break;
    }
    case Method::kTagClearBlobs: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<TagClearBlobsTask*>(orig_task), reinterpret_cast<TagClearBlobsTask*>(dup_task));
      break;
    }
    case Method::kUpdateSize: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<UpdateSizeTask*>(orig_task), reinterpret_cast<UpdateSizeTask*>(dup_task));
      break;
    }
    case Method::kAppendBlobSchema: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<AppendBlobSchemaTask*>(orig_task), reinterpret_cast<AppendBlobSchemaTask*>(dup_task));
      break;
    }
    case Method::kAppendBlob: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<AppendBlobTask*>(orig_task), reinterpret_cast<AppendBlobTask*>(dup_task));
      break;
    }
    case Method::kGetSize: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<GetSizeTask*>(orig_task), reinterpret_cast<GetSizeTask*>(dup_task));
      break;
    }
    case Method::kSetBlobMdm: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<SetBlobMdmTask*>(orig_task), reinterpret_cast<SetBlobMdmTask*>(dup_task));
      break;
    }
    case Method::kGetContainedBlobIds: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<GetContainedBlobIdsTask*>(orig_task), reinterpret_cast<GetContainedBlobIdsTask*>(dup_task));
      break;
    }
    case Method::kPollTagMetadata: {
      hrun::CALL_DUPLICATE_END(replica, reinterpret_cast<PollTagMetadataTask*>(orig_task), reinterpret_cast<PollTagMetadataTask*>(dup_task));
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
    case Method::kGetOrCreateTag: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<GetOrCreateTagTask*>(task));
      break;
    }
    case Method::kGetTagId: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<GetTagIdTask*>(task));
      break;
    }
    case Method::kGetTagName: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<GetTagNameTask*>(task));
      break;
    }
    case Method::kRenameTag: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<RenameTagTask*>(task));
      break;
    }
    case Method::kDestroyTag: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<DestroyTagTask*>(task));
      break;
    }
    case Method::kTagAddBlob: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<TagAddBlobTask*>(task));
      break;
    }
    case Method::kTagRemoveBlob: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<TagRemoveBlobTask*>(task));
      break;
    }
    case Method::kTagClearBlobs: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<TagClearBlobsTask*>(task));
      break;
    }
    case Method::kUpdateSize: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<UpdateSizeTask*>(task));
      break;
    }
    case Method::kAppendBlobSchema: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<AppendBlobSchemaTask*>(task));
      break;
    }
    case Method::kAppendBlob: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<AppendBlobTask*>(task));
      break;
    }
    case Method::kGetSize: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<GetSizeTask*>(task));
      break;
    }
    case Method::kSetBlobMdm: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<SetBlobMdmTask*>(task));
      break;
    }
    case Method::kGetContainedBlobIds: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<GetContainedBlobIdsTask*>(task));
      break;
    }
    case Method::kPollTagMetadata: {
      hrun::CALL_REPLICA_START(count, reinterpret_cast<PollTagMetadataTask*>(task));
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
    case Method::kGetOrCreateTag: {
      hrun::CALL_REPLICA_END(reinterpret_cast<GetOrCreateTagTask*>(task));
      break;
    }
    case Method::kGetTagId: {
      hrun::CALL_REPLICA_END(reinterpret_cast<GetTagIdTask*>(task));
      break;
    }
    case Method::kGetTagName: {
      hrun::CALL_REPLICA_END(reinterpret_cast<GetTagNameTask*>(task));
      break;
    }
    case Method::kRenameTag: {
      hrun::CALL_REPLICA_END(reinterpret_cast<RenameTagTask*>(task));
      break;
    }
    case Method::kDestroyTag: {
      hrun::CALL_REPLICA_END(reinterpret_cast<DestroyTagTask*>(task));
      break;
    }
    case Method::kTagAddBlob: {
      hrun::CALL_REPLICA_END(reinterpret_cast<TagAddBlobTask*>(task));
      break;
    }
    case Method::kTagRemoveBlob: {
      hrun::CALL_REPLICA_END(reinterpret_cast<TagRemoveBlobTask*>(task));
      break;
    }
    case Method::kTagClearBlobs: {
      hrun::CALL_REPLICA_END(reinterpret_cast<TagClearBlobsTask*>(task));
      break;
    }
    case Method::kUpdateSize: {
      hrun::CALL_REPLICA_END(reinterpret_cast<UpdateSizeTask*>(task));
      break;
    }
    case Method::kAppendBlobSchema: {
      hrun::CALL_REPLICA_END(reinterpret_cast<AppendBlobSchemaTask*>(task));
      break;
    }
    case Method::kAppendBlob: {
      hrun::CALL_REPLICA_END(reinterpret_cast<AppendBlobTask*>(task));
      break;
    }
    case Method::kGetSize: {
      hrun::CALL_REPLICA_END(reinterpret_cast<GetSizeTask*>(task));
      break;
    }
    case Method::kSetBlobMdm: {
      hrun::CALL_REPLICA_END(reinterpret_cast<SetBlobMdmTask*>(task));
      break;
    }
    case Method::kGetContainedBlobIds: {
      hrun::CALL_REPLICA_END(reinterpret_cast<GetContainedBlobIdsTask*>(task));
      break;
    }
    case Method::kPollTagMetadata: {
      hrun::CALL_REPLICA_END(reinterpret_cast<PollTagMetadataTask*>(task));
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
    case Method::kGetOrCreateTag: {
      ar << *reinterpret_cast<GetOrCreateTagTask*>(task);
      break;
    }
    case Method::kGetTagId: {
      ar << *reinterpret_cast<GetTagIdTask*>(task);
      break;
    }
    case Method::kGetTagName: {
      ar << *reinterpret_cast<GetTagNameTask*>(task);
      break;
    }
    case Method::kRenameTag: {
      ar << *reinterpret_cast<RenameTagTask*>(task);
      break;
    }
    case Method::kDestroyTag: {
      ar << *reinterpret_cast<DestroyTagTask*>(task);
      break;
    }
    case Method::kTagAddBlob: {
      ar << *reinterpret_cast<TagAddBlobTask*>(task);
      break;
    }
    case Method::kTagRemoveBlob: {
      ar << *reinterpret_cast<TagRemoveBlobTask*>(task);
      break;
    }
    case Method::kTagClearBlobs: {
      ar << *reinterpret_cast<TagClearBlobsTask*>(task);
      break;
    }
    case Method::kUpdateSize: {
      ar << *reinterpret_cast<UpdateSizeTask*>(task);
      break;
    }
    case Method::kAppendBlobSchema: {
      ar << *reinterpret_cast<AppendBlobSchemaTask*>(task);
      break;
    }
    case Method::kAppendBlob: {
      ar << *reinterpret_cast<AppendBlobTask*>(task);
      break;
    }
    case Method::kGetSize: {
      ar << *reinterpret_cast<GetSizeTask*>(task);
      break;
    }
    case Method::kSetBlobMdm: {
      ar << *reinterpret_cast<SetBlobMdmTask*>(task);
      break;
    }
    case Method::kGetContainedBlobIds: {
      ar << *reinterpret_cast<GetContainedBlobIdsTask*>(task);
      break;
    }
    case Method::kPollTagMetadata: {
      ar << *reinterpret_cast<PollTagMetadataTask*>(task);
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
    case Method::kGetOrCreateTag: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<GetOrCreateTagTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetOrCreateTagTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kGetTagId: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<GetTagIdTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetTagIdTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kGetTagName: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<GetTagNameTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetTagNameTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kRenameTag: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<RenameTagTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<RenameTagTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kDestroyTag: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<DestroyTagTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<DestroyTagTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kTagAddBlob: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<TagAddBlobTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<TagAddBlobTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kTagRemoveBlob: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<TagRemoveBlobTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<TagRemoveBlobTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kTagClearBlobs: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<TagClearBlobsTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<TagClearBlobsTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kUpdateSize: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<UpdateSizeTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<UpdateSizeTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kAppendBlobSchema: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<AppendBlobSchemaTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<AppendBlobSchemaTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kAppendBlob: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<AppendBlobTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<AppendBlobTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kGetSize: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<GetSizeTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetSizeTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kSetBlobMdm: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<SetBlobMdmTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<SetBlobMdmTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kGetContainedBlobIds: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<GetContainedBlobIdsTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetContainedBlobIdsTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kPollTagMetadata: {
      task_ptr.ptr_ = HRUN_CLIENT->NewEmptyTask<PollTagMetadataTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<PollTagMetadataTask*>(task_ptr.ptr_);
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
    case Method::kGetOrCreateTag: {
      ar << *reinterpret_cast<GetOrCreateTagTask*>(task);
      break;
    }
    case Method::kGetTagId: {
      ar << *reinterpret_cast<GetTagIdTask*>(task);
      break;
    }
    case Method::kGetTagName: {
      ar << *reinterpret_cast<GetTagNameTask*>(task);
      break;
    }
    case Method::kRenameTag: {
      ar << *reinterpret_cast<RenameTagTask*>(task);
      break;
    }
    case Method::kDestroyTag: {
      ar << *reinterpret_cast<DestroyTagTask*>(task);
      break;
    }
    case Method::kTagAddBlob: {
      ar << *reinterpret_cast<TagAddBlobTask*>(task);
      break;
    }
    case Method::kTagRemoveBlob: {
      ar << *reinterpret_cast<TagRemoveBlobTask*>(task);
      break;
    }
    case Method::kTagClearBlobs: {
      ar << *reinterpret_cast<TagClearBlobsTask*>(task);
      break;
    }
    case Method::kUpdateSize: {
      ar << *reinterpret_cast<UpdateSizeTask*>(task);
      break;
    }
    case Method::kAppendBlobSchema: {
      ar << *reinterpret_cast<AppendBlobSchemaTask*>(task);
      break;
    }
    case Method::kAppendBlob: {
      ar << *reinterpret_cast<AppendBlobTask*>(task);
      break;
    }
    case Method::kGetSize: {
      ar << *reinterpret_cast<GetSizeTask*>(task);
      break;
    }
    case Method::kSetBlobMdm: {
      ar << *reinterpret_cast<SetBlobMdmTask*>(task);
      break;
    }
    case Method::kGetContainedBlobIds: {
      ar << *reinterpret_cast<GetContainedBlobIdsTask*>(task);
      break;
    }
    case Method::kPollTagMetadata: {
      ar << *reinterpret_cast<PollTagMetadataTask*>(task);
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
    case Method::kGetOrCreateTag: {
      ar.Deserialize(replica, *reinterpret_cast<GetOrCreateTagTask*>(task));
      break;
    }
    case Method::kGetTagId: {
      ar.Deserialize(replica, *reinterpret_cast<GetTagIdTask*>(task));
      break;
    }
    case Method::kGetTagName: {
      ar.Deserialize(replica, *reinterpret_cast<GetTagNameTask*>(task));
      break;
    }
    case Method::kRenameTag: {
      ar.Deserialize(replica, *reinterpret_cast<RenameTagTask*>(task));
      break;
    }
    case Method::kDestroyTag: {
      ar.Deserialize(replica, *reinterpret_cast<DestroyTagTask*>(task));
      break;
    }
    case Method::kTagAddBlob: {
      ar.Deserialize(replica, *reinterpret_cast<TagAddBlobTask*>(task));
      break;
    }
    case Method::kTagRemoveBlob: {
      ar.Deserialize(replica, *reinterpret_cast<TagRemoveBlobTask*>(task));
      break;
    }
    case Method::kTagClearBlobs: {
      ar.Deserialize(replica, *reinterpret_cast<TagClearBlobsTask*>(task));
      break;
    }
    case Method::kUpdateSize: {
      ar.Deserialize(replica, *reinterpret_cast<UpdateSizeTask*>(task));
      break;
    }
    case Method::kAppendBlobSchema: {
      ar.Deserialize(replica, *reinterpret_cast<AppendBlobSchemaTask*>(task));
      break;
    }
    case Method::kAppendBlob: {
      ar.Deserialize(replica, *reinterpret_cast<AppendBlobTask*>(task));
      break;
    }
    case Method::kGetSize: {
      ar.Deserialize(replica, *reinterpret_cast<GetSizeTask*>(task));
      break;
    }
    case Method::kSetBlobMdm: {
      ar.Deserialize(replica, *reinterpret_cast<SetBlobMdmTask*>(task));
      break;
    }
    case Method::kGetContainedBlobIds: {
      ar.Deserialize(replica, *reinterpret_cast<GetContainedBlobIdsTask*>(task));
      break;
    }
    case Method::kPollTagMetadata: {
      ar.Deserialize(replica, *reinterpret_cast<PollTagMetadataTask*>(task));
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
    case Method::kGetOrCreateTag: {
      return reinterpret_cast<GetOrCreateTagTask*>(task)->GetGroup(group);
    }
    case Method::kGetTagId: {
      return reinterpret_cast<GetTagIdTask*>(task)->GetGroup(group);
    }
    case Method::kGetTagName: {
      return reinterpret_cast<GetTagNameTask*>(task)->GetGroup(group);
    }
    case Method::kRenameTag: {
      return reinterpret_cast<RenameTagTask*>(task)->GetGroup(group);
    }
    case Method::kDestroyTag: {
      return reinterpret_cast<DestroyTagTask*>(task)->GetGroup(group);
    }
    case Method::kTagAddBlob: {
      return reinterpret_cast<TagAddBlobTask*>(task)->GetGroup(group);
    }
    case Method::kTagRemoveBlob: {
      return reinterpret_cast<TagRemoveBlobTask*>(task)->GetGroup(group);
    }
    case Method::kTagClearBlobs: {
      return reinterpret_cast<TagClearBlobsTask*>(task)->GetGroup(group);
    }
    case Method::kUpdateSize: {
      return reinterpret_cast<UpdateSizeTask*>(task)->GetGroup(group);
    }
    case Method::kAppendBlobSchema: {
      return reinterpret_cast<AppendBlobSchemaTask*>(task)->GetGroup(group);
    }
    case Method::kAppendBlob: {
      return reinterpret_cast<AppendBlobTask*>(task)->GetGroup(group);
    }
    case Method::kGetSize: {
      return reinterpret_cast<GetSizeTask*>(task)->GetGroup(group);
    }
    case Method::kSetBlobMdm: {
      return reinterpret_cast<SetBlobMdmTask*>(task)->GetGroup(group);
    }
    case Method::kGetContainedBlobIds: {
      return reinterpret_cast<GetContainedBlobIdsTask*>(task)->GetGroup(group);
    }
    case Method::kPollTagMetadata: {
      return reinterpret_cast<PollTagMetadataTask*>(task)->GetGroup(group);
    }
  }
  return -1;
}

#endif  // HRUN_HERMES_BUCKET_MDM_METHODS_H_