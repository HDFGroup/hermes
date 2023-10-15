#ifndef LABSTOR_HERMES_BLOB_MDM_LIB_EXEC_H_
#define LABSTOR_HERMES_BLOB_MDM_LIB_EXEC_H_

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
    case Method::kPutBlob: {
      PutBlob(reinterpret_cast<PutBlobTask *>(task), rctx);
      break;
    }
    case Method::kGetBlob: {
      GetBlob(reinterpret_cast<GetBlobTask *>(task), rctx);
      break;
    }
    case Method::kTruncateBlob: {
      TruncateBlob(reinterpret_cast<TruncateBlobTask *>(task), rctx);
      break;
    }
    case Method::kDestroyBlob: {
      DestroyBlob(reinterpret_cast<DestroyBlobTask *>(task), rctx);
      break;
    }
    case Method::kTagBlob: {
      TagBlob(reinterpret_cast<TagBlobTask *>(task), rctx);
      break;
    }
    case Method::kBlobHasTag: {
      BlobHasTag(reinterpret_cast<BlobHasTagTask *>(task), rctx);
      break;
    }
    case Method::kGetBlobId: {
      GetBlobId(reinterpret_cast<GetBlobIdTask *>(task), rctx);
      break;
    }
    case Method::kGetOrCreateBlobId: {
      GetOrCreateBlobId(reinterpret_cast<GetOrCreateBlobIdTask *>(task), rctx);
      break;
    }
    case Method::kGetBlobName: {
      GetBlobName(reinterpret_cast<GetBlobNameTask *>(task), rctx);
      break;
    }
    case Method::kGetBlobSize: {
      GetBlobSize(reinterpret_cast<GetBlobSizeTask *>(task), rctx);
      break;
    }
    case Method::kGetBlobScore: {
      GetBlobScore(reinterpret_cast<GetBlobScoreTask *>(task), rctx);
      break;
    }
    case Method::kGetBlobBuffers: {
      GetBlobBuffers(reinterpret_cast<GetBlobBuffersTask *>(task), rctx);
      break;
    }
    case Method::kRenameBlob: {
      RenameBlob(reinterpret_cast<RenameBlobTask *>(task), rctx);
      break;
    }
    case Method::kReorganizeBlob: {
      ReorganizeBlob(reinterpret_cast<ReorganizeBlobTask *>(task), rctx);
      break;
    }
    case Method::kSetBucketMdm: {
      SetBucketMdm(reinterpret_cast<SetBucketMdmTask *>(task), rctx);
      break;
    }
    case Method::kFlushData: {
      FlushData(reinterpret_cast<FlushDataTask *>(task), rctx);
      break;
    }
    case Method::kPollBlobMetadata: {
      PollBlobMetadata(reinterpret_cast<PollBlobMetadataTask *>(task), rctx);
      break;
    }
    case Method::kPollTargetMetadata: {
      PollTargetMetadata(reinterpret_cast<PollTargetMetadataTask *>(task), rctx);
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
    case Method::kPutBlob: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<PutBlobTask *>(task));
      break;
    }
    case Method::kGetBlob: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<GetBlobTask *>(task));
      break;
    }
    case Method::kTruncateBlob: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<TruncateBlobTask *>(task));
      break;
    }
    case Method::kDestroyBlob: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<DestroyBlobTask *>(task));
      break;
    }
    case Method::kTagBlob: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<TagBlobTask *>(task));
      break;
    }
    case Method::kBlobHasTag: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<BlobHasTagTask *>(task));
      break;
    }
    case Method::kGetBlobId: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<GetBlobIdTask *>(task));
      break;
    }
    case Method::kGetOrCreateBlobId: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<GetOrCreateBlobIdTask *>(task));
      break;
    }
    case Method::kGetBlobName: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<GetBlobNameTask *>(task));
      break;
    }
    case Method::kGetBlobSize: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<GetBlobSizeTask *>(task));
      break;
    }
    case Method::kGetBlobScore: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<GetBlobScoreTask *>(task));
      break;
    }
    case Method::kGetBlobBuffers: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<GetBlobBuffersTask *>(task));
      break;
    }
    case Method::kRenameBlob: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<RenameBlobTask *>(task));
      break;
    }
    case Method::kReorganizeBlob: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<ReorganizeBlobTask *>(task));
      break;
    }
    case Method::kSetBucketMdm: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<SetBucketMdmTask *>(task));
      break;
    }
    case Method::kFlushData: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<FlushDataTask *>(task));
      break;
    }
    case Method::kPollBlobMetadata: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<PollBlobMetadataTask *>(task));
      break;
    }
    case Method::kPollTargetMetadata: {
      LABSTOR_CLIENT->DelTask(reinterpret_cast<PollTargetMetadataTask *>(task));
      break;
    }
  }
}
/** Duplicate a task */
void Dup(u32 method, Task *orig_task, std::vector<LPointer<Task>> &dups) override {
  switch (method) {
    case Method::kConstruct: {
      labstor::CALL_DUPLICATE(reinterpret_cast<ConstructTask*>(orig_task), dups);
      break;
    }
    case Method::kDestruct: {
      labstor::CALL_DUPLICATE(reinterpret_cast<DestructTask*>(orig_task), dups);
      break;
    }
    case Method::kPutBlob: {
      labstor::CALL_DUPLICATE(reinterpret_cast<PutBlobTask*>(orig_task), dups);
      break;
    }
    case Method::kGetBlob: {
      labstor::CALL_DUPLICATE(reinterpret_cast<GetBlobTask*>(orig_task), dups);
      break;
    }
    case Method::kTruncateBlob: {
      labstor::CALL_DUPLICATE(reinterpret_cast<TruncateBlobTask*>(orig_task), dups);
      break;
    }
    case Method::kDestroyBlob: {
      labstor::CALL_DUPLICATE(reinterpret_cast<DestroyBlobTask*>(orig_task), dups);
      break;
    }
    case Method::kTagBlob: {
      labstor::CALL_DUPLICATE(reinterpret_cast<TagBlobTask*>(orig_task), dups);
      break;
    }
    case Method::kBlobHasTag: {
      labstor::CALL_DUPLICATE(reinterpret_cast<BlobHasTagTask*>(orig_task), dups);
      break;
    }
    case Method::kGetBlobId: {
      labstor::CALL_DUPLICATE(reinterpret_cast<GetBlobIdTask*>(orig_task), dups);
      break;
    }
    case Method::kGetOrCreateBlobId: {
      labstor::CALL_DUPLICATE(reinterpret_cast<GetOrCreateBlobIdTask*>(orig_task), dups);
      break;
    }
    case Method::kGetBlobName: {
      labstor::CALL_DUPLICATE(reinterpret_cast<GetBlobNameTask*>(orig_task), dups);
      break;
    }
    case Method::kGetBlobSize: {
      labstor::CALL_DUPLICATE(reinterpret_cast<GetBlobSizeTask*>(orig_task), dups);
      break;
    }
    case Method::kGetBlobScore: {
      labstor::CALL_DUPLICATE(reinterpret_cast<GetBlobScoreTask*>(orig_task), dups);
      break;
    }
    case Method::kGetBlobBuffers: {
      labstor::CALL_DUPLICATE(reinterpret_cast<GetBlobBuffersTask*>(orig_task), dups);
      break;
    }
    case Method::kRenameBlob: {
      labstor::CALL_DUPLICATE(reinterpret_cast<RenameBlobTask*>(orig_task), dups);
      break;
    }
    case Method::kReorganizeBlob: {
      labstor::CALL_DUPLICATE(reinterpret_cast<ReorganizeBlobTask*>(orig_task), dups);
      break;
    }
    case Method::kSetBucketMdm: {
      labstor::CALL_DUPLICATE(reinterpret_cast<SetBucketMdmTask*>(orig_task), dups);
      break;
    }
    case Method::kFlushData: {
      labstor::CALL_DUPLICATE(reinterpret_cast<FlushDataTask*>(orig_task), dups);
      break;
    }
    case Method::kPollBlobMetadata: {
      labstor::CALL_DUPLICATE(reinterpret_cast<PollBlobMetadataTask*>(orig_task), dups);
      break;
    }
    case Method::kPollTargetMetadata: {
      labstor::CALL_DUPLICATE(reinterpret_cast<PollTargetMetadataTask*>(orig_task), dups);
      break;
    }
  }
}
/** Register the duplicate output with the origin task */
void DupEnd(u32 method, u32 replica, Task *orig_task, Task *dup_task) override {
  switch (method) {
    case Method::kConstruct: {
      labstor::CALL_DUPLICATE_END(replica, reinterpret_cast<ConstructTask*>(orig_task), reinterpret_cast<ConstructTask*>(dup_task));
      break;
    }
    case Method::kDestruct: {
      labstor::CALL_DUPLICATE_END(replica, reinterpret_cast<DestructTask*>(orig_task), reinterpret_cast<DestructTask*>(dup_task));
      break;
    }
    case Method::kPutBlob: {
      labstor::CALL_DUPLICATE_END(replica, reinterpret_cast<PutBlobTask*>(orig_task), reinterpret_cast<PutBlobTask*>(dup_task));
      break;
    }
    case Method::kGetBlob: {
      labstor::CALL_DUPLICATE_END(replica, reinterpret_cast<GetBlobTask*>(orig_task), reinterpret_cast<GetBlobTask*>(dup_task));
      break;
    }
    case Method::kTruncateBlob: {
      labstor::CALL_DUPLICATE_END(replica, reinterpret_cast<TruncateBlobTask*>(orig_task), reinterpret_cast<TruncateBlobTask*>(dup_task));
      break;
    }
    case Method::kDestroyBlob: {
      labstor::CALL_DUPLICATE_END(replica, reinterpret_cast<DestroyBlobTask*>(orig_task), reinterpret_cast<DestroyBlobTask*>(dup_task));
      break;
    }
    case Method::kTagBlob: {
      labstor::CALL_DUPLICATE_END(replica, reinterpret_cast<TagBlobTask*>(orig_task), reinterpret_cast<TagBlobTask*>(dup_task));
      break;
    }
    case Method::kBlobHasTag: {
      labstor::CALL_DUPLICATE_END(replica, reinterpret_cast<BlobHasTagTask*>(orig_task), reinterpret_cast<BlobHasTagTask*>(dup_task));
      break;
    }
    case Method::kGetBlobId: {
      labstor::CALL_DUPLICATE_END(replica, reinterpret_cast<GetBlobIdTask*>(orig_task), reinterpret_cast<GetBlobIdTask*>(dup_task));
      break;
    }
    case Method::kGetOrCreateBlobId: {
      labstor::CALL_DUPLICATE_END(replica, reinterpret_cast<GetOrCreateBlobIdTask*>(orig_task), reinterpret_cast<GetOrCreateBlobIdTask*>(dup_task));
      break;
    }
    case Method::kGetBlobName: {
      labstor::CALL_DUPLICATE_END(replica, reinterpret_cast<GetBlobNameTask*>(orig_task), reinterpret_cast<GetBlobNameTask*>(dup_task));
      break;
    }
    case Method::kGetBlobSize: {
      labstor::CALL_DUPLICATE_END(replica, reinterpret_cast<GetBlobSizeTask*>(orig_task), reinterpret_cast<GetBlobSizeTask*>(dup_task));
      break;
    }
    case Method::kGetBlobScore: {
      labstor::CALL_DUPLICATE_END(replica, reinterpret_cast<GetBlobScoreTask*>(orig_task), reinterpret_cast<GetBlobScoreTask*>(dup_task));
      break;
    }
    case Method::kGetBlobBuffers: {
      labstor::CALL_DUPLICATE_END(replica, reinterpret_cast<GetBlobBuffersTask*>(orig_task), reinterpret_cast<GetBlobBuffersTask*>(dup_task));
      break;
    }
    case Method::kRenameBlob: {
      labstor::CALL_DUPLICATE_END(replica, reinterpret_cast<RenameBlobTask*>(orig_task), reinterpret_cast<RenameBlobTask*>(dup_task));
      break;
    }
    case Method::kReorganizeBlob: {
      labstor::CALL_DUPLICATE_END(replica, reinterpret_cast<ReorganizeBlobTask*>(orig_task), reinterpret_cast<ReorganizeBlobTask*>(dup_task));
      break;
    }
    case Method::kSetBucketMdm: {
      labstor::CALL_DUPLICATE_END(replica, reinterpret_cast<SetBucketMdmTask*>(orig_task), reinterpret_cast<SetBucketMdmTask*>(dup_task));
      break;
    }
    case Method::kFlushData: {
      labstor::CALL_DUPLICATE_END(replica, reinterpret_cast<FlushDataTask*>(orig_task), reinterpret_cast<FlushDataTask*>(dup_task));
      break;
    }
    case Method::kPollBlobMetadata: {
      labstor::CALL_DUPLICATE_END(replica, reinterpret_cast<PollBlobMetadataTask*>(orig_task), reinterpret_cast<PollBlobMetadataTask*>(dup_task));
      break;
    }
    case Method::kPollTargetMetadata: {
      labstor::CALL_DUPLICATE_END(replica, reinterpret_cast<PollTargetMetadataTask*>(orig_task), reinterpret_cast<PollTargetMetadataTask*>(dup_task));
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
    case Method::kPutBlob: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<PutBlobTask*>(task));
      break;
    }
    case Method::kGetBlob: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<GetBlobTask*>(task));
      break;
    }
    case Method::kTruncateBlob: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<TruncateBlobTask*>(task));
      break;
    }
    case Method::kDestroyBlob: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<DestroyBlobTask*>(task));
      break;
    }
    case Method::kTagBlob: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<TagBlobTask*>(task));
      break;
    }
    case Method::kBlobHasTag: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<BlobHasTagTask*>(task));
      break;
    }
    case Method::kGetBlobId: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<GetBlobIdTask*>(task));
      break;
    }
    case Method::kGetOrCreateBlobId: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<GetOrCreateBlobIdTask*>(task));
      break;
    }
    case Method::kGetBlobName: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<GetBlobNameTask*>(task));
      break;
    }
    case Method::kGetBlobSize: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<GetBlobSizeTask*>(task));
      break;
    }
    case Method::kGetBlobScore: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<GetBlobScoreTask*>(task));
      break;
    }
    case Method::kGetBlobBuffers: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<GetBlobBuffersTask*>(task));
      break;
    }
    case Method::kRenameBlob: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<RenameBlobTask*>(task));
      break;
    }
    case Method::kReorganizeBlob: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<ReorganizeBlobTask*>(task));
      break;
    }
    case Method::kSetBucketMdm: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<SetBucketMdmTask*>(task));
      break;
    }
    case Method::kFlushData: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<FlushDataTask*>(task));
      break;
    }
    case Method::kPollBlobMetadata: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<PollBlobMetadataTask*>(task));
      break;
    }
    case Method::kPollTargetMetadata: {
      labstor::CALL_REPLICA_START(count, reinterpret_cast<PollTargetMetadataTask*>(task));
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
    case Method::kPutBlob: {
      labstor::CALL_REPLICA_END(reinterpret_cast<PutBlobTask*>(task));
      break;
    }
    case Method::kGetBlob: {
      labstor::CALL_REPLICA_END(reinterpret_cast<GetBlobTask*>(task));
      break;
    }
    case Method::kTruncateBlob: {
      labstor::CALL_REPLICA_END(reinterpret_cast<TruncateBlobTask*>(task));
      break;
    }
    case Method::kDestroyBlob: {
      labstor::CALL_REPLICA_END(reinterpret_cast<DestroyBlobTask*>(task));
      break;
    }
    case Method::kTagBlob: {
      labstor::CALL_REPLICA_END(reinterpret_cast<TagBlobTask*>(task));
      break;
    }
    case Method::kBlobHasTag: {
      labstor::CALL_REPLICA_END(reinterpret_cast<BlobHasTagTask*>(task));
      break;
    }
    case Method::kGetBlobId: {
      labstor::CALL_REPLICA_END(reinterpret_cast<GetBlobIdTask*>(task));
      break;
    }
    case Method::kGetOrCreateBlobId: {
      labstor::CALL_REPLICA_END(reinterpret_cast<GetOrCreateBlobIdTask*>(task));
      break;
    }
    case Method::kGetBlobName: {
      labstor::CALL_REPLICA_END(reinterpret_cast<GetBlobNameTask*>(task));
      break;
    }
    case Method::kGetBlobSize: {
      labstor::CALL_REPLICA_END(reinterpret_cast<GetBlobSizeTask*>(task));
      break;
    }
    case Method::kGetBlobScore: {
      labstor::CALL_REPLICA_END(reinterpret_cast<GetBlobScoreTask*>(task));
      break;
    }
    case Method::kGetBlobBuffers: {
      labstor::CALL_REPLICA_END(reinterpret_cast<GetBlobBuffersTask*>(task));
      break;
    }
    case Method::kRenameBlob: {
      labstor::CALL_REPLICA_END(reinterpret_cast<RenameBlobTask*>(task));
      break;
    }
    case Method::kReorganizeBlob: {
      labstor::CALL_REPLICA_END(reinterpret_cast<ReorganizeBlobTask*>(task));
      break;
    }
    case Method::kSetBucketMdm: {
      labstor::CALL_REPLICA_END(reinterpret_cast<SetBucketMdmTask*>(task));
      break;
    }
    case Method::kFlushData: {
      labstor::CALL_REPLICA_END(reinterpret_cast<FlushDataTask*>(task));
      break;
    }
    case Method::kPollBlobMetadata: {
      labstor::CALL_REPLICA_END(reinterpret_cast<PollBlobMetadataTask*>(task));
      break;
    }
    case Method::kPollTargetMetadata: {
      labstor::CALL_REPLICA_END(reinterpret_cast<PollTargetMetadataTask*>(task));
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
    case Method::kPutBlob: {
      ar << *reinterpret_cast<PutBlobTask*>(task);
      break;
    }
    case Method::kGetBlob: {
      ar << *reinterpret_cast<GetBlobTask*>(task);
      break;
    }
    case Method::kTruncateBlob: {
      ar << *reinterpret_cast<TruncateBlobTask*>(task);
      break;
    }
    case Method::kDestroyBlob: {
      ar << *reinterpret_cast<DestroyBlobTask*>(task);
      break;
    }
    case Method::kTagBlob: {
      ar << *reinterpret_cast<TagBlobTask*>(task);
      break;
    }
    case Method::kBlobHasTag: {
      ar << *reinterpret_cast<BlobHasTagTask*>(task);
      break;
    }
    case Method::kGetBlobId: {
      ar << *reinterpret_cast<GetBlobIdTask*>(task);
      break;
    }
    case Method::kGetOrCreateBlobId: {
      ar << *reinterpret_cast<GetOrCreateBlobIdTask*>(task);
      break;
    }
    case Method::kGetBlobName: {
      ar << *reinterpret_cast<GetBlobNameTask*>(task);
      break;
    }
    case Method::kGetBlobSize: {
      ar << *reinterpret_cast<GetBlobSizeTask*>(task);
      break;
    }
    case Method::kGetBlobScore: {
      ar << *reinterpret_cast<GetBlobScoreTask*>(task);
      break;
    }
    case Method::kGetBlobBuffers: {
      ar << *reinterpret_cast<GetBlobBuffersTask*>(task);
      break;
    }
    case Method::kRenameBlob: {
      ar << *reinterpret_cast<RenameBlobTask*>(task);
      break;
    }
    case Method::kReorganizeBlob: {
      ar << *reinterpret_cast<ReorganizeBlobTask*>(task);
      break;
    }
    case Method::kSetBucketMdm: {
      ar << *reinterpret_cast<SetBucketMdmTask*>(task);
      break;
    }
    case Method::kFlushData: {
      ar << *reinterpret_cast<FlushDataTask*>(task);
      break;
    }
    case Method::kPollBlobMetadata: {
      ar << *reinterpret_cast<PollBlobMetadataTask*>(task);
      break;
    }
    case Method::kPollTargetMetadata: {
      ar << *reinterpret_cast<PollTargetMetadataTask*>(task);
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
    case Method::kPutBlob: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<PutBlobTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<PutBlobTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kGetBlob: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<GetBlobTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetBlobTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kTruncateBlob: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<TruncateBlobTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<TruncateBlobTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kDestroyBlob: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<DestroyBlobTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<DestroyBlobTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kTagBlob: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<TagBlobTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<TagBlobTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kBlobHasTag: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<BlobHasTagTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<BlobHasTagTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kGetBlobId: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<GetBlobIdTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetBlobIdTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kGetOrCreateBlobId: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<GetOrCreateBlobIdTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetOrCreateBlobIdTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kGetBlobName: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<GetBlobNameTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetBlobNameTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kGetBlobSize: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<GetBlobSizeTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetBlobSizeTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kGetBlobScore: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<GetBlobScoreTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetBlobScoreTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kGetBlobBuffers: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<GetBlobBuffersTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<GetBlobBuffersTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kRenameBlob: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<RenameBlobTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<RenameBlobTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kReorganizeBlob: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<ReorganizeBlobTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<ReorganizeBlobTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kSetBucketMdm: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<SetBucketMdmTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<SetBucketMdmTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kFlushData: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<FlushDataTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<FlushDataTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kPollBlobMetadata: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<PollBlobMetadataTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<PollBlobMetadataTask*>(task_ptr.ptr_);
      break;
    }
    case Method::kPollTargetMetadata: {
      task_ptr.ptr_ = LABSTOR_CLIENT->NewEmptyTask<PollTargetMetadataTask>(task_ptr.shm_);
      ar >> *reinterpret_cast<PollTargetMetadataTask*>(task_ptr.ptr_);
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
    case Method::kPutBlob: {
      ar << *reinterpret_cast<PutBlobTask*>(task);
      break;
    }
    case Method::kGetBlob: {
      ar << *reinterpret_cast<GetBlobTask*>(task);
      break;
    }
    case Method::kTruncateBlob: {
      ar << *reinterpret_cast<TruncateBlobTask*>(task);
      break;
    }
    case Method::kDestroyBlob: {
      ar << *reinterpret_cast<DestroyBlobTask*>(task);
      break;
    }
    case Method::kTagBlob: {
      ar << *reinterpret_cast<TagBlobTask*>(task);
      break;
    }
    case Method::kBlobHasTag: {
      ar << *reinterpret_cast<BlobHasTagTask*>(task);
      break;
    }
    case Method::kGetBlobId: {
      ar << *reinterpret_cast<GetBlobIdTask*>(task);
      break;
    }
    case Method::kGetOrCreateBlobId: {
      ar << *reinterpret_cast<GetOrCreateBlobIdTask*>(task);
      break;
    }
    case Method::kGetBlobName: {
      ar << *reinterpret_cast<GetBlobNameTask*>(task);
      break;
    }
    case Method::kGetBlobSize: {
      ar << *reinterpret_cast<GetBlobSizeTask*>(task);
      break;
    }
    case Method::kGetBlobScore: {
      ar << *reinterpret_cast<GetBlobScoreTask*>(task);
      break;
    }
    case Method::kGetBlobBuffers: {
      ar << *reinterpret_cast<GetBlobBuffersTask*>(task);
      break;
    }
    case Method::kRenameBlob: {
      ar << *reinterpret_cast<RenameBlobTask*>(task);
      break;
    }
    case Method::kReorganizeBlob: {
      ar << *reinterpret_cast<ReorganizeBlobTask*>(task);
      break;
    }
    case Method::kSetBucketMdm: {
      ar << *reinterpret_cast<SetBucketMdmTask*>(task);
      break;
    }
    case Method::kFlushData: {
      ar << *reinterpret_cast<FlushDataTask*>(task);
      break;
    }
    case Method::kPollBlobMetadata: {
      ar << *reinterpret_cast<PollBlobMetadataTask*>(task);
      break;
    }
    case Method::kPollTargetMetadata: {
      ar << *reinterpret_cast<PollTargetMetadataTask*>(task);
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
    case Method::kPutBlob: {
      ar.Deserialize(replica, *reinterpret_cast<PutBlobTask*>(task));
      break;
    }
    case Method::kGetBlob: {
      ar.Deserialize(replica, *reinterpret_cast<GetBlobTask*>(task));
      break;
    }
    case Method::kTruncateBlob: {
      ar.Deserialize(replica, *reinterpret_cast<TruncateBlobTask*>(task));
      break;
    }
    case Method::kDestroyBlob: {
      ar.Deserialize(replica, *reinterpret_cast<DestroyBlobTask*>(task));
      break;
    }
    case Method::kTagBlob: {
      ar.Deserialize(replica, *reinterpret_cast<TagBlobTask*>(task));
      break;
    }
    case Method::kBlobHasTag: {
      ar.Deserialize(replica, *reinterpret_cast<BlobHasTagTask*>(task));
      break;
    }
    case Method::kGetBlobId: {
      ar.Deserialize(replica, *reinterpret_cast<GetBlobIdTask*>(task));
      break;
    }
    case Method::kGetOrCreateBlobId: {
      ar.Deserialize(replica, *reinterpret_cast<GetOrCreateBlobIdTask*>(task));
      break;
    }
    case Method::kGetBlobName: {
      ar.Deserialize(replica, *reinterpret_cast<GetBlobNameTask*>(task));
      break;
    }
    case Method::kGetBlobSize: {
      ar.Deserialize(replica, *reinterpret_cast<GetBlobSizeTask*>(task));
      break;
    }
    case Method::kGetBlobScore: {
      ar.Deserialize(replica, *reinterpret_cast<GetBlobScoreTask*>(task));
      break;
    }
    case Method::kGetBlobBuffers: {
      ar.Deserialize(replica, *reinterpret_cast<GetBlobBuffersTask*>(task));
      break;
    }
    case Method::kRenameBlob: {
      ar.Deserialize(replica, *reinterpret_cast<RenameBlobTask*>(task));
      break;
    }
    case Method::kReorganizeBlob: {
      ar.Deserialize(replica, *reinterpret_cast<ReorganizeBlobTask*>(task));
      break;
    }
    case Method::kSetBucketMdm: {
      ar.Deserialize(replica, *reinterpret_cast<SetBucketMdmTask*>(task));
      break;
    }
    case Method::kFlushData: {
      ar.Deserialize(replica, *reinterpret_cast<FlushDataTask*>(task));
      break;
    }
    case Method::kPollBlobMetadata: {
      ar.Deserialize(replica, *reinterpret_cast<PollBlobMetadataTask*>(task));
      break;
    }
    case Method::kPollTargetMetadata: {
      ar.Deserialize(replica, *reinterpret_cast<PollTargetMetadataTask*>(task));
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
    case Method::kPutBlob: {
      return reinterpret_cast<PutBlobTask*>(task)->GetGroup(group);
    }
    case Method::kGetBlob: {
      return reinterpret_cast<GetBlobTask*>(task)->GetGroup(group);
    }
    case Method::kTruncateBlob: {
      return reinterpret_cast<TruncateBlobTask*>(task)->GetGroup(group);
    }
    case Method::kDestroyBlob: {
      return reinterpret_cast<DestroyBlobTask*>(task)->GetGroup(group);
    }
    case Method::kTagBlob: {
      return reinterpret_cast<TagBlobTask*>(task)->GetGroup(group);
    }
    case Method::kBlobHasTag: {
      return reinterpret_cast<BlobHasTagTask*>(task)->GetGroup(group);
    }
    case Method::kGetBlobId: {
      return reinterpret_cast<GetBlobIdTask*>(task)->GetGroup(group);
    }
    case Method::kGetOrCreateBlobId: {
      return reinterpret_cast<GetOrCreateBlobIdTask*>(task)->GetGroup(group);
    }
    case Method::kGetBlobName: {
      return reinterpret_cast<GetBlobNameTask*>(task)->GetGroup(group);
    }
    case Method::kGetBlobSize: {
      return reinterpret_cast<GetBlobSizeTask*>(task)->GetGroup(group);
    }
    case Method::kGetBlobScore: {
      return reinterpret_cast<GetBlobScoreTask*>(task)->GetGroup(group);
    }
    case Method::kGetBlobBuffers: {
      return reinterpret_cast<GetBlobBuffersTask*>(task)->GetGroup(group);
    }
    case Method::kRenameBlob: {
      return reinterpret_cast<RenameBlobTask*>(task)->GetGroup(group);
    }
    case Method::kReorganizeBlob: {
      return reinterpret_cast<ReorganizeBlobTask*>(task)->GetGroup(group);
    }
    case Method::kSetBucketMdm: {
      return reinterpret_cast<SetBucketMdmTask*>(task)->GetGroup(group);
    }
    case Method::kFlushData: {
      return reinterpret_cast<FlushDataTask*>(task)->GetGroup(group);
    }
    case Method::kPollBlobMetadata: {
      return reinterpret_cast<PollBlobMetadataTask*>(task)->GetGroup(group);
    }
    case Method::kPollTargetMetadata: {
      return reinterpret_cast<PollTargetMetadataTask*>(task)->GetGroup(group);
    }
  }
  return -1;
}

#endif  // LABSTOR_HERMES_BLOB_MDM_METHODS_H_