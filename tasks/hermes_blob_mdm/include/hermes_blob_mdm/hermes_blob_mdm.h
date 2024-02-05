//
// Created by lukemartinlogan on 6/29/23.
//

#ifndef HRUN_hermes_blob_mdm_H_
#define HRUN_hermes_blob_mdm_H_

#include "hermes_blob_mdm_tasks.h"

namespace hermes::blob_mdm {

/** Create hermes_blob_mdm requests */
class Client : public TaskLibClient {

 public:
  /** Default constructor */
  Client() = default;

  /** Destructor */
  ~Client() = default;

  /** Create a hermes_blob_mdm */
  HSHM_ALWAYS_INLINE
      LPointer<ConstructTask> AsyncCreate(const TaskNode &task_node,
                                          const DomainId &domain_id,
                                          const std::string &state_name) {
    id_ = TaskStateId::GetNull();
    QueueManagerInfo &qm = HRUN_CLIENT->server_config_.queue_manager_;
    std::vector<PriorityInfo> queue_info;
    return HRUN_ADMIN->AsyncCreateTaskState<ConstructTask>(
        task_node, domain_id, state_name, id_, queue_info);
  }
  void AsyncCreateComplete(ConstructTask *task) {
    if (task->IsModuleComplete()) {
      id_ = task->id_;
      queue_id_ = QueueId(id_);
      HRUN_CLIENT->DelTask(task);
    }
  }
  HRUN_TASK_NODE_ROOT(AsyncCreate);
  template<typename ...Args>
  HSHM_ALWAYS_INLINE
  void CreateRoot(Args&& ...args) {
    LPointer<ConstructTask> task = AsyncCreateRoot(std::forward<Args>(args)...);
    task->Wait();
    AsyncCreateComplete(task.ptr_);
  }

  /** Destroy task state + queue */
  HSHM_ALWAYS_INLINE
  void DestroyRoot(const DomainId &domain_id) {
    HRUN_ADMIN->DestroyTaskStateRoot(domain_id, id_);
  }

  /**====================================
   * Blob Operations
   * ===================================*/

  /** Sets the BUCKET MDM */
  void AsyncSetBucketMdmConstruct(SetBucketMdmTask *task,
                                  const TaskNode &task_node,
                                  const DomainId &domain_id,
                                  const TaskStateId &blob_mdm,
                                  const TaskStateId &stager_mdm,
                                  const TaskStateId &op_mdm) {
    HRUN_CLIENT->ConstructTask<SetBucketMdmTask>(
        task, task_node, domain_id, id_, blob_mdm, stager_mdm, op_mdm);
  }
  void SetBucketMdmRoot(const DomainId &domain_id,
                        const TaskStateId &blob_mdm,
                        const TaskStateId &stager_mdm,
                        const TaskStateId &op_mdm) {
    LPointer<hrunpq::TypedPushTask<SetBucketMdmTask>> push_task =
                                                          AsyncSetBucketMdmRoot(domain_id, blob_mdm, stager_mdm, op_mdm);
    push_task->Wait();
    HRUN_CLIENT->DelTask(push_task);
  }
  HRUN_TASK_NODE_PUSH_ROOT(SetBucketMdm);

  /**
   * Get \a blob_name BLOB from \a bkt_id bucket
   * */
  void AsyncGetOrCreateBlobIdConstruct(GetOrCreateBlobIdTask* task,
                                       const TaskNode &task_node,
                                       TagId tag_id,
                                       const hshm::charbuf &blob_name) {
    u32 hash = HashBlobName(tag_id, blob_name);
    HRUN_CLIENT->ConstructTask<GetOrCreateBlobIdTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id, blob_name);
  }
  BlobId GetOrCreateBlobIdRoot(TagId tag_id, const hshm::charbuf &blob_name) {
    LPointer<hrunpq::TypedPushTask<GetOrCreateBlobIdTask>> push_task =
                                                               AsyncGetOrCreateBlobIdRoot(tag_id, blob_name);
    push_task->Wait();
    GetOrCreateBlobIdTask *task = push_task->get();
    BlobId blob_id = task->blob_id_;
    HRUN_CLIENT->DelTask(push_task);
    return blob_id;
  }
  HRUN_TASK_NODE_PUSH_ROOT(GetOrCreateBlobId);

  /**
  * Create a blob's metadata
  *
  * @param tag_id id of the bucket
  * @param blob_name semantic blob name
  * @param blob_id the id of the blob
  * @param blob_off the offset of the data placed in existing blob
  * @param blob_size the amount of data being placed
  * @param blob a SHM pointer to the data to place
  * @param score the current score of the blob
  * @param replace whether to replace the blob if it exists
  * @param[OUT] did_create whether the blob was created or not
  * */
  void AsyncPutBlobConstruct(
      PutBlobTask *task,
      const TaskNode &task_node,
      TagId tag_id, const hshm::charbuf &blob_name,
      const BlobId &blob_id, size_t blob_off, size_t blob_size,
      const hipc::Pointer &blob, float score,
      u32 flags,
      Context ctx = Context(),
      u32 task_flags = TASK_FIRE_AND_FORGET | TASK_DATA_OWNER | TASK_LOW_LATENCY) {
    HRUN_CLIENT->ConstructTask<PutBlobTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_name, blob_id,
        blob_off, blob_size,
        blob, score, flags, ctx, task_flags);
  }
  HRUN_TASK_NODE_PUSH_ROOT(PutBlob);

  /** Get a blob's data */
  void AsyncGetBlobConstruct(GetBlobTask *task,
                             const TaskNode &task_node,
                             const TagId &tag_id,
                             const hshm::charbuf &blob_name,
                             const BlobId &blob_id,
                             size_t off,
                             ssize_t data_size,
                             hipc::Pointer &data,
                             Context ctx = Context(),
                             u32 flags = 0) {
    // HILOG(kDebug, "Beginning GET (task_node={})", task_node);
    HRUN_CLIENT->ConstructTask<GetBlobTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_name, blob_id, off, data_size, data, ctx, flags);
  }
  size_t GetBlobRoot(const TagId &tag_id,
                     const BlobId &blob_id,
                     size_t off,
                     ssize_t data_size,
                     hipc::Pointer &data,
                     Context ctx = Context(),
                     u32 flags = 0) {
    LPointer<hrunpq::TypedPushTask<GetBlobTask>> push_task =
        AsyncGetBlobRoot(tag_id, hshm::charbuf(""),
                         blob_id, off, data_size, data, ctx, flags);
    push_task->Wait();
    GetBlobTask *task = push_task->get();
    data = task->data_;
    size_t true_size = task->data_size_;
    HRUN_CLIENT->DelTask(push_task);
    return true_size;
  }
  HRUN_TASK_NODE_PUSH_ROOT(GetBlob);

  /**
   * Reorganize a blob
   *
   * @param blob_id id of the blob being reorganized
   * @param score the new score of the blob
   * @param node_id the node to reorganize the blob to
   * */
  void AsyncReorganizeBlobConstruct(ReorganizeBlobTask *task,
                                    const TaskNode &task_node,
                                    const TagId &tag_id,
                                    const hshm::charbuf &blob_name,
                                    const BlobId &blob_id,
                                    float score,
                                    bool user_score,
                                    const Context &ctx = Context(),
                                    u32 task_flags = TASK_LOW_LATENCY | TASK_FIRE_AND_FORGET) {
    // HILOG(kDebug, "Beginning REORGANIZE (task_node={})", task_node);
    HRUN_CLIENT->ConstructTask<ReorganizeBlobTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_name, blob_id, score, user_score, ctx, task_flags);
  }
  HRUN_TASK_NODE_PUSH_ROOT(ReorganizeBlob);

  /**
   * Tag a blob
   *
   * @param blob_id id of the blob being tagged
   * @param tag_name tag name
   * */
  void AsyncTagBlobConstruct(TagBlobTask *task,
                             const TaskNode &task_node,
                             const TagId &tag_id,
                             const BlobId &blob_id,
                             const TagId &tag) {
    HRUN_CLIENT->ConstructTask<TagBlobTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_id, tag);
  }
  void TagBlobRoot(const TagId &tag_id,
                   const BlobId &blob_id,
                   const TagId &tag) {
    LPointer<hrunpq::TypedPushTask<TagBlobTask>> push_task =
                                                     AsyncTagBlobRoot(tag_id, blob_id, tag);
    push_task->Wait();
    HRUN_CLIENT->DelTask(push_task);
  }
  HRUN_TASK_NODE_PUSH_ROOT(TagBlob);

  /**
   * Check if blob has a tag
   * */
  void AsyncBlobHasTagConstruct(BlobHasTagTask *task,
                                const TaskNode &task_node,
                                const TagId &tag_id,
                                const BlobId &blob_id,
                                const TagId &tag) {
    HRUN_CLIENT->ConstructTask<BlobHasTagTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_id, tag);
  }
  bool BlobHasTagRoot(const TagId &tag_id,
                      const BlobId &blob_id,
                      const TagId &tag) {
    LPointer<hrunpq::TypedPushTask<BlobHasTagTask>> push_task =
                                                        AsyncBlobHasTagRoot(tag_id, blob_id, tag);
    push_task->Wait();
    BlobHasTagTask *task = push_task->get();
    bool has_tag = task->has_tag_;
    HRUN_CLIENT->DelTask(push_task);
    return has_tag;
  }
  HRUN_TASK_NODE_PUSH_ROOT(BlobHasTag);

  /**
   * Get \a blob_name BLOB from \a bkt_id bucket
   * */
  void AsyncGetBlobIdConstruct(GetBlobIdTask *task,
                               const TaskNode &task_node,
                               const TagId &tag_id,
                               const hshm::charbuf &blob_name) {
    u32 hash = HashBlobName(tag_id, blob_name);
    HRUN_CLIENT->ConstructTask<GetBlobIdTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id, blob_name);
  }
  BlobId GetBlobIdRoot(const TagId &tag_id,
                       const hshm::charbuf &blob_name) {
    LPointer<hrunpq::TypedPushTask<GetBlobIdTask>> push_task =
                                                       AsyncGetBlobIdRoot(tag_id, blob_name);
    push_task->Wait();
    GetBlobIdTask *task = push_task->get();
    BlobId blob_id = task->blob_id_;
    HRUN_CLIENT->DelTask(push_task);
    return blob_id;
  }
  HRUN_TASK_NODE_PUSH_ROOT(GetBlobId);

  /**
   * Get \a blob_name BLOB name from \a blob_id BLOB id
   * */
  void AsyncGetBlobNameConstruct(GetBlobNameTask *task,
                                 const TaskNode &task_node,
                                 const TagId &tag_id,
                                 const BlobId &blob_id) {
    HRUN_CLIENT->ConstructTask<GetBlobNameTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_id);
  }
  std::string GetBlobNameRoot(const TagId &tag_id,
                              const BlobId &blob_id) {
    LPointer<hrunpq::TypedPushTask<GetBlobNameTask>> push_task =
                                                         AsyncGetBlobNameRoot(tag_id, blob_id);
    push_task->Wait();
    GetBlobNameTask *task = push_task->get();
    std::string blob_name = task->blob_name_->str();
    HRUN_CLIENT->DelTask(push_task);
    return blob_name;
  }
  HRUN_TASK_NODE_PUSH_ROOT(GetBlobName);

  /**
   * Get \a size from \a blob_id BLOB id
   * */
  void AsyncGetBlobSizeConstruct(GetBlobSizeTask *task,
                                 const TaskNode &task_node,
                                 const TagId &tag_id,
                                 const hshm::charbuf &blob_name,
                                 const BlobId &blob_id) {
    // HILOG(kDebug, "Getting blob size {}", task_node);
    HRUN_CLIENT->ConstructTask<GetBlobSizeTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_name, blob_id);
  }
  size_t GetBlobSizeRoot(const TagId &tag_id,
                         const hshm::charbuf &blob_name,
                         const BlobId &blob_id) {
    LPointer<hrunpq::TypedPushTask<GetBlobSizeTask>> push_task =
                                                         AsyncGetBlobSizeRoot(tag_id, blob_name, blob_id);
    push_task->Wait();
    GetBlobSizeTask *task = push_task->get();
    size_t size = task->size_;
    HRUN_CLIENT->DelTask(push_task);
    return size;
  }
  HRUN_TASK_NODE_PUSH_ROOT(GetBlobSize);

  /**
   * Get \a score from \a blob_id BLOB id
   * */
  void AsyncGetBlobScoreConstruct(GetBlobScoreTask *task,
                                  const TaskNode &task_node,
                                  const TagId &tag_id,
                                  const BlobId &blob_id) {
    HRUN_CLIENT->ConstructTask<GetBlobScoreTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_id);
  }
  float GetBlobScoreRoot(const TagId &tag_id,
                         const BlobId &blob_id) {
    LPointer<hrunpq::TypedPushTask<GetBlobScoreTask>> push_task =
                                                          AsyncGetBlobScoreRoot(tag_id, blob_id);
    push_task->Wait();
    GetBlobScoreTask *task = push_task->get();
    float score = task->score_;
    HRUN_CLIENT->DelTask(push_task);
    return score;
  }
  HRUN_TASK_NODE_PUSH_ROOT(GetBlobScore);

  /**
   * Get \a blob_id blob's buffers
   * */
  void AsyncGetBlobBuffersConstruct(GetBlobBuffersTask *task,
                                    const TaskNode &task_node,
                                    const TagId &tag_id,
                                    const BlobId &blob_id) {
    HRUN_CLIENT->ConstructTask<GetBlobBuffersTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_id);
  }
  std::vector<BufferInfo> GetBlobBuffersRoot(const TagId &tag_id,
                                             const BlobId &blob_id) {
    LPointer<hrunpq::TypedPushTask<GetBlobBuffersTask>> push_task = AsyncGetBlobBuffersRoot(tag_id, blob_id);
    push_task->Wait();
    GetBlobBuffersTask *task = push_task->get();
    std::vector<BufferInfo> buffers =
        hshm::to_stl_vector<BufferInfo>(*task->buffers_);
    HRUN_CLIENT->DelTask(push_task);
    return buffers;
  }
  HRUN_TASK_NODE_PUSH_ROOT(GetBlobBuffers)

  /**
   * Rename \a blob_id blob to \a new_blob_name new blob name
   * in \a bkt_id bucket.
   * */
  void AsyncRenameBlobConstruct(RenameBlobTask *task,
                                const TaskNode &task_node,
                                const TagId &tag_id,
                                const BlobId &blob_id,
                                const hshm::charbuf &new_blob_name) {
    HRUN_CLIENT->ConstructTask<RenameBlobTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_id, new_blob_name);
  }
  void RenameBlobRoot(const TagId &tag_id,
                      const BlobId &blob_id,
                      const hshm::charbuf &new_blob_name) {
    LPointer<hrunpq::TypedPushTask<RenameBlobTask>> push_task =
                                                        AsyncRenameBlobRoot(tag_id, blob_id, new_blob_name);
    push_task->Wait();
    HRUN_CLIENT->DelTask(push_task);
  }
  HRUN_TASK_NODE_PUSH_ROOT(RenameBlob);

  /**
   * Truncate a blob to a new size
   * */
  void AsyncTruncateBlobConstruct(TruncateBlobTask *task,
                                  const TaskNode &task_node,
                                  const TagId &tag_id,
                                  const BlobId &blob_id,
                                  size_t new_size) {
    HRUN_CLIENT->ConstructTask<TruncateBlobTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_id, new_size);
  }
  void TruncateBlobRoot(const TagId &tag_id,
                        const BlobId &blob_id,
                        size_t new_size) {
    LPointer<hrunpq::TypedPushTask<TruncateBlobTask>> push_task =
                                                          AsyncTruncateBlobRoot(tag_id, blob_id, new_size);
    push_task->Wait();
    HRUN_CLIENT->DelTask(push_task);
  }
  HRUN_TASK_NODE_PUSH_ROOT(TruncateBlob);

  /**
   * Destroy \a blob_id blob in \a bkt_id bucket
   * */
  void AsyncDestroyBlobConstruct(DestroyBlobTask *task,
                                 const TaskNode &task_node,
                                 const TagId &tag_id,
                                 const BlobId &blob_id,
                                 bool update_size = true) {
    HRUN_CLIENT->ConstructTask<DestroyBlobTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_),
        id_, tag_id, blob_id, update_size);
  }
  void DestroyBlobRoot(const TagId &tag_id,
                       const BlobId &blob_id,
                       bool update_size = true) {
    LPointer<hrunpq::TypedPushTask<DestroyBlobTask>> push_task =
                                                         AsyncDestroyBlobRoot(tag_id, blob_id, update_size);
    push_task->Wait();
    HRUN_CLIENT->DelTask(push_task);
  }
  HRUN_TASK_NODE_PUSH_ROOT(DestroyBlob);

  /** Initialize automatic flushing */
  void AsyncFlushDataConstruct(FlushDataTask *task,
                               const TaskNode &task_node,
                               size_t period_ms) {
    HRUN_CLIENT->ConstructTask<FlushDataTask>(
        task, task_node, id_, period_ms);
  }
  HRUN_TASK_NODE_PUSH_ROOT(FlushData);

  /**
   * Get all blob metadata
   * */
  void AsyncPollBlobMetadataConstruct(PollBlobMetadataTask *task,
                                      const TaskNode &task_node) {
    HRUN_CLIENT->ConstructTask<PollBlobMetadataTask>(
        task, task_node, id_);
  }
  std::vector<BlobInfo> PollBlobMetadataRoot() {
    LPointer<hrunpq::TypedPushTask<PollBlobMetadataTask>> push_task =
                                                              AsyncPollBlobMetadataRoot();
    push_task->Wait();
    PollBlobMetadataTask *task = push_task->get();
    std::vector<BlobInfo> blob_mdms =
        task->DeserializeBlobMetadata();
    HRUN_CLIENT->DelTask(push_task);
    return blob_mdms;
  }
  HRUN_TASK_NODE_PUSH_ROOT(PollBlobMetadata);

  /**
  * Get all target metadata
  * */
  void AsyncPollTargetMetadataConstruct(PollTargetMetadataTask *task,
                                        const TaskNode &task_node) {
    HRUN_CLIENT->ConstructTask<PollTargetMetadataTask>(
        task, task_node, id_);
  }
  std::vector<TargetStats> PollTargetMetadataRoot() {
    LPointer<hrunpq::TypedPushTask<PollTargetMetadataTask>> push_task =
                                                                AsyncPollTargetMetadataRoot();
    push_task->Wait();
    PollTargetMetadataTask *task = push_task->get();
    std::vector<TargetStats> target_mdms =
        task->DeserializeTargetMetadata();
    HRUN_CLIENT->DelTask(push_task);
    return target_mdms;
  }
  HRUN_TASK_NODE_PUSH_ROOT(PollTargetMetadata);
};

}  // namespace hrun

#endif  // HRUN_hermes_blob_mdm_H_