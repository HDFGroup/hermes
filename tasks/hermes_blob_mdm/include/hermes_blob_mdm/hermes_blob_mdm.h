//
// Created by lukemartinlogan on 6/29/23.
//

#ifndef LABSTOR_hermes_blob_mdm_H_
#define LABSTOR_hermes_blob_mdm_H_

#include "hermes_blob_mdm_tasks.h"

namespace hermes::blob_mdm {

/** Create hermes_blob_mdm requests */
class Client : public TaskLibClient {

 public:
  /** Default constructor */
  Client() = default;

  /** Destructor */
  ~Client() = default;

  /** Initialize directly using TaskStateId */
  void Init(const TaskStateId &id) {
    id_ = id;
    queue_id_ = QueueId(id_);
  }

  /** Create a hermes_blob_mdm */
  HSHM_ALWAYS_INLINE
  LPointer<ConstructTask> AsyncCreate(const TaskNode &task_node,
                                      const DomainId &domain_id,
                                      const std::string &state_name) {
    id_ = TaskStateId::GetNull();
    QueueManagerInfo &qm = LABSTOR_CLIENT->server_config_.queue_manager_;
    std::vector<PriorityInfo> queue_info = {
        {1, 1, qm.queue_depth_, 0},
        {1, 1, qm.queue_depth_, QUEUE_LONG_RUNNING},
        {qm.max_lanes_, qm.max_lanes_, qm.queue_depth_, QUEUE_LOW_LATENCY}
    };
    return LABSTOR_ADMIN->AsyncCreateTaskState<ConstructTask>(
        task_node, domain_id, state_name, id_, queue_info);
  }
  void AsyncCreateComplete(ConstructTask *task) {
    if (task->IsComplete()) {
      id_ = task->id_;
      queue_id_ = QueueId(id_);
      LABSTOR_CLIENT->DelTask(task);
    }
  }
  LABSTOR_TASK_NODE_ROOT(AsyncCreate);
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
    LABSTOR_ADMIN->DestroyTaskStateRoot(domain_id, id_);
  }

  /**====================================
   * Blob Operations
   * ===================================*/

  /** Sets the BUCKET MDM */
  void AsyncSetBucketMdmConstruct(SetBucketMdmTask *task,
                                  const TaskNode &task_node,
                                  const DomainId &domain_id,
                                  const TaskStateId &blob_mdm_id) {
    LABSTOR_CLIENT->ConstructTask<SetBucketMdmTask>(
        task, task_node, domain_id, id_, blob_mdm_id);
  }
  void SetBucketMdmRoot(const DomainId &domain_id,
                        const TaskStateId &blob_mdm_id) {
    LPointer<labpq::TypedPushTask<SetBucketMdmTask>> push_task =
        AsyncSetBucketMdmRoot(domain_id, blob_mdm_id);
    push_task->Wait();
    LABSTOR_CLIENT->DelTask(push_task);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(SetBucketMdm);

  /**
   * Get \a blob_name BLOB from \a bkt_id bucket
   * */
  void AsyncGetOrCreateBlobIdConstruct(GetOrCreateBlobIdTask* task,
                                       const TaskNode &task_node,
                                       TagId tag_id,
                                       const hshm::charbuf &blob_name) {
    u32 hash = std::hash<hshm::charbuf>{}(blob_name);
    LABSTOR_CLIENT->ConstructTask<GetOrCreateBlobIdTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id, blob_name);
  }
  BlobId GetOrCreateBlobIdRoot(TagId tag_id, const hshm::charbuf &blob_name) {
    LPointer<labpq::TypedPushTask<GetOrCreateBlobIdTask>> push_task =
        AsyncGetOrCreateBlobIdRoot(tag_id, blob_name);
    push_task->Wait();
    GetOrCreateBlobIdTask *task = push_task->get();
    BlobId blob_id = task->blob_id_;
    LABSTOR_CLIENT->DelTask(push_task);
    return blob_id;
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(GetOrCreateBlobId);

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
      BlobId &blob_id, size_t blob_off, size_t blob_size,
      const hipc::Pointer &blob, float score,
      bitfield32_t flags,
      Context ctx = Context(),
      bitfield32_t task_flags = bitfield32_t(TASK_FIRE_AND_FORGET | TASK_DATA_OWNER | TASK_LOW_LATENCY)) {
    LABSTOR_CLIENT->ConstructTask<PutBlobTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_name, blob_id,
        blob_off, blob_size,
        blob, score, flags, ctx, task_flags);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(PutBlob);

  /** Get a blob's data */
  void AsyncGetBlobConstruct(GetBlobTask *task,
                             const TaskNode &task_node,
                             const TagId &tag_id,
                             const std::string &blob_name,
                             const BlobId &blob_id,
                             size_t off,
                             ssize_t data_size,
                             hipc::Pointer &data,
                             Context ctx = Context(),
                             bitfield32_t flags = bitfield32_t(0)) {
    HILOG(kDebug, "Beginning GET (task_node={})", task_node);
    LABSTOR_CLIENT->ConstructTask<GetBlobTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_name, blob_id, off, data_size, data, ctx, flags);
  }
  size_t GetBlobRoot(const TagId &tag_id,
                     const BlobId &blob_id,
                     size_t off,
                     ssize_t data_size,
                     hipc::Pointer &data,
                     Context ctx = Context(),
                     bitfield32_t flags = bitfield32_t(0)) {
    LPointer<labpq::TypedPushTask<GetBlobTask>> push_task =
        AsyncGetBlobRoot(tag_id, "", blob_id, off, data_size, data, ctx, flags);
    push_task->Wait();
    GetBlobTask *task = push_task->get();
    data = task->data_;
    size_t true_size = task->data_size_;
    LABSTOR_CLIENT->DelTask(push_task);
    return true_size;
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(GetBlob);

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
                                    const BlobId &blob_id,
                                    float score,
                                    u32 node_id) {
    HILOG(kDebug, "Beginning REORGANIZE (task_node={})", task_node);
    LABSTOR_CLIENT->ConstructTask<ReorganizeBlobTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_id, score, node_id);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(ReorganizeBlob);

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
    LABSTOR_CLIENT->ConstructTask<TagBlobTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_id, tag);
  }
  void TagBlobRoot(const TagId &tag_id,
                   const BlobId &blob_id,
                   const TagId &tag) {
    LPointer<labpq::TypedPushTask<TagBlobTask>> push_task =
       AsyncTagBlobRoot(tag_id, blob_id, tag);
    push_task->Wait();
    LABSTOR_CLIENT->DelTask(push_task);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(TagBlob);

  /**
   * Check if blob has a tag
   * */
  void AsyncBlobHasTagConstruct(BlobHasTagTask *task,
                                const TaskNode &task_node,
                                const TagId &tag_id,
                                const BlobId &blob_id,
                                const TagId &tag) {
    LABSTOR_CLIENT->ConstructTask<BlobHasTagTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_id, tag);
  }
  bool BlobHasTagRoot(const TagId &tag_id,
                      const BlobId &blob_id,
                      const TagId &tag) {
    LPointer<labpq::TypedPushTask<BlobHasTagTask>> push_task =
        AsyncBlobHasTagRoot(tag_id, blob_id, tag);
    push_task->Wait();
    BlobHasTagTask *task = push_task->get();
    bool has_tag = task->has_tag_;
    LABSTOR_CLIENT->DelTask(push_task);
    return has_tag;
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(BlobHasTag);

  /**
   * Get \a blob_name BLOB from \a bkt_id bucket
   * */
  void AsyncGetBlobIdConstruct(GetBlobIdTask *task,
                               const TaskNode &task_node,
                               const TagId &tag_id,
                               const hshm::charbuf &blob_name) {
    u32 hash = std::hash<hshm::charbuf>{}(blob_name);
    LABSTOR_CLIENT->ConstructTask<GetBlobIdTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id, blob_name);
  }
  BlobId GetBlobIdRoot(const TagId &tag_id,
                       const hshm::charbuf &blob_name) {
    LPointer<labpq::TypedPushTask<GetBlobIdTask>> push_task =
        AsyncGetBlobIdRoot(tag_id, blob_name);
    push_task->Wait();
    GetBlobIdTask *task = push_task->get();
    BlobId blob_id = task->blob_id_;
    LABSTOR_CLIENT->DelTask(push_task);
    return blob_id;
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(GetBlobId);

  /**
   * Get \a blob_name BLOB name from \a blob_id BLOB id
   * */
  void AsyncGetBlobNameConstruct(GetBlobNameTask *task,
                                 const TaskNode &task_node,
                                 const TagId &tag_id,
                                 const BlobId &blob_id) {
    LABSTOR_CLIENT->ConstructTask<GetBlobNameTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_id);
  }
  std::string GetBlobNameRoot(const TagId &tag_id,
                              const BlobId &blob_id) {
    LPointer<labpq::TypedPushTask<GetBlobNameTask>> push_task =
        AsyncGetBlobNameRoot(tag_id, blob_id);
    push_task->Wait();
    GetBlobNameTask *task = push_task->get();
    std::string blob_name = task->blob_name_->str();
    LABSTOR_CLIENT->DelTask(push_task);
    return blob_name;
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(GetBlobName);

  /**
   * Get \a size from \a blob_id BLOB id
   * */
  void AsyncGetBlobSizeConstruct(GetBlobSizeTask *task,
                                 const TaskNode &task_node,
                                 const TagId &tag_id,
                                 const BlobId &blob_id) {
    HILOG(kDebug, "Getting blob size {}", task_node);
    LABSTOR_CLIENT->ConstructTask<GetBlobSizeTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_id);
  }
  size_t GetBlobSizeRoot(const TagId &tag_id,
                         const BlobId &blob_id) {
    LPointer<labpq::TypedPushTask<GetBlobSizeTask>> push_task =
        AsyncGetBlobSizeRoot(tag_id, blob_id);
    push_task->Wait();
    GetBlobSizeTask *task = push_task->get();
    size_t size = task->size_;
    LABSTOR_CLIENT->DelTask(push_task);
    return size;
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(GetBlobSize);

  /**
   * Get \a score from \a blob_id BLOB id
   * */
  void AsyncGetBlobScoreConstruct(GetBlobScoreTask *task,
                                  const TaskNode &task_node,
                                  const TagId &tag_id,
                                  const BlobId &blob_id) {
    LABSTOR_CLIENT->ConstructTask<GetBlobScoreTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_id);
  }
  float GetBlobScoreRoot(const TagId &tag_id,
                         const BlobId &blob_id) {
    LPointer<labpq::TypedPushTask<GetBlobScoreTask>> push_task =
        AsyncGetBlobScoreRoot(tag_id, blob_id);
    push_task->Wait();
    GetBlobScoreTask *task = push_task->get();
    float score = task->score_;
    LABSTOR_CLIENT->DelTask(push_task);
    return score;
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(GetBlobScore);

  /**
   * Get \a blob_id blob's buffers
   * */
  void AsyncGetBlobBuffersConstruct(GetBlobBuffersTask *task,
                                    const TaskNode &task_node,
                                    const TagId &tag_id,
                                    const BlobId &blob_id) {
    LABSTOR_CLIENT->ConstructTask<GetBlobBuffersTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_id);
  }
  std::vector<BufferInfo> GetBlobBuffersRoot(const TagId &tag_id,
                                             const BlobId &blob_id) {
    LPointer<labpq::TypedPushTask<GetBlobBuffersTask>> push_task = AsyncGetBlobBuffersRoot(tag_id, blob_id);
    push_task->Wait();
    GetBlobBuffersTask *task = push_task->get();
    std::vector<BufferInfo> buffers =
        hshm::to_stl_vector<BufferInfo>(*task->buffers_);
    LABSTOR_CLIENT->DelTask(push_task);
    return buffers;
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(GetBlobBuffers)

  /**
   * Rename \a blob_id blob to \a new_blob_name new blob name
   * in \a bkt_id bucket.
   * */
  void AsyncRenameBlobConstruct(RenameBlobTask *task,
                                const TaskNode &task_node,
                                const TagId &tag_id,
                                const BlobId &blob_id,
                                const hshm::charbuf &new_blob_name) {
    LABSTOR_CLIENT->ConstructTask<RenameBlobTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_id, new_blob_name);
  }
  void RenameBlobRoot(const TagId &tag_id,
                      const BlobId &blob_id,
                      const hshm::charbuf &new_blob_name) {
    LPointer<labpq::TypedPushTask<RenameBlobTask>> push_task =
        AsyncRenameBlobRoot(tag_id, blob_id, new_blob_name);
    push_task->Wait();
    LABSTOR_CLIENT->DelTask(push_task);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(RenameBlob);

  /**
   * Truncate a blob to a new size
   * */
  void AsyncTruncateBlobConstruct(TruncateBlobTask *task,
                                  const TaskNode &task_node,
                                  const TagId &tag_id,
                                  const BlobId &blob_id,
                                  size_t new_size) {
    LABSTOR_CLIENT->ConstructTask<TruncateBlobTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_), id_,
        tag_id, blob_id, new_size);
  }
  void TruncateBlobRoot(const TagId &tag_id,
                        const BlobId &blob_id,
                        size_t new_size) {
    LPointer<labpq::TypedPushTask<TruncateBlobTask>> push_task =
        AsyncTruncateBlobRoot(tag_id, blob_id, new_size);
    push_task->Wait();
    LABSTOR_CLIENT->DelTask(push_task);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(TruncateBlob);

  /**
   * Destroy \a blob_id blob in \a bkt_id bucket
   * */
  void AsyncDestroyBlobConstruct(DestroyBlobTask *task,
                                 const TaskNode &task_node,
                                 const TagId &tag_id,
                                 const BlobId &blob_id) {
    LABSTOR_CLIENT->ConstructTask<DestroyBlobTask>(
        task, task_node, DomainId::GetNode(blob_id.node_id_),
        id_, tag_id, blob_id);
  }
  void DestroyBlobRoot(const TagId &tag_id,
                       const BlobId &blob_id) {
    LPointer<labpq::TypedPushTask<DestroyBlobTask>> push_task =
        AsyncDestroyBlobRoot(tag_id, blob_id);
    push_task->Wait();
    LABSTOR_CLIENT->DelTask(push_task);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(DestroyBlob);
};

}  // namespace labstor

#endif  // LABSTOR_hermes_blob_mdm_H_
