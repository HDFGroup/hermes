//
// Created by lukemartinlogan on 6/29/23.
//

#ifndef LABSTOR_hermes_bucket_mdm_H_
#define LABSTOR_hermes_bucket_mdm_H_

#include "hermes_bucket_mdm_tasks.h"

namespace hermes::bucket_mdm {

/** Create hermes_bucket_mdm requests */
class Client : public TaskLibClient {
 public:
  /** Default constructor */
  Client() = default;

  /** Destructor */
  ~Client() = default;

  /** Create a hermes_bucket_mdm */
  HSHM_ALWAYS_INLINE
  void CreateRoot(const DomainId &domain_id,
                  const std::string &state_name) {
    id_ = TaskStateId::GetNull();
    QueueManagerInfo &qm = LABSTOR_CLIENT->server_config_.queue_manager_;
    std::vector<PriorityInfo> queue_info = {
        {1, 1, qm.queue_depth_, 0},
        {1, 1, qm.queue_depth_, QUEUE_LONG_RUNNING},
        {qm.max_lanes_, qm.max_lanes_, qm.queue_depth_, QUEUE_LOW_LATENCY}
    };
    id_ = LABSTOR_ADMIN->CreateTaskStateRoot<ConstructTask>(
        domain_id, state_name, id_, queue_info);
    queue_id_ = QueueId(id_);
  }

  /** Destroy task state + queue */
  HSHM_ALWAYS_INLINE
  void DestroyRoot(const DomainId &domain_id) {
    LABSTOR_ADMIN->DestroyTaskStateRoot(domain_id, id_);
  }

  /**====================================
   * Tag Operations
   * ===================================*/

  /** Sets the BLOB MDM */
  void AsyncSetBlobMdmConstruct(SetBlobMdmTask *task,
                                const TaskNode &task_node,
                                const DomainId &domain_id,
                                const TaskStateId &blob_mdm,
                                const TaskStateId &stager_mdm) {
    LABSTOR_CLIENT->ConstructTask<SetBlobMdmTask>(
        task, task_node, domain_id, id_, blob_mdm, stager_mdm);
  }
  void SetBlobMdmRoot(const DomainId &domain_id,
                      const TaskStateId &blob_mdm,
                      const TaskStateId &stager_mdm) {
    LPointer<labpq::TypedPushTask<SetBlobMdmTask>> push_task =
        AsyncSetBlobMdmRoot(domain_id, blob_mdm, stager_mdm);
    push_task->Wait();
    LABSTOR_CLIENT->DelTask(push_task);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(SetBlobMdm);

  /** Update statistics after blob PUT (fire & forget) */
  HSHM_ALWAYS_INLINE
  void AsyncUpdateSizeConstruct(UpdateSizeTask *task,
                                const TaskNode &task_node,
                                TagId tag_id,
                                size_t update,
                                int mode) {
    LABSTOR_CLIENT->ConstructTask<UpdateSizeTask>(
        task, task_node, DomainId::GetNode(tag_id.node_id_), id_,
        tag_id, update, mode);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(UpdateSize);

  /** Append data to the bucket (fire & forget) */
  HSHM_ALWAYS_INLINE
  void AsyncAppendBlobSchemaConstruct(AppendBlobSchemaTask *task,
                                      const TaskNode &task_node,
                                      TagId tag_id,
                                      size_t data_size,
                                      size_t page_size) {
    LABSTOR_CLIENT->ConstructTask<AppendBlobSchemaTask>(
        task, task_node, DomainId::GetNode(tag_id.node_id_), id_,
        tag_id, data_size, page_size);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(AppendBlobSchema);

  /** Append data to the bucket (fire & forget) */
  HSHM_ALWAYS_INLINE
  void AsyncAppendBlobConstruct(
      AppendBlobTask *task,
      const TaskNode &task_node,
      TagId tag_id,
      size_t data_size,
      const hipc::Pointer &data,
      size_t page_size,
      float score,
      u32 node_id,
      const Context &ctx) {
    LABSTOR_CLIENT->ConstructTask<AppendBlobTask>(
        task, task_node, DomainId::GetLocal(), id_,
        tag_id, data_size, data, page_size, score, node_id, ctx);
  }
  HSHM_ALWAYS_INLINE
  void AppendBlobRoot(TagId tag_id,
                      size_t data_size,
                      const hipc::Pointer &data,
                      size_t page_size,
                      float score,
                      u32 node_id,
                      const Context &ctx) {
    AsyncAppendBlobRoot(tag_id, data_size, data, page_size, score, node_id, ctx);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(AppendBlob);

  /** Create a tag or get the ID of existing tag */
  HSHM_ALWAYS_INLINE
  void AsyncGetOrCreateTagConstruct(GetOrCreateTagTask *task,
                                    const TaskNode &task_node,
                                    const hshm::charbuf &tag_name,
                                    bool blob_owner,
                                    const std::vector<TraitId> &traits,
                                    size_t backend_size,
                                    u32 flags) {
    HILOG(kDebug, "Creating a tag {}", tag_name.str());
    u32 hash = std::hash<hshm::charbuf>{}(tag_name);
    LABSTOR_CLIENT->ConstructTask<GetOrCreateTagTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_name, blob_owner, traits, backend_size, flags);
  }
  HSHM_ALWAYS_INLINE
  TagId GetOrCreateTagRoot(const hshm::charbuf &tag_name,
                           bool blob_owner,
                           const std::vector<TraitId> &traits,
                           size_t backend_size,
                           u32 flags) {
    LPointer<labpq::TypedPushTask<GetOrCreateTagTask>> push_task = 
        AsyncGetOrCreateTagRoot(tag_name, blob_owner, traits, backend_size, flags);
    push_task->Wait();
    GetOrCreateTagTask *task = push_task->get();
    TagId tag_id = task->tag_id_;
    LABSTOR_CLIENT->DelTask(push_task);
    return tag_id;
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(GetOrCreateTag);

  /** Get tag ID */
  void AsyncGetTagIdConstruct(GetTagIdTask *task,
                              const TaskNode &task_node,
                              const hshm::charbuf &tag_name) {
    u32 hash = std::hash<hshm::charbuf>{}(tag_name);
    LABSTOR_CLIENT->ConstructTask<GetTagIdTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_name);
  }
  TagId GetTagIdRoot(const hshm::charbuf &tag_name) {
    LPointer<labpq::TypedPushTask<GetTagIdTask>> push_task = 
        AsyncGetTagIdRoot(tag_name);
    push_task->Wait();
    GetTagIdTask *task = push_task->get();
    TagId tag_id = task->tag_id_;
    LABSTOR_CLIENT->DelTask(push_task);
    return tag_id;
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(GetTagId);

  /** Get tag name */
  void AsyncGetTagNameConstruct(GetTagNameTask *task,
                                const TaskNode &task_node,
                                const TagId &tag_id) {
    u32 hash = tag_id.hash_;
    LABSTOR_CLIENT->ConstructTask<GetTagNameTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id);
  }
  hshm::string GetTagNameRoot(const TagId &tag_id) {
    LPointer<labpq::TypedPushTask<GetTagNameTask>> push_task = 
        AsyncGetTagNameRoot(tag_id);
    push_task->Wait();
    GetTagNameTask *task = push_task->get();
    hshm::string tag_name = hshm::to_charbuf<hipc::string>(*task->tag_name_.get());
    LABSTOR_CLIENT->DelTask(push_task);
    return tag_name;
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(GetTagName);

  /** Rename tag */
  void AsyncRenameTagConstruct(RenameTagTask *task,
                               const TaskNode &task_node,
                               const TagId &tag_id,
                               const hshm::charbuf &new_tag_name) {
    u32 hash = tag_id.hash_;
    LABSTOR_CLIENT->ConstructTask<RenameTagTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id, new_tag_name);
  }
  void RenameTagRoot(const TagId &tag_id, const hshm::charbuf &new_tag_name) {
    LPointer<labpq::TypedPushTask<RenameTagTask>> push_task = 
        AsyncRenameTagRoot(tag_id, new_tag_name);
    push_task->Wait();
    LABSTOR_CLIENT->DelTask(push_task);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(RenameTag);

  /** Destroy tag */
  void AsyncDestroyTagConstruct(DestroyTagTask *task,
                                const TaskNode &task_node,
                                const TagId &tag_id) {
    u32 hash = tag_id.hash_;
    LABSTOR_CLIENT->ConstructTask<DestroyTagTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id);
  }
  void DestroyTagRoot(const TagId &tag_id) {
    LPointer<labpq::TypedPushTask<DestroyTagTask>> push_task = 
        AsyncDestroyTagRoot(tag_id);
    push_task->Wait();
    LABSTOR_CLIENT->DelTask(push_task);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(DestroyTag);

  /** Add a blob to a tag */
  void AsyncTagAddBlobConstruct(TagAddBlobTask *task,
                                const TaskNode &task_node,
                                const TagId &tag_id,
                                const BlobId &blob_id) {
    u32 hash = tag_id.hash_;
    LABSTOR_CLIENT->ConstructTask<TagAddBlobTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id, blob_id);
  }
  void TagAddBlobRoot(const TagId &tag_id, const BlobId &blob_id) {
    LPointer<labpq::TypedPushTask<TagAddBlobTask>> push_task = 
        AsyncTagAddBlobRoot(tag_id, blob_id);
    push_task->Wait();
    LABSTOR_CLIENT->DelTask(push_task);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(TagAddBlob);

  /** Remove a blob from a tag */
  void AsyncTagRemoveBlobConstruct(TagRemoveBlobTask *task,
                                   const TaskNode &task_node,
                                   const TagId &tag_id, const BlobId &blob_id) {
    u32 hash = tag_id.hash_;
    LABSTOR_CLIENT->ConstructTask<TagRemoveBlobTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id, blob_id);
  }
  void TagRemoveBlobRoot(const TagId &tag_id, const BlobId &blob_id) {
    LPointer<labpq::TypedPushTask<TagRemoveBlobTask>> push_task = 
        AsyncTagRemoveBlobRoot(tag_id, blob_id);
    push_task->Wait();
    LABSTOR_CLIENT->DelTask(push_task);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(TagRemoveBlob);

  /** Clear blobs from a tag */
  void AsyncTagClearBlobsConstruct(TagClearBlobsTask *task,
                                   const TaskNode &task_node,
                                   const TagId &tag_id) {
    u32 hash = tag_id.hash_;
    LABSTOR_CLIENT->ConstructTask<TagClearBlobsTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id);
  }
  void TagClearBlobsRoot(const TagId &tag_id) {
    LPointer<labpq::TypedPushTask<TagClearBlobsTask>> push_task = 
        AsyncTagClearBlobsRoot(tag_id);
    push_task->Wait();
    LABSTOR_CLIENT->DelTask(push_task);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(TagClearBlobs);

  /** Get the size of a bucket */
  void AsyncGetSizeConstruct(GetSizeTask *task,
                             const TaskNode &task_node,
                             const TagId &tag_id) {
    u32 hash = tag_id.hash_;
    LABSTOR_CLIENT->ConstructTask<GetSizeTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id);
  }
  size_t GetSizeRoot(const TagId &tag_id) {
    LPointer<labpq::TypedPushTask<GetSizeTask>> push_task =
        AsyncGetSizeRoot(tag_id);
    push_task->Wait();
    GetSizeTask *task = push_task->get();
    size_t size = task->size_;
    LABSTOR_CLIENT->DelTask(push_task);
    return size;
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(GetSize);

  /** Get contained blob ids */
  void AsyncGetContainedBlobIdsConstruct(GetContainedBlobIdsTask *task,
                             const TaskNode &task_node,
                             const TagId &tag_id) {
    u32 hash = tag_id.hash_;
    LABSTOR_CLIENT->ConstructTask<GetContainedBlobIdsTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id);
  }
  std::vector<BlobId> GetContainedBlobIdsRoot(const TagId &tag_id) {
    LPointer<labpq::TypedPushTask<GetContainedBlobIdsTask>> push_task =
        AsyncGetContainedBlobIdsRoot(tag_id);
    push_task->Wait();
    GetContainedBlobIdsTask *task = push_task->get();
    std::vector<BlobId> blob_ids = task->blob_ids_->vec();
    LABSTOR_CLIENT->DelTask(push_task);
    return blob_ids;
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(GetContainedBlobIds);
};

}  // namespace labstor

#endif  // LABSTOR_hermes_bucket_mdm_H_
