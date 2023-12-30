//
// Created by lukemartinlogan on 6/29/23.
//

#ifndef HRUN_hermes_bucket_mdm_H_
#define HRUN_hermes_bucket_mdm_H_

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
    QueueManagerInfo &qm = HRUN_CLIENT->server_config_.queue_manager_;
    std::vector<PriorityInfo> queue_info;
    id_ = HRUN_ADMIN->CreateTaskStateRoot<ConstructTask>(
        domain_id, state_name, id_, queue_info);
    Init(id_, HRUN_ADMIN->queue_id_);
  }

  /** Destroy task state + queue */
  HSHM_ALWAYS_INLINE
  void DestroyRoot(const DomainId &domain_id) {
    HRUN_ADMIN->DestroyTaskStateRoot(domain_id, id_);
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
    HRUN_CLIENT->ConstructTask<SetBlobMdmTask>(
        task, task_node, domain_id, id_, blob_mdm, stager_mdm);
  }
  void SetBlobMdmRoot(const DomainId &domain_id,
                      const TaskStateId &blob_mdm,
                      const TaskStateId &stager_mdm) {
    LPointer<hrunpq::TypedPushTask<SetBlobMdmTask>> push_task =
        AsyncSetBlobMdmRoot(domain_id, blob_mdm, stager_mdm);
    push_task->Wait();
    HRUN_CLIENT->DelTask(push_task);
  }
  HRUN_TASK_NODE_PUSH_ROOT(SetBlobMdm);

  /** Update statistics after blob PUT (fire & forget) */
  HSHM_ALWAYS_INLINE
  void AsyncUpdateSizeConstruct(UpdateSizeTask *task,
                                const TaskNode &task_node,
                                TagId tag_id,
                                ssize_t update,
                                int mode) {
    HRUN_CLIENT->ConstructTask<UpdateSizeTask>(
        task, task_node, DomainId::GetNode(tag_id.node_id_), id_,
        tag_id, update, mode);
  }
  HRUN_TASK_NODE_PUSH_ROOT(UpdateSize);

  /** Append data to the bucket (fire & forget) */
  HSHM_ALWAYS_INLINE
  void AsyncAppendBlobSchemaConstruct(AppendBlobSchemaTask *task,
                                      const TaskNode &task_node,
                                      TagId tag_id,
                                      size_t data_size,
                                      size_t page_size) {
    HRUN_CLIENT->ConstructTask<AppendBlobSchemaTask>(
        task, task_node, DomainId::GetNode(tag_id.node_id_), id_,
        tag_id, data_size, page_size);
  }
  HRUN_TASK_NODE_PUSH_ROOT(AppendBlobSchema);

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
    HRUN_CLIENT->ConstructTask<AppendBlobTask>(
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
  HRUN_TASK_NODE_PUSH_ROOT(AppendBlob);

  /** Create a tag or get the ID of existing tag */
  HSHM_ALWAYS_INLINE
  void AsyncGetOrCreateTagConstruct(GetOrCreateTagTask *task,
                                    const TaskNode &task_node,
                                    const hshm::charbuf &tag_name,
                                    bool blob_owner,
                                    const std::vector<TraitId> &traits,
                                    size_t backend_size,
                                    u32 flags,
                                    const Context &ctx = Context()) {
    HILOG(kDebug, "Creating a tag {}", tag_name.str());
    HRUN_CLIENT->ConstructTask<GetOrCreateTagTask>(
        task, task_node, id_,
        tag_name, blob_owner, traits, backend_size, flags, ctx);
  }
  HSHM_ALWAYS_INLINE
  TagId GetOrCreateTagRoot(const hshm::charbuf &tag_name,
                           bool blob_owner,
                           const std::vector<TraitId> &traits,
                           size_t backend_size,
                           u32 flags,
                           const Context &ctx = Context()) {
    LPointer<hrunpq::TypedPushTask<GetOrCreateTagTask>> push_task =
        AsyncGetOrCreateTagRoot(tag_name, blob_owner, traits, backend_size, flags, ctx);
    push_task->Wait();
    GetOrCreateTagTask *task = push_task->get();
    TagId tag_id = task->tag_id_;
    HRUN_CLIENT->DelTask(push_task);
    return tag_id;
  }
  HRUN_TASK_NODE_PUSH_ROOT(GetOrCreateTag);

  /** Get tag ID */
  void AsyncGetTagIdConstruct(GetTagIdTask *task,
                              const TaskNode &task_node,
                              const hshm::charbuf &tag_name) {
    u32 hash = std::hash<hshm::charbuf>{}(tag_name);
    HRUN_CLIENT->ConstructTask<GetTagIdTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_name);
  }
  TagId GetTagIdRoot(const hshm::charbuf &tag_name) {
    LPointer<hrunpq::TypedPushTask<GetTagIdTask>> push_task =
        AsyncGetTagIdRoot(tag_name);
    push_task->Wait();
    GetTagIdTask *task = push_task->get();
    TagId tag_id = task->tag_id_;
    HRUN_CLIENT->DelTask(push_task);
    return tag_id;
  }
  HRUN_TASK_NODE_PUSH_ROOT(GetTagId);

  /** Get tag name */
  void AsyncGetTagNameConstruct(GetTagNameTask *task,
                                const TaskNode &task_node,
                                const TagId &tag_id) {
    u32 hash = tag_id.hash_;
    HRUN_CLIENT->ConstructTask<GetTagNameTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id);
  }
  hshm::string GetTagNameRoot(const TagId &tag_id) {
    LPointer<hrunpq::TypedPushTask<GetTagNameTask>> push_task =
        AsyncGetTagNameRoot(tag_id);
    push_task->Wait();
    GetTagNameTask *task = push_task->get();
    hshm::string tag_name = hshm::to_charbuf<hipc::string>(*task->tag_name_.get());
    HRUN_CLIENT->DelTask(push_task);
    return tag_name;
  }
  HRUN_TASK_NODE_PUSH_ROOT(GetTagName);

  /** Rename tag */
  void AsyncRenameTagConstruct(RenameTagTask *task,
                               const TaskNode &task_node,
                               const TagId &tag_id,
                               const hshm::charbuf &new_tag_name) {
    u32 hash = tag_id.hash_;
    HRUN_CLIENT->ConstructTask<RenameTagTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id, new_tag_name);
  }
  void RenameTagRoot(const TagId &tag_id, const hshm::charbuf &new_tag_name) {
    LPointer<hrunpq::TypedPushTask<RenameTagTask>> push_task =
        AsyncRenameTagRoot(tag_id, new_tag_name);
    push_task->Wait();
    HRUN_CLIENT->DelTask(push_task);
  }
  HRUN_TASK_NODE_PUSH_ROOT(RenameTag);

  /** Destroy tag */
  void AsyncDestroyTagConstruct(DestroyTagTask *task,
                                const TaskNode &task_node,
                                const TagId &tag_id) {
    u32 hash = tag_id.hash_;
    HRUN_CLIENT->ConstructTask<DestroyTagTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id);
  }
  void DestroyTagRoot(const TagId &tag_id) {
    LPointer<hrunpq::TypedPushTask<DestroyTagTask>> push_task =
        AsyncDestroyTagRoot(tag_id);
    push_task->Wait();
    HRUN_CLIENT->DelTask(push_task);
  }
  HRUN_TASK_NODE_PUSH_ROOT(DestroyTag);

  /** Add a blob to a tag */
  void AsyncTagAddBlobConstruct(TagAddBlobTask *task,
                                const TaskNode &task_node,
                                const TagId &tag_id,
                                const BlobId &blob_id) {
    u32 hash = tag_id.hash_;
    HRUN_CLIENT->ConstructTask<TagAddBlobTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id, blob_id);
  }
  void TagAddBlobRoot(const TagId &tag_id, const BlobId &blob_id) {
    LPointer<hrunpq::TypedPushTask<TagAddBlobTask>> push_task =
        AsyncTagAddBlobRoot(tag_id, blob_id);
    push_task->Wait();
    HRUN_CLIENT->DelTask(push_task);
  }
  HRUN_TASK_NODE_PUSH_ROOT(TagAddBlob);

  /** Remove a blob from a tag */
  void AsyncTagRemoveBlobConstruct(TagRemoveBlobTask *task,
                                   const TaskNode &task_node,
                                   const TagId &tag_id, const BlobId &blob_id) {
    u32 hash = tag_id.hash_;
    HRUN_CLIENT->ConstructTask<TagRemoveBlobTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id, blob_id);
  }
  void TagRemoveBlobRoot(const TagId &tag_id, const BlobId &blob_id) {
    LPointer<hrunpq::TypedPushTask<TagRemoveBlobTask>> push_task =
        AsyncTagRemoveBlobRoot(tag_id, blob_id);
    push_task->Wait();
    HRUN_CLIENT->DelTask(push_task);
  }
  HRUN_TASK_NODE_PUSH_ROOT(TagRemoveBlob);

  /** Clear blobs from a tag */
  void AsyncTagClearBlobsConstruct(TagClearBlobsTask *task,
                                   const TaskNode &task_node,
                                   const TagId &tag_id) {
    u32 hash = tag_id.hash_;
    HRUN_CLIENT->ConstructTask<TagClearBlobsTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id);
  }
  void TagClearBlobsRoot(const TagId &tag_id) {
    LPointer<hrunpq::TypedPushTask<TagClearBlobsTask>> push_task =
        AsyncTagClearBlobsRoot(tag_id);
    push_task->Wait();
    HRUN_CLIENT->DelTask(push_task);
  }
  HRUN_TASK_NODE_PUSH_ROOT(TagClearBlobs);

  /** Get the size of a bucket */
  void AsyncGetSizeConstruct(GetSizeTask *task,
                             const TaskNode &task_node,
                             const TagId &tag_id) {
    u32 hash = tag_id.hash_;
    HRUN_CLIENT->ConstructTask<GetSizeTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id);
  }
  size_t GetSizeRoot(const TagId &tag_id) {
    LPointer<hrunpq::TypedPushTask<GetSizeTask>> push_task =
        AsyncGetSizeRoot(tag_id);
    push_task->Wait();
    GetSizeTask *task = push_task->get();
    size_t size = task->size_;
    HRUN_CLIENT->DelTask(push_task);
    return size;
  }
  HRUN_TASK_NODE_PUSH_ROOT(GetSize);

  /** Get contained blob ids */
  void AsyncGetContainedBlobIdsConstruct(GetContainedBlobIdsTask *task,
                             const TaskNode &task_node,
                             const TagId &tag_id) {
    u32 hash = tag_id.hash_;
    HRUN_CLIENT->ConstructTask<GetContainedBlobIdsTask>(
        task, task_node, DomainId::GetNode(HASH_TO_NODE_ID(hash)), id_,
        tag_id);
  }
  std::vector<BlobId> GetContainedBlobIdsRoot(const TagId &tag_id) {
    LPointer<hrunpq::TypedPushTask<GetContainedBlobIdsTask>> push_task =
        AsyncGetContainedBlobIdsRoot(tag_id);
    push_task->Wait();
    GetContainedBlobIdsTask *task = push_task->get();
    std::vector<BlobId> blob_ids = task->blob_ids_->vec();
    HRUN_CLIENT->DelTask(push_task);
    return blob_ids;
  }
  HRUN_TASK_NODE_PUSH_ROOT(GetContainedBlobIds);

  /**
  * Get all tag metadata
  * */
  void AsyncPollTagMetadataConstruct(PollTagMetadataTask *task,
                                        const TaskNode &task_node) {
    HRUN_CLIENT->ConstructTask<PollTagMetadataTask>(
        task, task_node, id_);
  }
  std::vector<TagInfo> PollTagMetadataRoot() {
    LPointer<hrunpq::TypedPushTask<PollTagMetadataTask>> push_task =
        AsyncPollTagMetadataRoot();
    push_task->Wait();
    PollTagMetadataTask *task = push_task->get();
    std::vector<TagInfo> target_mdms =
        task->DeserializeTagMetadata();
    HRUN_CLIENT->DelTask(push_task);
    return target_mdms;
  }
  HRUN_TASK_NODE_PUSH_ROOT(PollTagMetadata);
};

}  // namespace hrun

#endif  // HRUN_hermes_bucket_mdm_H_
