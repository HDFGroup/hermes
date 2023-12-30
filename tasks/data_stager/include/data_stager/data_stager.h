//
// Created by lukemartinlogan on 6/29/23.
//

#ifndef HRUN_data_stager_H_
#define HRUN_data_stager_H_

#include "data_stager_tasks.h"

namespace hermes::data_stager {

/** Create data_stager requests */
class Client : public TaskLibClient {
 public:
  /** Default constructor */
  Client() = default;

  /** Destructor */
  ~Client() = default;

  /** Async create a task state */
  HSHM_ALWAYS_INLINE
  LPointer<ConstructTask> AsyncCreate(const TaskNode &task_node,
                                      const DomainId &domain_id,
                                      const std::string &state_name,
                                      const TaskStateId &blob_mdm) {
    id_ = TaskStateId::GetNull();
    QueueManagerInfo &qm = HRUN_CLIENT->server_config_.queue_manager_;
    std::vector<PriorityInfo> queue_info;
    return HRUN_ADMIN->AsyncCreateTaskState<ConstructTask>(
        task_node, domain_id, state_name, id_, queue_info, blob_mdm);
  }
  HRUN_TASK_NODE_ROOT(AsyncCreate)
  template<typename ...Args>
  HSHM_ALWAYS_INLINE
  void CreateRoot(Args&& ...args) {
    LPointer<ConstructTask> task =
        AsyncCreateRoot(std::forward<Args>(args)...);
    task->Wait();
    id_ = task->id_;
    Init(id_, HRUN_ADMIN->queue_id_);
    HRUN_CLIENT->DelTask(task);
  }

  /** Destroy task state + queue */
  HSHM_ALWAYS_INLINE
  void DestroyRoot(const DomainId &domain_id) {
    HRUN_ADMIN->DestroyTaskStateRoot(domain_id, id_);
  }

  /** Register task state */
  HSHM_ALWAYS_INLINE
  void AsyncRegisterStagerConstruct(RegisterStagerTask *task,
                                    const TaskNode &task_node,
                                    const BucketId &bkt_id,
                                    const hshm::charbuf &path,
                                    const hshm::charbuf &params) {
    HRUN_CLIENT->ConstructTask<RegisterStagerTask>(
        task, task_node, id_, bkt_id, path, params);
  }
  HSHM_ALWAYS_INLINE
  void RegisterStagerRoot(const BucketId &bkt_id,
                          const hshm::charbuf &path,
                          const hshm::charbuf params) {
    LPointer<hrunpq::TypedPushTask<RegisterStagerTask>> task =
        AsyncRegisterStagerRoot(bkt_id, path, params);
    task.ptr_->Wait();
  }
  HRUN_TASK_NODE_PUSH_ROOT(RegisterStager);

  /** Unregister task state */
  HSHM_ALWAYS_INLINE
  void AsyncUnregisterStagerConstruct(UnregisterStagerTask *task,
                                      const TaskNode &task_node,
                                      const BucketId &bkt_id) {
    HRUN_CLIENT->ConstructTask<UnregisterStagerTask>(
        task, task_node, id_, bkt_id);
  }
  HSHM_ALWAYS_INLINE
  void UnregisterStagerRoot(const BucketId &bkt_id) {
    LPointer<hrunpq::TypedPushTask<UnregisterStagerTask>> task =
        AsyncUnregisterStagerRoot(bkt_id);
    task.ptr_->Wait();
  }
  HRUN_TASK_NODE_PUSH_ROOT(UnregisterStager);

  /** Stage in data from a remote source */
  HSHM_ALWAYS_INLINE
  void AsyncStageInConstruct(StageInTask *task,
                            const TaskNode &task_node,
                            const BucketId &bkt_id,
                            const hshm::charbuf &blob_name,
                            float score,
                            u32 node_id) {
    HRUN_CLIENT->ConstructTask<StageInTask>(
        task, task_node, id_, bkt_id,
        blob_name, score, node_id);
  }
  HSHM_ALWAYS_INLINE
  void StageInRoot(const BucketId &bkt_id,
               const hshm::charbuf &blob_name,
               float score,
               u32 node_id) {
    LPointer<hrunpq::TypedPushTask<StageInTask>> task =
        AsyncStageInRoot(bkt_id, blob_name, score, node_id);
    task.ptr_->Wait();
  }
  HRUN_TASK_NODE_PUSH_ROOT(StageIn);

  /** Stage out data to a remote source */
  HSHM_ALWAYS_INLINE
  void AsyncStageOutConstruct(StageOutTask *task,
                              const TaskNode &task_node,
                              const BucketId &bkt_id,
                              const hshm::charbuf &blob_name,
                              const hipc::Pointer &data,
                              size_t data_size,
                              u32 task_flags) {
    HRUN_CLIENT->ConstructTask<StageOutTask>(
        task, task_node, id_, bkt_id,
        blob_name, data, data_size, task_flags);
  }
  HSHM_ALWAYS_INLINE
  void StageOutRoot(const BucketId &bkt_id,
                    const hshm::charbuf &blob_name,
                    const hipc::Pointer &data,
                    size_t data_size,
                    u32 task_flags) {
    LPointer<hrunpq::TypedPushTask<StageOutTask>> task =
        AsyncStageOutRoot(bkt_id, blob_name, data, data_size, task_flags);
    task.ptr_->Wait();
  }
  HRUN_TASK_NODE_PUSH_ROOT(StageOut);

  /** Parse url */
  static inline
  bool GetUrlProtocolAndAction(const std::string &url,
                               std::string &protocol,
                               std::string &action,
                               std::vector<std::string> &tokens) {
    // Determine the protocol + action
    std::string token;
    size_t found = url.find("://");
    if (found == std::string::npos) {
      return false;
    }
    protocol = url.substr(0, found);
    action = url.substr(found + 3);

    // Divide action into tokens
    std::stringstream ss(action);
    while (std::getline(ss, token, ':')) {
      tokens.push_back(token);
    }
    return true;
  }

  /** Bucket name from url */
  static inline hshm::charbuf GetTagNameFromUrl(const hshm::string &url) {
    // Parse url
    std::string protocol, action;
    std::vector<std::string> tokens;
    bool ret = GetUrlProtocolAndAction(url.str(), protocol, action, tokens);
    if (ret) {
      std::string &path = tokens[0];
      return hshm::charbuf(path);
    } else {
      return url;
    }
  }
};

}  // namespace hrun

#endif  // HRUN_data_stager_H_
