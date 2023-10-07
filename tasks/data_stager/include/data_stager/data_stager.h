//
// Created by lukemartinlogan on 6/29/23.
//

#ifndef LABSTOR_data_stager_H_
#define LABSTOR_data_stager_H_

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
    QueueManagerInfo &qm = LABSTOR_CLIENT->server_config_.queue_manager_;
    std::vector<PriorityInfo> queue_info = {
        {1, 1, qm.queue_depth_, 0},
        {1, 1, qm.queue_depth_, QUEUE_LONG_RUNNING},
        {qm.max_lanes_, qm.max_lanes_, qm.queue_depth_, QUEUE_LOW_LATENCY}
    };
    return LABSTOR_ADMIN->AsyncCreateTaskState<ConstructTask>(
        task_node, domain_id, state_name, id_, queue_info, blob_mdm);
  }
  LABSTOR_TASK_NODE_ROOT(AsyncCreate)
  template<typename ...Args>
  HSHM_ALWAYS_INLINE
  void CreateRoot(Args&& ...args) {
    LPointer<ConstructTask> task =
        AsyncCreateRoot(std::forward<Args>(args)...);
    task->Wait();
    id_ = task->id_;
    queue_id_ = QueueId(id_);
    LABSTOR_CLIENT->DelTask(task);
  }

  /** Destroy task state + queue */
  HSHM_ALWAYS_INLINE
  void DestroyRoot(const DomainId &domain_id) {
    LABSTOR_ADMIN->DestroyTaskStateRoot(domain_id, id_);
  }

  /** Register task state */
  HSHM_ALWAYS_INLINE
  void AsyncRegisterStagerConstruct(RegisterStagerTask *task,
                                    const TaskNode &task_node,
                                    const BucketId &bkt_id,
                                    const hshm::charbuf &url) {
    LABSTOR_CLIENT->ConstructTask<RegisterStagerTask>(
        task, task_node, id_, bkt_id, url);
  }
  HSHM_ALWAYS_INLINE
  void RegisterStagerRoot(const BucketId &bkt_id,
                          const hshm::charbuf &url) {
    LPointer<labpq::TypedPushTask<RegisterStagerTask>> task =
        AsyncRegisterStagerRoot(bkt_id, url);
    task.ptr_->Wait();
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(RegisterStager);

  /** Unregister task state */
  HSHM_ALWAYS_INLINE
  void AsyncUnregisterStagerConstruct(UnregisterStagerTask *task,
                                      const TaskNode &task_node,
                                      const BucketId &bkt_id) {
    LABSTOR_CLIENT->ConstructTask<UnregisterStagerTask>(
        task, task_node, id_, bkt_id);
  }
  HSHM_ALWAYS_INLINE
  void UnregisterStagerRoot(const BucketId &bkt_id) {
    LPointer<labpq::TypedPushTask<UnregisterStagerTask>> task =
        AsyncUnregisterStagerRoot(bkt_id);
    task.ptr_->Wait();
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(UnregisterStager);

  /** Stage in data from a remote source */
  HSHM_ALWAYS_INLINE
  void AsyncStageInConstruct(StageInTask *task,
                            const TaskNode &task_node,
                            const BucketId &bkt_id,
                            const hshm::charbuf &blob_name,
                            float score,
                            u32 node_id) {
    LABSTOR_CLIENT->ConstructTask<StageInTask>(
        task, task_node, id_, bkt_id,
        blob_name, score, node_id);
  }
  HSHM_ALWAYS_INLINE
  void StageInRoot(const BucketId &bkt_id,
               const hshm::charbuf &blob_name,
               float score,
               u32 node_id) {
    LPointer<labpq::TypedPushTask<StageInTask>> task =
        AsyncStageInRoot(bkt_id, blob_name, score, node_id);
    task.ptr_->Wait();
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(StageIn);

  /** Stage out data to a remote source */
  HSHM_ALWAYS_INLINE
  void AsyncStageOutConstruct(StageOutTask *task,
                              const TaskNode &task_node,
                              const BucketId &bkt_id,
                              const hshm::charbuf &blob_name,
                              const hipc::Pointer &data,
                              size_t data_size,
                              u32 task_flags) {
    LABSTOR_CLIENT->ConstructTask<StageOutTask>(
        task, task_node, id_, bkt_id,
        blob_name, data, data_size, task_flags);
  }
  HSHM_ALWAYS_INLINE
  void StageOutRoot(const BucketId &bkt_id,
                    const hshm::charbuf &blob_name,
                    const hipc::Pointer &data,
                    size_t data_size,
                    u32 task_flags) {
    LPointer<labpq::TypedPushTask<StageOutTask>> task =
        AsyncStageOutRoot(bkt_id, blob_name, data, data_size, task_flags);
    task.ptr_->Wait();
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(StageOut);

  /** Parse url */
  static void GetUrlProtocolAndAction(const std::string &url,
                                      std::string &protocol,
                                      std::string &action,
                                      std::vector<std::string> &tokens) {
    // Determine the protocol + action
    std::string token;
    size_t found = url.find("://");
    if (found != std::string::npos) {
      protocol = url.substr(0, found);
      action = url.substr(found + 3);
    }

    // Divide action into tokens
    std::stringstream ss(action);
    while (std::getline(ss, token, ':')) {
      tokens.push_back(token);
    }
  }

  /** Bucket name from url */
  static hshm::charbuf GetTagNameFromUrl(const hshm::string &url) {
    // Parse url
    std::string protocol, action;
    std::vector<std::string> tokens;
    GetUrlProtocolAndAction(url.str(), protocol, action, tokens);
    if (protocol == "file") {
      // file://[path]:[page_size]
      std::string path = tokens[0];
      return hshm::charbuf(path);
    }
    return url;
  }
};

}  // namespace labstor

#endif  // LABSTOR_data_stager_H_
