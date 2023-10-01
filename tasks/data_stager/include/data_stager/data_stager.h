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
    auto *task = AsyncCreateRoot(std::forward<Args>(args)...);
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
                              size_t data_size) {
    LABSTOR_CLIENT->ConstructTask<StageOutTask>(
        task, task_node, id_, bkt_id,
        blob_name, data, data_size);
  }
  HSHM_ALWAYS_INLINE
  void StageOutRoot(const BucketId &bkt_id,
                    const hshm::charbuf &blob_name,
                    const hipc::Pointer &data,
                    size_t data_size) {
    LPointer<labpq::TypedPushTask<StageOutTask>> task =
        AsyncStageOutRoot(bkt_id, blob_name, data, data_size);
    task.ptr_->Wait();
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(StageOut);
};

}  // namespace labstor

#endif  // LABSTOR_data_stager_H_
