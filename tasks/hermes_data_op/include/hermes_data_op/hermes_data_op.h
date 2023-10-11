//
// Created by lukemartinlogan on 6/29/23.
//

#ifndef LABSTOR_hermes_data_op_H_
#define LABSTOR_hermes_data_op_H_

#include "hermes_data_op_tasks.h"

namespace hermes::data_op {

/** Create hermes_data_op requests */
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
                                      TaskStateId &bkt_mdm_id,
                                      TaskStateId &blob_mdm_id) {
    id_ = TaskStateId::GetNull();
    QueueManagerInfo &qm = LABSTOR_CLIENT->server_config_.queue_manager_;
    std::vector<PriorityInfo> queue_info = {
        {1, 1, qm.queue_depth_, 0},
        {1, 1, qm.queue_depth_, QUEUE_LONG_RUNNING},
        {qm.max_lanes_, qm.max_lanes_, qm.queue_depth_, QUEUE_LOW_LATENCY}
    };
    return LABSTOR_ADMIN->AsyncCreateTaskState<ConstructTask>(
        task_node, domain_id, state_name, id_, queue_info,
        bkt_mdm_id, blob_mdm_id);
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

  /** Register the OpGraph to perform on data */
  HSHM_ALWAYS_INLINE
  void AsyncRegisterOpConstruct(RegisterOpTask *task,
                                const TaskNode &task_node,
                                const OpGraph &op_graph) {
    LABSTOR_CLIENT->ConstructTask<RegisterOpTask>(
        task, task_node, DomainId::GetGlobal(), id_, op_graph);
  }
  HSHM_ALWAYS_INLINE
  void RegisterOpRoot(const OpGraph &op_graph) {
    LPointer<labpq::TypedPushTask<RegisterOpTask>> task =
        AsyncRegisterOpRoot(op_graph);
    task.ptr_->Wait();
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(RegisterOp);

  /** Register data as ready for operations to be performed */
  HSHM_ALWAYS_INLINE
  void AsyncRegisterDataConstruct(RegisterDataTask *task,
                                  const TaskNode &task_node,
                                  const BucketId &bkt_id,
                                  const std::string &blob_name,
                                  const BlobId &blob_id,
                                  size_t off,
                                  size_t size) {
    LABSTOR_CLIENT->ConstructTask<RegisterDataTask>(
        task, task_node, id_, bkt_id,
        blob_name, blob_id, off, size);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(RegisterData);

  /** Async task to run operators */
  HSHM_ALWAYS_INLINE
  void AsyncRunOpConstruct(RunOpTask *task,
                           const TaskNode &task_node) {
    LABSTOR_CLIENT->ConstructTask<RunOpTask>(
        task, task_node, id_);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(RunOp);
};

}  // namespace labstor

#endif  // LABSTOR_hermes_data_op_H_
