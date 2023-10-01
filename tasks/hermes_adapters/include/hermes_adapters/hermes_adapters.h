//
// Created by lukemartinlogan on 6/29/23.
//

#ifndef LABSTOR_hermes_adapters_H_
#define LABSTOR_hermes_adapters_H_

#include "hermes_adapters_tasks.h"

namespace labstor::hermes_adapters {

/** Create hermes_adapters requests */
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

  /** Call a custom method */
  HSHM_ALWAYS_INLINE
  void AsyncCustomConstruct(CustomTask *task,
                            const TaskNode &task_node,
                            const DomainId &domain_id) {
    LABSTOR_CLIENT->ConstructTask<CustomTask>(
        task, task_node, domain_id, id_);
  }
  HSHM_ALWAYS_INLINE
  void CustomRoot(const DomainId &domain_id) {
    LPointer<labpq::TypedPushTask<CustomTask>> task = AsyncCustomRoot(domain_id);
    task.ptr_->Wait();
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(Custom);
};

}  // namespace labstor

#endif  // LABSTOR_hermes_adapters_H_
