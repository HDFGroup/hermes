//
// Created by lukemartinlogan on 6/29/23.
//

#ifndef HRUN_hermes_adapters_H_
#define HRUN_hermes_adapters_H_

#include "hermes_adapters_tasks.h"

namespace hrun::hermes_adapters {

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
    QueueManagerInfo &qm = HRUN_CLIENT->server_config_.queue_manager_;
    std::vector<PriorityInfo> queue_info = {
        {1, 1, qm.queue_depth_, 0},
        {1, 1, qm.queue_depth_, QUEUE_LONG_RUNNING},
        {qm.max_lanes_, qm.max_lanes_, qm.queue_depth_, QUEUE_LOW_LATENCY}
    };
    return HRUN_ADMIN->AsyncCreateTaskState<ConstructTask>(
        task_node, domain_id, state_name, id_, queue_info);
  }
  HRUN_TASK_NODE_ROOT(AsyncCreate)
  template<typename ...Args>
  HSHM_ALWAYS_INLINE
  void CreateRoot(Args&& ...args) {
    LPointer<ConstructTask> task =
        AsyncCreateRoot(std::forward<Args>(args)...);
    task->Wait();
    id_ = task->id_;
    queue_id_ = QueueId(id_);
    HRUN_CLIENT->DelTask(task);
  }

  /** Destroy task state + queue */
  HSHM_ALWAYS_INLINE
  void DestroyRoot(const DomainId &domain_id) {
    HRUN_ADMIN->DestroyTaskStateRoot(domain_id, id_);
  }

  /** Call a custom method */
  HSHM_ALWAYS_INLINE
  void AsyncCustomConstruct(CustomTask *task,
                            const TaskNode &task_node,
                            const DomainId &domain_id) {
    HRUN_CLIENT->ConstructTask<CustomTask>(
        task, task_node, domain_id, id_);
  }
  HSHM_ALWAYS_INLINE
  void CustomRoot(const DomainId &domain_id) {
    LPointer<hrunpq::TypedPushTask<CustomTask>> task = AsyncCustomRoot(domain_id);
    task.ptr_->Wait();
  }
  HRUN_TASK_NODE_PUSH_ROOT(Custom);
};

}  // namespace hrun

#endif  // HRUN_hermes_adapters_H_
