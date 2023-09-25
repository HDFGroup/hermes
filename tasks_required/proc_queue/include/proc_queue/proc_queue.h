//
// Created by lukemartinlogan on 6/29/23.
//

#ifndef LABSTOR_proc_queue_H_
#define LABSTOR_proc_queue_H_

#include "proc_queue_tasks.h"

namespace labstor::proc_queue {

/** Create proc_queue requests */
class Client : public TaskLibClient {
 public:
  /** Default constructor */
  Client() {
    id_ = TaskStateId(LABSTOR_QM_CLIENT->process_queue_);
    queue_id_ = LABSTOR_QM_CLIENT->process_queue_;
  }

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
  LABSTOR_TASK_NODE_ROOT(AsyncCreate);
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

  /** Call a custom method */
  template<typename TaskT>
  HSHM_ALWAYS_INLINE
  void AsyncPushConstruct(labpq::TypedPushTask<TaskT> *task,
                          const TaskNode &task_node,
                          const DomainId &domain_id,
                          const hipc::LPointer<TaskT> &subtask) {
    LABSTOR_CLIENT->ConstructTask(
        task, task_node, domain_id, id_, subtask);
  }
  template<typename TaskT>
  HSHM_ALWAYS_INLINE
  LPointer<labpq::TypedPushTask<TaskT>>
  AsyncPush(const TaskNode &task_node,
            const DomainId &domain_id,
            const hipc::LPointer<TaskT> &subtask) {
    LPointer<labpq::TypedPushTask<TaskT>> push_task =
        LABSTOR_CLIENT->AllocateTask<labpq::TypedPushTask<TaskT>>();
    AsyncPushConstruct(push_task.ptr_, task_node, domain_id, subtask);
    MultiQueue *queue = LABSTOR_CLIENT->GetQueue(queue_id_);
    queue->Emplace(push_task->prio_, push_task->lane_hash_, push_task.shm_);
    return push_task;
  }
  LABSTOR_TASK_NODE_ROOT(AsyncPush);
};

}  // namespace labstor

#define LABSTOR_PROCESS_QUEUE \
  hshm::EasySingleton<labstor::proc_queue::Client>::GetInstance()

#endif  // LABSTOR_proc_queue_H_
