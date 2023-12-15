//
// Created by lukemartinlogan on 6/29/23.
//

#ifndef HRUN_small_message_H_
#define HRUN_small_message_H_

#include "small_message_tasks.h"

namespace hrun::small_message {

/** Create admin requests */
class Client : public TaskLibClient {

 public:
  /** Default constructor */
  Client() = default;

  /** Destructor */
  ~Client() = default;

  /** Create a small_message */
  HSHM_ALWAYS_INLINE
  void CreateRoot(const DomainId &domain_id,
                  const std::string &state_name) {
    id_ = TaskStateId::GetNull();
    QueueManagerInfo &qm = HRUN_CLIENT->server_config_.queue_manager_;
    std::vector<PriorityInfo> queue_info = {
        {TaskPrio::kAdmin, 1, 1, qm.queue_depth_, 0},
        {TaskPrio::kLongRunning, 1, 1, qm.queue_depth_, QUEUE_LONG_RUNNING},
        {TaskPrio::kLowLatency, qm.max_lanes_, qm.max_lanes_, qm.queue_depth_, QUEUE_LOW_LATENCY}
    };
    id_ = HRUN_ADMIN->CreateTaskStateRoot<ConstructTask>(
        domain_id, state_name, id_, queue_info);
    queue_id_ = QueueId(id_);
  }

  /** Destroy state + queue */
  HSHM_ALWAYS_INLINE
  void DestroyRoot(const DomainId &domain_id) {
    HRUN_ADMIN->DestroyTaskStateRoot(domain_id, id_);
  }

  /** Metadata task */
  LPointer<MdTask> AsyncMd(const TaskNode &task_node,
                           const DomainId &domain_id) {
    MultiQueue *queue = HRUN_CLIENT->GetQueue(queue_id_);
    auto task = HRUN_CLIENT->NewTask<MdTask>(
        task_node, domain_id, id_);
    queue->Emplace(TaskPrio::kLowLatency, 3, task.shm_);
    return task;
  }
  HRUN_TASK_NODE_ROOT(AsyncMd);
  int MdRoot(const DomainId &domain_id) {
    LPointer<MdTask> task = AsyncMdRoot(domain_id);
    task->Wait();
    int ret = task->ret_[0];
    HRUN_CLIENT->DelTask(task);
    return ret;
  }

  /** Metadata PUSH task */
  void AsyncMdPushConstruct(MdPushTask *task,
                            const TaskNode &task_node,
                            const DomainId &domain_id) {
    HRUN_CLIENT->ConstructTask<MdPushTask>(
        task, task_node, domain_id, id_);
  }
  int MdPushRoot(const DomainId &domain_id) {
    LPointer<hrunpq::TypedPushTask<MdPushTask>> push_task =
        AsyncMdPushRoot(domain_id);
    push_task->Wait();
    MdPushTask *task = push_task->get();
    int ret = task->ret_[0];
    HRUN_CLIENT->DelTask(push_task);
    return ret;
  }
  HRUN_TASK_NODE_PUSH_ROOT(MdPush);

  /** Io task */
  void AsyncIoConstruct(IoTask *task, const TaskNode &task_node,
                        const DomainId &domain_id) {
    HRUN_CLIENT->ConstructTask<IoTask>(
        task, task_node, domain_id, id_);
  }
  int IoRoot(const DomainId &domain_id) {
    LPointer<hrunpq::TypedPushTask<IoTask>> push_task = AsyncIoRoot(domain_id);
    push_task->Wait();
    IoTask *task = push_task->get();
    int ret = task->ret_;
    HRUN_CLIENT->DelTask(push_task);
    return ret;
  }
  HRUN_TASK_NODE_PUSH_ROOT(Io)
};

}  // namespace hrun

#endif  // HRUN_small_message_H_
