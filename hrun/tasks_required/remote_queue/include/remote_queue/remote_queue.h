//
// Created by lukemartinlogan on 6/29/23.
//

#ifndef HRUN_remote_queue_H_
#define HRUN_remote_queue_H_

#include "remote_queue_tasks.h"

namespace hrun::remote_queue {

/**
 * Create remote_queue requests
 *
 * This is ONLY used in the Hermes runtime, and
 * should never be called in client programs!!!
 * */
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
                                      const TaskStateId &state_id) {
    id_ = state_id;
    QueueManagerInfo &qm = HRUN_CLIENT->server_config_.queue_manager_;
    std::vector<PriorityInfo> queue_info;
    return HRUN_ADMIN->AsyncCreateTaskState<ConstructTask>(
        task_node, domain_id, state_name, id_, queue_info);
  }
  HRUN_TASK_NODE_ROOT(AsyncCreate);
  template<typename ...Args>
  HSHM_ALWAYS_INLINE
  void CreateRoot(Args&& ...args) {
    LPointer<ConstructTask> task =
        AsyncCreateRoot(std::forward<Args>(args)...);
    task->Wait();
    Init(id_, HRUN_ADMIN->queue_id_);
    HRUN_CLIENT->DelTask(task);
  }

  /** Destroy task state + queue */
  HSHM_ALWAYS_INLINE
  void DestroyRoot(const DomainId &domain_id) {
    HRUN_ADMIN->DestroyTaskStateRoot(domain_id, id_);
  }

  /** Disperse a task among a domain of nodes */
  HSHM_ALWAYS_INLINE
  void Disperse(Task *orig_task,
                TaskState *exec,
                std::vector<DomainId> &domain_ids) {
    // Serialize task + create the wait task
    orig_task->UnsetStarted();
    BinaryOutputArchive<true> ar(DomainId::GetNode(HRUN_CLIENT->node_id_));
    std::vector<DataTransfer> xfer =
        exec->SaveStart(orig_task->method_, ar, orig_task);

    // Create subtasks
    exec->ReplicateStart(orig_task->method_, domain_ids.size(), orig_task);
    LPointer<PushTask> push_task = HRUN_CLIENT->NewTask<PushTask>(
        orig_task->task_node_ + 1, DomainId::GetLocal(), id_,
        domain_ids, orig_task, exec, orig_task->method_, xfer);
    if (orig_task->IsRoot()) {
      push_task->SetRoot();
    }
    MultiQueue *queue = HRUN_CLIENT->GetQueue(queue_id_);
    queue->Emplace(push_task->prio_, orig_task->lane_hash_, push_task.shm_);
  }

  /** Disperse a task among each lane of this node */
  HSHM_ALWAYS_INLINE
  void DisperseLocal(Task *orig_task, TaskState *exec,
                     MultiQueue *orig_queue, LaneGroup *lane_group) {
    // Duplicate task
    std::vector<LPointer<Task>> dups(lane_group->num_lanes_);
    exec->Dup(orig_task->method_, orig_task, dups);
    for (size_t i = 0; i < dups.size(); ++i) {
      LPointer<Task> &task = dups[i];
      task->UnsetFireAndForget();
      task->lane_hash_ = i;
      task->UnsetLaneAll();
      orig_queue->Emplace(task->prio_, task->lane_hash_, task.shm_);
    }

    // Create duplicate task
    exec->ReplicateStart(orig_task->method_, lane_group->num_lanes_, orig_task);
    LPointer<DupTask> dup_task = HRUN_CLIENT->NewTask<DupTask>(
        orig_task->task_node_ + 1, id_,
        orig_task, exec, orig_task->method_, dups);
    MultiQueue *queue = HRUN_CLIENT->GetQueue(queue_id_);
    queue->Emplace(dup_task->prio_, orig_task->lane_hash_, dup_task.shm_);
  }

  /** Spawn task to accept new connections */
//  HSHM_ALWAYS_INLINE
//  AcceptTask* AsyncAcceptThread() {
//    hipc::Pointer p;
//    MultiQueue *queue = HRUN_CLIENT->GetQueue(queue_id_);
//    auto *task = HRUN_CLIENT->NewTask<AcceptTask>(
//        p, TaskNode::GetNull(), DomainId::GetLocal(), id_);
//    queue->Emplace(0, 0, p);
//    return task;
//  }
};

}  // namespace hrun

#endif  // HRUN_remote_queue_H_
