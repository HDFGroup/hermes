/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Distributed under BSD 3-Clause license.                                   *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Illinois Institute of Technology.                        *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Hermes. The full Hermes copyright notice, including  *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the top directory. If you do not  *
 * have access to the file, you may request a copy from help@hdfgroup.org.   *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#ifndef HRUN_proc_queue_H_
#define HRUN_proc_queue_H_

#include "proc_queue_tasks.h"

namespace hrun::proc_queue {

/** Create proc_queue requests */
class Client : public TaskLibClient {
 public:
  /** Default constructor */
  Client() {
    id_ = TaskStateId(HRUN_QM_CLIENT->process_queue_);
    queue_id_ = HRUN_QM_CLIENT->process_queue_;
  }

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
        // TODO(llogan): Specify different depth for proc queue
        {qm.max_lanes_, qm.max_lanes_, 16, QUEUE_LOW_LATENCY}
    };
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
  template<typename TaskT>
  HSHM_ALWAYS_INLINE
  void AsyncPushConstruct(hrunpq::TypedPushTask<TaskT> *task,
                          const TaskNode &task_node,
                          const DomainId &domain_id,
                          const hipc::LPointer<TaskT> &subtask) {
    HRUN_CLIENT->ConstructTask(
        task, task_node, domain_id, id_, subtask);
  }
  template<typename TaskT>
  HSHM_ALWAYS_INLINE
  LPointer<hrunpq::TypedPushTask<TaskT>>
  AsyncPush(const TaskNode &task_node,
            const DomainId &domain_id,
            const hipc::LPointer<TaskT> &subtask) {
    LPointer<hrunpq::TypedPushTask<TaskT>> push_task =
        HRUN_CLIENT->AllocateTask<hrunpq::TypedPushTask<TaskT>>();
    AsyncPushConstruct(push_task.ptr_, task_node, domain_id, subtask);
    MultiQueue *queue = HRUN_CLIENT->GetQueue(queue_id_);
    queue->Emplace(push_task->prio_, push_task->lane_hash_, push_task.shm_);
    return push_task;
  }
  HRUN_TASK_NODE_ROOT(AsyncPush);
};

}  // namespace hrun

#define HRUN_PROCESS_QUEUE \
  hshm::EasySingleton<hrun::proc_queue::Client>::GetInstance()

#endif  // HRUN_proc_queue_H_
