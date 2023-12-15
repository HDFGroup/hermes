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

#ifndef HRUN_TASK_NAME_H_
#define HRUN_TASK_NAME_H_

#include "TASK_NAME_tasks.h"

namespace hrun::TASK_NAME {

/** Create TASK_NAME requests */
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
        {TaskPrio::kAdmin, 1, 1, qm.queue_depth_, 0},
        {TaskPrio::kLongRunning, 1, 1, qm.queue_depth_, QUEUE_LONG_RUNNING},
        {TaskPrio::kLowLatency, qm.max_lanes_, qm.max_lanes_, qm.queue_depth_, QUEUE_LOW_LATENCY}
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

#endif  // HRUN_TASK_NAME_H_
