//
// Created by llogan on 7/2/23.
//

#ifndef LABSTOR_INCLUDE_LABSTOR_WORK_ORCHESTRATOR_SCHEDULER_H_
#define LABSTOR_INCLUDE_LABSTOR_WORK_ORCHESTRATOR_SCHEDULER_H_

#include "labstor/task_registry/task.h"

namespace labstor {

/** The set of methods in the admin task */
struct SchedulerMethod : public TaskMethod {
  TASK_METHOD_T kSchedule = TaskMethod::kLast;
};

/** The task type used for scheduling */
struct ScheduleTask : public Task, TaskFlags<TF_LOCAL> {
  OUT hipc::pod_array<int, 1> ret_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  ScheduleTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  ScheduleTask(hipc::Allocator *alloc,
               const TaskNode &task_node,
               const DomainId &domain_id,
               TaskStateId &state_id) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kLongRunning;
    task_state_ = state_id;
    method_ = SchedulerMethod::kSchedule;
    task_flags_.SetBits(TASK_LONG_RUNNING | TASK_REMOTE_DEBUG_MARK);
    SetPeriodSec(1);
    domain_id_ = domain_id;

    // Custom params
    ret_.construct(alloc, 1);
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

}  // namespace labstor

#endif  // LABSTOR_INCLUDE_LABSTOR_WORK_ORCHESTRATOR_SCHEDULER_H_
