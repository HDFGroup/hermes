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

#ifndef HRUN_INCLUDE_HRUN_WORK_ORCHESTRATOR_SCHEDULER_H_
#define HRUN_INCLUDE_HRUN_WORK_ORCHESTRATOR_SCHEDULER_H_

#include "hrun/task_registry/task.h"

namespace hrun {

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
    SetPeriodMs(250);
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

}  // namespace hrun

#endif  // HRUN_INCLUDE_HRUN_WORK_ORCHESTRATOR_SCHEDULER_H_
