//
// Created by lukemartinlogan on 8/11/23.
//

#ifndef LABSTOR_TASKS_TASK_TEMPL_INCLUDE_proc_queue_proc_queue_TASKS_H_
#define LABSTOR_TASKS_TASK_TEMPL_INCLUDE_proc_queue_proc_queue_TASKS_H_

#include "labstor/api/labstor_client.h"
#include "labstor/task_registry/task_lib.h"
#include "labstor_admin/labstor_admin.h"
#include "labstor/queue_manager/queue_manager_client.h"

namespace labstor::proc_queue {

#include "labstor/labstor_namespace.h"
#include "proc_queue_methods.h"

/**
 * A task to create proc_queue
 * */
using labstor::Admin::CreateTaskStateTask;
struct ConstructTask : public CreateTaskStateTask {
  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  ConstructTask(hipc::Allocator *alloc)
  : CreateTaskStateTask(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  ConstructTask(hipc::Allocator *alloc,
                const TaskNode &task_node,
                const DomainId &domain_id,
                const std::string &state_name,
                const TaskStateId &id,
                const std::vector<PriorityInfo> &queue_info)
      : CreateTaskStateTask(alloc, task_node, domain_id, state_name,
                            "proc_queue", id, queue_info) {
    // Custom params
  }

  HSHM_ALWAYS_INLINE
  ~ConstructTask() {
    // Custom params
  }
};

/** A task to destroy proc_queue */
using labstor::Admin::DestroyTaskStateTask;
struct DestructTask : public DestroyTaskStateTask {
  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  DestructTask(hipc::Allocator *alloc)
  : DestroyTaskStateTask(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  DestructTask(hipc::Allocator *alloc,
               const TaskNode &task_node,
               const DomainId &domain_id,
               TaskStateId &state_id)
  : DestroyTaskStateTask(alloc, task_node, domain_id, state_id) {}

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

class PushTaskPhase {
 public:
  TASK_METHOD_T kSchedule = 0;
  TASK_METHOD_T kWaitSchedule = 1;
};

/**
 * Push a task into the per-process queue
 * */
template<typename TaskT>
struct TypedPushTask : public Task, TaskFlags<TF_LOCAL> {
  IN LPointer<TaskT> sub_cli_;  /**< Pointer to the subtask (client + SHM) */
  TEMP LPointer<TaskT> sub_run_;  /**< Pointer to the subtask (runtime) */
  TEMP int phase_ = PushTaskPhase::kSchedule;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  TypedPushTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  TypedPushTask(hipc::Allocator *alloc,
                const TaskNode &task_node,
                const DomainId &domain_id,
                const TaskStateId &state_id,
                const hipc::LPointer<TaskT> &subtask) : Task(alloc) {
    // Initialize task
    hshm::NodeThreadId tid;
    task_node_ = task_node;
    lane_hash_ = tid.bits_.tid_ + tid.bits_.pid_;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kPush;
    task_flags_.SetBits(TASK_DATA_OWNER | TASK_LOW_LATENCY);
    domain_id_ = domain_id;

    // Custom params
    sub_cli_ = subtask;
  }

  /** Destructor */
  ~TypedPushTask() {
    if (!IsFireAndForget()) {
      LABSTOR_CLIENT->DelTask(sub_cli_);
    } else {
      LABSTOR_CLIENT->DelTask(sub_run_);
    }
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    group.resize(sizeof(u32));
    memcpy(group.data(), &lane_hash_, sizeof(u32));
    return 0;
  }

  /** Get the task address */
  HSHM_ALWAYS_INLINE
  TaskT* get() {
    return sub_cli_.ptr_;
  }
};

using PushTask = labstor::proc_queue::TypedPushTask<Task>;

}  // namespace labstor::proc_queue

namespace labpq = labstor::proc_queue;

#endif  // LABSTOR_TASKS_TASK_TEMPL_INCLUDE_proc_queue_proc_queue_TASKS_H_
