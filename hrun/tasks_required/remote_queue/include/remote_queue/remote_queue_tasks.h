//
// Created by lukemartinlogan on 8/14/23.
//

#ifndef HRUN_TASKS_REMOTE_QUEUE_INCLUDE_REMOTE_QUEUE_REMOTE_QUEUE_TASKS_H_
#define HRUN_TASKS_REMOTE_QUEUE_INCLUDE_REMOTE_QUEUE_REMOTE_QUEUE_TASKS_H_

#include "hrun/api/hrun_client.h"
#include "hrun/task_registry/task_lib.h"
#include "hrun_admin/hrun_admin.h"
#include "hrun/queue_manager/queue_manager_client.h"
#include "proc_queue/proc_queue.h"

#include <thallium.hpp>
#include <thallium/serialization/stl/pair.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/serialization/stl/list.hpp>

namespace tl = thallium;

namespace hrun::remote_queue {

#include "hrun/hrun_namespace.h"
#include "remote_queue_methods.h"

/**
 * A task to create remote_queue
 * */
using hrun::Admin::CreateTaskStateTask;
struct ConstructTask : public CreateTaskStateTask {
  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  ConstructTask(hipc::Allocator *alloc) : CreateTaskStateTask(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  ConstructTask(hipc::Allocator *alloc,
                const TaskNode &task_node,
                const DomainId &domain_id,
                const std::string &state_name,
                const TaskStateId &id,
                const std::vector<PriorityInfo> &queue_info)
      : CreateTaskStateTask(alloc, task_node, domain_id, state_name,
                            "remote_queue", id, queue_info) {
    // Custom params
  }
};

/** A task to destroy remote_queue */
using hrun::Admin::DestroyTaskStateTask;
struct DestructTask : public DestroyTaskStateTask {
  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  DestructTask(hipc::Allocator *alloc) : DestroyTaskStateTask(alloc) {}

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

/**
 * Phases of the push task
 * */
class PushPhase {
 public:
  TASK_METHOD_T kStart = 0;
  TASK_METHOD_T kWait = 1;
};


/**
 * A task to push a serialized task onto the remote queue
 * */
struct PushTask : public Task, TaskFlags<TF_LOCAL> {
  IN std::vector<DomainId> domain_ids_;
  IN Task *orig_task_;
  IN TaskState *exec_;
  IN u32 exec_method_;
  IN std::vector<DataTransfer> xfer_;
  TEMP std::string params_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  PushTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  PushTask(hipc::Allocator *alloc,
           const TaskNode &task_node,
           const DomainId &domain_id,
           const TaskStateId &state_id,
           std::vector<DomainId> &domain_ids,
           Task *orig_task,
           TaskState *exec,
           u32 exec_method,
           std::vector<DataTransfer> &xfer) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kPush;
    // task_flags_.SetBits(TASK_LOW_LATENCY | TASK_PREEMPTIVE | TASK_REMOTE_DEBUG_MARK | TASK_FIRE_AND_FORGET);
    task_flags_.SetBits(TASK_LOW_LATENCY | TASK_COROUTINE | TASK_REMOTE_DEBUG_MARK | TASK_FIRE_AND_FORGET);
    domain_id_ = domain_id;

    // Custom params
    domain_ids_ = std::move(domain_ids);
    orig_task_ = orig_task;
    exec_ = exec;
    exec_method_ = exec_method;
    xfer_ = std::move(xfer);
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

/**
 * A task to push a serialized task onto the remote queue
 * */
struct DupTask : public Task, TaskFlags<TF_LOCAL> {
  IN Task *orig_task_;
  IN TaskState *exec_;
  IN u32 exec_method_;
  IN std::vector<LPointer<Task>> dups_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  DupTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  DupTask(hipc::Allocator *alloc,
           const TaskNode &task_node,
           const TaskStateId &state_id,
           Task *orig_task,
           TaskState *exec,
           u32 exec_method,
           std::vector<LPointer<Task>> &dups) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kDup;
    task_flags_.SetBits(TASK_LOW_LATENCY | TASK_REMOTE_DEBUG_MARK | TASK_FIRE_AND_FORGET | TASK_COROUTINE);
    domain_id_ = DomainId::GetLocal();

    // Custom params
    orig_task_ = orig_task;
    exec_ = exec;
    exec_method_ = exec_method;
    dups_ = std::move(dups);
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

/**
 * A task to push a serialized task onto the remote queue
 * */
//struct AcceptTask : public Task {
//  /** Emplace constructor */
//  HSHM_ALWAYS_INLINE explicit
//  AcceptTask(hipc::Allocator *alloc,
//             const TaskNode &task_node,
//             const DomainId &domain_id,
//             const TaskStateId &state_id) : Task(alloc) {
//    // Initialize task
//    task_node_ = task_node;
//    lane_hash_ = 0;
//    task_state_ = state_id;
//    method_ = Method::kAccept;
//    task_flags_.SetBits(0);
//    domain_id_ = domain_id;
//  }
//};

} // namespace hrun::remote_queue

#endif //HRUN_TASKS_REMOTE_QUEUE_INCLUDE_REMOTE_QUEUE_REMOTE_QUEUE_TASKS_H_
