#ifndef HRUN_TASKS_HRUN_ADMIN_INCLUDE_HRUN_ADMIN_HRUN_ADMIN_TASKS_H_
#define HRUN_TASKS_HRUN_ADMIN_INCLUDE_HRUN_ADMIN_HRUN_ADMIN_TASKS_H_

#include "hrun/work_orchestrator/scheduler.h"
#include "hrun/api/hrun_client.h"
#include "hrun/queue_manager/queue_manager_client.h"
#include "hrun/hrun_namespace.h"

namespace hrun::Admin {

#include "hrun_admin_methods.h"

/** A template to register or destroy a task library */
template<int method>
struct RegisterTaskLibTaskTempl : public Task, TaskFlags<TF_SRL_SYM> {
  IN hipc::ShmArchive<hipc::string> lib_name_;
  OUT TaskStateId id_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  RegisterTaskLibTaskTempl(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  RegisterTaskLibTaskTempl(hipc::Allocator *alloc,
                           const TaskNode &task_node,
                           const DomainId &domain_id,
                           const std::string &lib_name) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kAdmin;
    task_state_ = HRUN_QM_CLIENT->admin_task_state_;
    if constexpr(method == 0)
    {
      method_ = Method::kRegisterTaskLib;
    } else {
      method_ = Method::kDestroyTaskLib;
    }
    task_flags_.SetBits(0);
    domain_id_ = domain_id;

    // Initialize QueueManager task
    HSHM_MAKE_AR(lib_name_, alloc, lib_name);
  }

  /** Destructor */
  ~RegisterTaskLibTaskTempl() {
    HSHM_DESTROY_AR(lib_name_);
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(lib_name_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
    ar(id_);
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

/** A task to register a Task Library */
using RegisterTaskLibTask = RegisterTaskLibTaskTempl<0>;

/** A task to destroy a Task Library */
using DestroyTaskLibTask = RegisterTaskLibTaskTempl<1>;

class CreateTaskStatePhase {
 public:
  // NOTE(llogan): kLast is intentially 0 so that the constructor
  // can seamlessly pass data to submethods
  TASK_METHOD_T kIdAllocStart = 0;
  TASK_METHOD_T kIdAllocWait = 1;
  TASK_METHOD_T kStateCreate = 2;
  TASK_METHOD_T kLast = 0;
};

/** A task to get or retrieve the ID of a task */
struct GetOrCreateTaskStateIdTask : public Task, TaskFlags<TF_SRL_SYM> {
  IN hipc::ShmArchive<hipc::string> state_name_;
  OUT TaskStateId id_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  GetOrCreateTaskStateIdTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  GetOrCreateTaskStateIdTask(hipc::Allocator *alloc,
                             const TaskNode &task_node,
                             const DomainId &domain_id,
                             const std::string &state_name) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kAdmin;
    task_state_ = HRUN_QM_CLIENT->admin_task_state_;
    method_ = Method::kGetOrCreateTaskStateId;
    task_flags_.SetBits(0);
    domain_id_ = domain_id;

    // Initialize
    HSHM_MAKE_AR(state_name_, alloc, state_name);
  }

  ~GetOrCreateTaskStateIdTask() {
    HSHM_DESTROY_AR(state_name_);
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(state_name_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
    ar(id_);
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

/** A task to register a Task state + Create a queue */
struct CreateTaskStateTask : public Task, TaskFlags<TF_SRL_SYM | TF_REPLICA> {
  IN hipc::ShmArchive<hipc::string> lib_name_;
  IN hipc::ShmArchive<hipc::string> state_name_;
  IN hipc::ShmArchive<hipc::vector<PriorityInfo>> queue_info_;
  INOUT TaskStateId id_;
  IN hipc::ShmArchive<hipc::string> custom_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  CreateTaskStateTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  CreateTaskStateTask(hipc::Allocator *alloc,
                      const TaskNode &task_node,
                      const DomainId &domain_id,
                      const std::string &state_name,
                      const std::string &lib_name,
                      const TaskStateId &id,
                      const std::vector<PriorityInfo> &queue_info) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kAdmin;
    task_state_ = HRUN_QM_CLIENT->admin_task_state_;
    method_ = Method::kCreateTaskState;
    task_flags_.SetBits(TASK_COROUTINE);
    domain_id_ = domain_id;

    // Initialize
    HSHM_MAKE_AR(state_name_, alloc, state_name);
    HSHM_MAKE_AR(lib_name_, alloc, lib_name);
    HSHM_MAKE_AR(queue_info_, alloc, queue_info);
    HSHM_MAKE_AR(custom_, alloc, "");
    id_ = id;
  }

  /** Destructor */
  ~CreateTaskStateTask() {
    HSHM_DESTROY_AR(state_name_);
    HSHM_DESTROY_AR(lib_name_);
    HSHM_DESTROY_AR(queue_info_);
    HSHM_DESTROY_AR(custom_);
  }

  /** Duplicate message */
  template<typename TaskT>
  void Dup(hipc::Allocator *alloc, TaskT &other) {
    task_dup(other);
  }

  /** Process duplicate message output */
  template<typename TaskT>
  void DupEnd(u32 replica, TaskT &dup_task) {
  }

  /** Replication (does nothing) */
  void ReplicateStart(u32 count) {
  }

  /** Replicate end (does nothing) */
  void ReplicateEnd() {
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(lib_name_, state_name_, id_, queue_info_, custom_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
    ar(id_);
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    LocalSerialize srl(group);
    srl << 16;
    return 0;
  }
};

/** A task to retrieve the ID of a task */
struct GetTaskStateIdTask : public Task, TaskFlags<TF_SRL_SYM> {
  IN hipc::ShmArchive<hipc::string>
      state_name_;
  OUT TaskStateId
      id_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  GetTaskStateIdTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  GetTaskStateIdTask(hipc::Allocator *alloc,
                     const TaskNode &task_node,
                     const DomainId &domain_id,
                     const std::string &state_name) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kAdmin;
    task_state_ = HRUN_QM_CLIENT->admin_task_state_;
    method_ = Method::kGetTaskStateId;
    task_flags_.SetBits(0);
    domain_id_ = domain_id;

    // Initialize
    HSHM_MAKE_AR(state_name_, alloc, state_name);
  }

  ~GetTaskStateIdTask() {
    HSHM_DESTROY_AR(state_name_);
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(state_name_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
    ar(id_);
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

/** A task to destroy a Task state */
struct DestroyTaskStateTask : public Task, TaskFlags<TF_SRL_SYM> {
  IN TaskStateId id_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  DestroyTaskStateTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  DestroyTaskStateTask(hipc::Allocator *alloc,
                       const TaskNode &task_node,
                       const DomainId &domain_id,
                       const TaskStateId &id) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kAdmin;
    task_state_ = HRUN_QM_CLIENT->admin_task_state_;
    method_ = Method::kDestroyTaskState;
    task_flags_.SetBits(0);
    domain_id_ = domain_id;

    // Initialize
    id_ = id;
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar & id_;
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

/** A task to destroy a Task state */
struct StopRuntimeTask : public Task, TaskFlags<TF_SRL_SYM> {
  /** SHM default constructor */
  StopRuntimeTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  StopRuntimeTask(hipc::Allocator *alloc,
                  const TaskNode &task_node,
                  const DomainId &domain_id) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kAdmin;
    task_state_ = HRUN_QM_CLIENT->admin_task_state_;
    method_ = Method::kStopRuntime;
    task_flags_.SetBits(TASK_FIRE_AND_FORGET | TASK_FLUSH);
    domain_id_ = domain_id;
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

/** A task to destroy a Task state */
template<int method>
struct SetWorkOrchestratorPolicyTask : public Task, TaskFlags<TF_SRL_SYM> {
  IN TaskStateId policy_id_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  SetWorkOrchestratorPolicyTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  SetWorkOrchestratorPolicyTask(hipc::Allocator *alloc,
                                const TaskNode &task_node,
                                const DomainId &domain_id,
                                const TaskStateId &policy_id) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kAdmin;
    task_state_ = HRUN_QM_CLIENT->admin_task_state_;
    if constexpr(method == 0) {
      method_ = Method::kSetWorkOrchQueuePolicy;
    } else {
      method_ = Method::kSetWorkOrchProcPolicy;
    }
    task_flags_.SetBits(0);
    domain_id_ = domain_id;

    // Initialize
    policy_id_ = policy_id;
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(policy_id_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};
using SetWorkOrchQueuePolicyTask = SetWorkOrchestratorPolicyTask<0>;
using SetWorkOrchProcPolicyTask = SetWorkOrchestratorPolicyTask<1>;

/** A task to destroy a Task state */
struct FlushTask : public Task, TaskFlags<TF_SRL_SYM | TF_REPLICA> {
  /** SHM default constructor */
  FlushTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  FlushTask(hipc::Allocator *alloc,
            const TaskNode &task_node,
            const DomainId &domain_id) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kAdmin;
    task_state_ = HRUN_QM_CLIENT->admin_task_state_;
    method_ = Method::kFlush;
    task_flags_.SetBits(TASK_FLUSH | TASK_COROUTINE);
    domain_id_ = domain_id;
  }

  /** Duplicate message */
  template<typename TaskT>
  void Dup(hipc::Allocator *alloc, TaskT &other) {
    task_dup(other);
  }

  /** Process duplicate message output */
  template<typename TaskT>
  void DupEnd(u32 replica, TaskT &dup_task) {}

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {}

  /** Begin replication */
  void ReplicateStart(u32 count) {}

  /** Finalize replication */
  void ReplicateEnd() {}

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    hrun::LocalSerialize srl(group);
    srl << task_state_;
    return 0;
  }
};


}  // namespace hrun::Admin

#endif  // HRUN_TASKS_HRUN_ADMIN_INCLUDE_HRUN_ADMIN_HRUN_ADMIN_TASKS_H_
