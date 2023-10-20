//
// Created by lukemartinlogan on 8/11/23.
//

#ifndef HRUN_TASKS_TASK_TEMPL_INCLUDE_data_stager_data_stager_TASKS_H_
#define HRUN_TASKS_TASK_TEMPL_INCLUDE_data_stager_data_stager_TASKS_H_

#include "hrun/api/hrun_client.h"
#include "hrun/task_registry/task_lib.h"
#include "hrun_admin/hrun_admin.h"
#include "hrun/queue_manager/queue_manager_client.h"
#include "proc_queue/proc_queue.h"
#include "hermes/hermes_types.h"

namespace hermes::data_stager {

#include "data_stager_methods.h"
#include "hrun/hrun_namespace.h"
using hrun::proc_queue::TypedPushTask;
using hrun::proc_queue::PushTask;

/**
 * A task to create data_stager
 * */
using hrun::Admin::CreateTaskStateTask;
struct ConstructTask : public CreateTaskStateTask {
  TaskStateId blob_mdm_;

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
                const std::vector<PriorityInfo> &queue_info,
                const TaskStateId &blob_mdm)
      : CreateTaskStateTask(alloc, task_node, domain_id, state_name,
                            "data_stager", id, queue_info) {
    // Custom params
    blob_mdm_ = blob_mdm;
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    CreateTaskStateTask::SerializeStart(ar);
    ar(blob_mdm_);
  }

  HSHM_ALWAYS_INLINE
  ~ConstructTask() {
    // Custom params
  }
};

/** A task to destroy data_stager */
using hrun::Admin::DestroyTaskStateTask;
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

/**
 * Register a new stager
 * */
struct RegisterStagerTask : public Task, TaskFlags<TF_SRL_SYM | TF_REPLICA> {
  hermes::BucketId bkt_id_;
  hipc::ShmArchive<hipc::string> url_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  RegisterStagerTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  RegisterStagerTask(hipc::Allocator *alloc,
                     const TaskNode &task_node,
                     const TaskStateId &state_id,
                     hermes::BucketId bkt_id,
                     const hshm::charbuf &url) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = bkt_id.hash_;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kRegisterStager;
    task_flags_.SetBits(TASK_LOW_LATENCY | TASK_FIRE_AND_FORGET);
    domain_id_ = DomainId::GetGlobal();

    // Custom params
    bkt_id_ = bkt_id;
    HSHM_MAKE_AR(url_, alloc, url);
  }

  /** Destructor */
  HSHM_ALWAYS_INLINE
  ~RegisterStagerTask() {
    HSHM_DESTROY_AR(url_)
  }

  /** Duplicate message */
  void Dup(hipc::Allocator *alloc, RegisterStagerTask &other) {
    task_dup(other);
  }

  /** Process duplicate message output */
  void DupEnd(u32 replica, RegisterStagerTask &dup_task) {
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(bkt_id_, url_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
  }

  /** Begin replication */
  void ReplicateStart(u32 count) {}

  /** Finalize replication */
  void ReplicateEnd() {}

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    hrun::LocalSerialize srl(group);
    srl << bkt_id_.unique_;
    srl << bkt_id_.node_id_;
    return 0;
  }
};

/**
 * Unregister a new stager
 * */
struct UnregisterStagerTask : public Task, TaskFlags<TF_SRL_SYM | TF_REPLICA> {
  hermes::BucketId bkt_id_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  UnregisterStagerTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  UnregisterStagerTask(hipc::Allocator *alloc,
                       const TaskNode &task_node,
                       const TaskStateId &state_id,
                       const hermes::BucketId &bkt_id) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = bkt_id.hash_;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kUnregisterStager;
    task_flags_.SetBits(TASK_FIRE_AND_FORGET);
    domain_id_ = DomainId::GetGlobal();

    // Custom params
    bkt_id_ = bkt_id;
  }

  /** Duplicate message */
  void Dup(hipc::Allocator *alloc, UnregisterStagerTask &other) {
    task_dup(other);
  }

  /** Process duplicate message output */
  void DupEnd(u32 replica, UnregisterStagerTask &dup_task) {
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(bkt_id_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
  }

  /** Begin replication */
  void ReplicateStart(u32 count) {}

  /** Finalize replication */
  void ReplicateEnd() {}

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

/**
 * A task to stage in data from a remote source
 * */
struct StageInTask : public Task, TaskFlags<TF_LOCAL> {
  hermes::BucketId bkt_id_;
  hipc::ShmArchive<hipc::charbuf> blob_name_;
  float score_;
  u32 node_id_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  StageInTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  StageInTask(hipc::Allocator *alloc,
              const TaskNode &task_node,
              const TaskStateId &state_id,
              const BucketId &bkt_id,
              const hshm::charbuf &blob_name,
              float score,
              u32 node_id) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = bkt_id.hash_;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kStageIn;
    task_flags_.SetBits(TASK_COROUTINE | TASK_LOW_LATENCY | TASK_REMOTE_DEBUG_MARK);
    domain_id_ = DomainId::GetLocal();

    // Custom params
    bkt_id_ = bkt_id;
    HSHM_MAKE_AR(blob_name_, alloc, blob_name);
    score_ = score;
    node_id_ = node_id;
  }

  /** Destructor */
  HSHM_ALWAYS_INLINE
  ~StageInTask() {
    HSHM_DESTROY_AR(blob_name_)
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    hrun::LocalSerialize srl(group);
    srl << bkt_id_.unique_;
    srl << bkt_id_.node_id_;
    return 0;
  }
};

/**
 * A task to stage data out of a hermes to a remote source
 * */
struct StageOutTask : public Task, TaskFlags<TF_LOCAL> {
  hermes::BucketId bkt_id_;
  hipc::ShmArchive<hipc::charbuf> blob_name_;
  hipc::Pointer data_;
  size_t data_size_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  StageOutTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  StageOutTask(hipc::Allocator *alloc,
               const TaskNode &task_node,
               const TaskStateId &state_id,
               const BucketId &bkt_id,
               const hshm::charbuf &blob_name,
               const hipc::Pointer &data,
               size_t data_size,
               u32 task_flags): Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = bkt_id.hash_;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kStageOut;
    task_flags_.SetBits(task_flags | TASK_COROUTINE | TASK_LOW_LATENCY | TASK_REMOTE_DEBUG_MARK);
    domain_id_ = DomainId::GetLocal();

    // Custom params
    bkt_id_ = bkt_id;
    HSHM_MAKE_AR(blob_name_, alloc, blob_name);
    data_ = data;
    data_size_ = data_size;
  }

  /** Destructor */
  HSHM_ALWAYS_INLINE
  ~StageOutTask() {
    HSHM_DESTROY_AR(blob_name_)
    if (IsDataOwner()) {
      HRUN_CLIENT->FreeBuffer(data_);
    }
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    hrun::LocalSerialize srl(group);
    srl << bkt_id_.unique_;
    srl << bkt_id_.node_id_;
    return 0;
  }
};

}  // namespace hrun::data_stager

#endif  // HRUN_TASKS_TASK_TEMPL_INCLUDE_data_stager_data_stager_TASKS_H_
