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
  TaskStateId bkt_mdm_;

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
                const TaskStateId &blob_mdm,
                const TaskStateId &bkt_mdm)
      : CreateTaskStateTask(alloc, task_node, domain_id, state_name,
                            "data_stager", id, queue_info) {
    // Custom params
    blob_mdm_ = blob_mdm;
    bkt_mdm_ = bkt_mdm;
    std::stringstream ss;
    cereal::BinaryOutputArchive ar(ss);
    ar(blob_mdm_, bkt_mdm_);
    std::string data = ss.str();
    *custom_ = data;
  }

  /** Deserialize parameters */
  void Deserialize() {
    std::string data = custom_->str();
    std::stringstream ss(data);
    cereal::BinaryInputArchive ar(ss);
    ar(blob_mdm_, bkt_mdm_);
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
  hipc::ShmArchive<hipc::string> tag_name_;
  hipc::ShmArchive<hipc::string> params_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  RegisterStagerTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  RegisterStagerTask(hipc::Allocator *alloc,
                     const TaskNode &task_node,
                     const TaskStateId &state_id,
                     hermes::BucketId bkt_id,
                     const hshm::charbuf &tag_name,
                     const hshm::charbuf &params) : Task(alloc) {
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
    HSHM_MAKE_AR(tag_name_, alloc, tag_name);
    HSHM_MAKE_AR(params_, alloc, params);
  }

  /** Destructor */
  HSHM_ALWAYS_INLINE
  ~RegisterStagerTask() {
    HSHM_DESTROY_AR(tag_name_)
    HSHM_DESTROY_AR(params_)
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
    ar(bkt_id_, tag_name_, params_);
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
 * Unregister a new stager
 * */
struct UnregisterStagerTask : public Task, TaskFlags<TF_SRL_SYM | TF_REPLICA> {
  IN hermes::BucketId bkt_id_;

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
  IN hermes::BucketId bkt_id_;
  IN hipc::ShmArchive<hipc::charbuf> blob_name_;
  IN float score_;
  IN u32 node_id_;

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
//    hrun::LocalSerialize srl(group);
//    srl << bkt_id_.unique_;
//    srl << bkt_id_.node_id_;
//    return 0;
    return TASK_UNORDERED;
  }
};

/**
 * A task to stage data out of a hermes to a remote source
 * */
struct StageOutTask : public Task, TaskFlags<TF_LOCAL> {
  IN hermes::BucketId bkt_id_;
  IN hipc::ShmArchive<hipc::charbuf> blob_name_;
  IN hipc::Pointer data_;
  IN size_t data_size_;

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
    return TASK_UNORDERED;
  }
};

/**
 * A task to stage data out in a Hermes deployment
 * */
struct UpdateSizeTask : public Task, TaskFlags<TF_LOCAL> {
  IN hermes::BucketId bkt_id_;
  IN hipc::ShmArchive<hipc::charbuf> blob_name_;
  IN size_t blob_off_, data_size_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  UpdateSizeTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  UpdateSizeTask(hipc::Allocator *alloc,
                 const TaskNode &task_node,
                 const TaskStateId &state_id,
                 const BucketId &bkt_id,
                 const hshm::charbuf &blob_name,
                 size_t blob_off,
                 size_t data_size,
                 u32 task_flags): Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = bkt_id.hash_;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kUpdateSize;
    task_flags_.SetBits(task_flags | TASK_FIRE_AND_FORGET | TASK_LOW_LATENCY | TASK_REMOTE_DEBUG_MARK);
    domain_id_ = DomainId::GetLocal();

    // Custom params
    bkt_id_ = bkt_id;
    HSHM_MAKE_AR(blob_name_, alloc, blob_name);
    blob_off_ = blob_off;
    data_size_ = data_size;
  }

  /** Destructor */
  HSHM_ALWAYS_INLINE
  ~UpdateSizeTask() {
    HSHM_DESTROY_AR(blob_name_)
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

}  // namespace hrun::data_stager

#endif  // HRUN_TASKS_TASK_TEMPL_INCLUDE_data_stager_data_stager_TASKS_H_
