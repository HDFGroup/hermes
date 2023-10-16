//
// Created by lukemartinlogan on 8/11/23.
//

#ifndef HRUN_TASKS_TASK_TEMPL_INCLUDE_hermes_data_op_hermes_data_op_TASKS_H_
#define HRUN_TASKS_TASK_TEMPL_INCLUDE_hermes_data_op_hermes_data_op_TASKS_H_

#include "hrun/api/hrun_client.h"
#include "hrun/task_registry/task_lib.h"
#include "hrun_admin/hrun_admin.h"
#include "hrun/queue_manager/queue_manager_client.h"
#include "proc_queue/proc_queue.h"
#include "hermes/hermes_types.h"
#include "hermes_data_op/hermes_data_op.h"
#include "hermes_bucket_mdm/hermes_bucket_mdm.h"

namespace hermes::data_op {

#include "hermes_data_op_methods.h"
#include "hrun/hrun_namespace.h"
using hrun::proc_queue::TypedPushTask;
using hrun::proc_queue::PushTask;

/** The bucket to use for op data */
struct OpBucketName {
  std::string url_;  // URL of bucket
  BucketId bkt_id_;  // Bucket ID (internal)
  LPointer<bucket_mdm::GetOrCreateTagTask>
      bkt_id_task_;  // Task to get bucket ID (internal)

  /** Default constructor */
  OpBucketName() = default;

  /** Emplace constructor */
  OpBucketName(const std::string &url) : url_(url) {}

  template<typename Ar>
  void serialize(Ar &ar) {
    ar(url_);
  }
};

/** An operation to perform on a set of inputs */
struct Op {
  std::vector<OpBucketName> in_;  // Input URLs, indicates data format as well
  OpBucketName var_name_;         // Output URL
  std::string op_name_;           // Operation name
  u32 op_id_;                     // Operation ID (internal)

  template<typename Ar>
  void serialize(Ar &ar) {
    ar(in_, var_name_, op_name_);
  }
};

/** The graph of operations to perform on data */
struct OpGraph {
  std::vector<Op> ops_;

  template<typename Ar>
  void serialize(Ar &ar) {
    ar(ops_);
  }
};

/**
 * A task to create hermes_data_op
 * */
using hrun::Admin::CreateTaskStateTask;
struct ConstructTask : public CreateTaskStateTask {
  IN TaskStateId bkt_mdm_;
  IN TaskStateId blob_mdm_;

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
                const TaskStateId &bkt_mdm_id,
                const TaskStateId &blob_mdm_id)
      : CreateTaskStateTask(alloc, task_node, domain_id, state_name,
                            "hermes_data_op", id, queue_info) {
    // Custom params
    bkt_mdm_ = bkt_mdm_id;
    blob_mdm_ = blob_mdm_id;
  }

  HSHM_ALWAYS_INLINE
  ~ConstructTask() {
    // Custom params
  }
};

/** A task to destroy hermes_data_op */
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
 * Register an operation to perform on data
 * */
struct RegisterOpTask : public Task, TaskFlags<TF_SRL_SYM | TF_REPLICA> {
  IN hipc::ShmArchive<hipc::string> op_graph_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  RegisterOpTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  RegisterOpTask(hipc::Allocator *alloc,
                 const TaskNode &task_node,
                 const DomainId &domain_id,
                 const TaskStateId &state_id,
                 const OpGraph &graph) : Task(alloc) {
    // Store OpGraph
    std::stringstream ss;
    cereal::BinaryOutputArchive ar(ss);
    ar << graph;
    std::string op_graph_str = ss.str();
    HSHM_MAKE_AR(op_graph_, alloc, op_graph_str);

    // Initialize task
    task_node_ = task_node;
    lane_hash_ = std::hash<std::string>{}(op_graph_str);
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kRegisterOp;
    task_flags_.SetBits(TASK_COROUTINE);
    domain_id_ = domain_id;
  }

  /** Get opgraph */
  OpGraph GetOpGraph() {
    OpGraph graph;
    std::stringstream ss(op_graph_->str());
    cereal::BinaryInputArchive ar(ss);
    ar >> graph;
    return graph;
  }

  /** Destructor */
  ~RegisterOpTask() {
    HSHM_DESTROY_AR(op_graph_);
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(op_graph_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
  }

  /** Duplicate message */
  void Dup(hipc::Allocator *alloc, RegisterOpTask &other) {
    task_dup(other);
    HSHM_MAKE_AR(op_graph_, alloc, *other.op_graph_);
  }

  /** Process duplicate message output */
  void DupEnd(u32 replica, RegisterOpTask &dup_task) {
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
 * Register some data as being available for derived
 * quantity calculation
 * */
struct OpData {
  BucketId bkt_id_;
  BlobId blob_id_;
  size_t off_;
  size_t size_;
  u64 data_id_;
  std::string blob_name_;
  int refcnt_;

  OpData() : refcnt_(0) {}
};
struct RegisterDataTask : public Task, TaskFlags<TF_LOCAL> {
  IN OpData data_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  RegisterDataTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  RegisterDataTask(hipc::Allocator *alloc,
            const TaskNode &task_node,
            const TaskStateId &state_id,
            const BucketId &bkt_id,
            const std::string &blob_name,
            const BlobId &blob_id,
            size_t off,
            size_t size) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = bkt_id.hash_;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kRegisterData;
    task_flags_.SetBits(TASK_COROUTINE | TASK_FIRE_AND_FORGET);
    domain_id_ = DomainId::GetLocal();

    // Custom params
    data_.bkt_id_ = bkt_id;
    data_.blob_name_ = blob_name;
    data_.blob_id_ = blob_id;
    data_.off_ = off;
    data_.size_ = size;
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

/**
 * Run an operation over a piece of data
 * */
struct RunOpTask : public Task, TaskFlags<TF_LOCAL | TF_REPLICA> {
  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  RunOpTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  RunOpTask(hipc::Allocator *alloc,
             const TaskNode &task_node,
             const TaskStateId &state_id) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kRunOp;
    task_flags_.SetBits(TASK_LONG_RUNNING | TASK_COROUTINE | TASK_LANE_ALL);
    SetPeriodSec(1);  // TODO(llogan): don't hardcode this
    domain_id_ = DomainId::GetLocal();
  }

  /** Duplicate message */
  void Dup(hipc::Allocator *alloc, RunOpTask &other) {
    task_dup(other);
  }

  /** Process duplicate message output */
  void DupEnd(u32 replica, RunOpTask &dup_task) {}

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

}  // namespace hermes::data_op

#endif  // HRUN_TASKS_TASK_TEMPL_INCLUDE_hermes_data_op_hermes_data_op_TASKS_H_
