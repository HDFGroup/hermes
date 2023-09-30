//
// Created by lukemartinlogan on 8/11/23.
//

#ifndef LABSTOR_TASKS_TASK_TEMPL_INCLUDE_data_stager_data_stager_TASKS_H_
#define LABSTOR_TASKS_TASK_TEMPL_INCLUDE_data_stager_data_stager_TASKS_H_

#include "labstor/api/labstor_client.h"
#include "labstor/task_registry/task_lib.h"
#include "labstor_admin/labstor_admin.h"
#include "labstor/queue_manager/queue_manager_client.h"
#include "proc_queue/proc_queue.h"
#include "hermes/hermes_types.h"

namespace hermes::data_stager {

#include "data_stager_methods.h"
#include "labstor/labstor_namespace.h"
using labstor::proc_queue::TypedPushTask;
using labstor::proc_queue::PushTask;

/**
 * A task to create data_stager
 * */
using labstor::Admin::CreateTaskStateTask;
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

  HSHM_ALWAYS_INLINE
  ~ConstructTask() {
    // Custom params
  }
};

/** A task to destroy data_stager */
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

/**
 * Register a new stager
 * */
struct RegisterStagerTask : public Task, TaskFlags<TF_SRL_SYM> {
  hermes::BucketId bkt_id_;
  hipc::ShmArchive<hipc::string> url_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  RegisterStagerTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  RegisterStagerTask(hipc::Allocator *alloc,
                     const TaskNode &task_node,
                     const DomainId &domain_id,
                     const TaskStateId &state_id,
                     hermes::BucketId bkt_id,
                     hshm::charbuf &url) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kStageIn;
    task_flags_.SetBits(0);
    domain_id_ = domain_id;

    // Custom params
    bkt_id_ = bkt_id;
    HSHM_MAKE_AR(url_, alloc, url);
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

/**
 * A task to stage in data from a remote source
 * */
struct StageInTask : public Task, TaskFlags<TF_SRL_SYM> {
  hermes::BucketId bkt_id_;
  hipc::ShmArchive<hipc::charbuf> blob_name_;
  float score_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  StageInTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  StageInTask(hipc::Allocator *alloc,
              const TaskNode &task_node,
              const DomainId &domain_id,
              const TaskStateId &state_id) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kStageIn;
    task_flags_.SetBits(0);
    domain_id_ = domain_id;

    // Custom params
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

/**
 * A task to stage data out of a hermes to a remote source
 * */
struct StageOutTask : public Task, TaskFlags<TF_SRL_SYM> {
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
               const DomainId &domain_id,
               const TaskStateId &state_id) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kStageOut;
    task_flags_.SetBits(0);
    domain_id_ = domain_id;

    // Custom params
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

}  // namespace labstor::data_stager

#endif  // LABSTOR_TASKS_TASK_TEMPL_INCLUDE_data_stager_data_stager_TASKS_H_
