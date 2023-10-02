//
// Created by lukemartinlogan on 8/11/23.
//

#ifndef LABSTOR_TASKS_SMALL_MESSAGE_INCLUDE_SMALL_MESSAGE_SMALL_MESSAGE_TASKS_H_
#define LABSTOR_TASKS_SMALL_MESSAGE_INCLUDE_SMALL_MESSAGE_SMALL_MESSAGE_TASKS_H_

#include "labstor/api/labstor_client.h"
#include "labstor/task_registry/task_lib.h"
#include "labstor_admin/labstor_admin.h"
#include "labstor/queue_manager/queue_manager_client.h"
#include "proc_queue/proc_queue.h"

namespace labstor::small_message {

#include "small_message_methods.h"
#include "labstor/labstor_namespace.h"

/**
 * A task to create small_message
 * */
using labstor::Admin::CreateTaskStateTask;
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
                            "small_message", id, queue_info) {
  }
};

/** A task to destroy small_message */
using labstor::Admin::DestroyTaskStateTask;
struct DestructTask : public DestroyTaskStateTask {
  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  DestructTask(hipc::Allocator *alloc) : DestroyTaskStateTask(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE
  DestructTask(hipc::Allocator *alloc,
               const TaskNode &task_node,
               const DomainId &domain_id,
               TaskStateId &state_id)
      : DestroyTaskStateTask(alloc, task_node, domain_id, state_id) {}
};

/**
 * A custom task in small_message
 * */
struct MdTask : public Task, TaskFlags<TF_SRL_SYM | TF_REPLICA> {
  OUT hipc::pod_array<int, 1> ret_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  MdTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  MdTask(hipc::Allocator *alloc,
         const TaskNode &task_node,
         const DomainId &domain_id,
         TaskStateId &state_id) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kMd;
    task_flags_.SetBits(TASK_LOW_LATENCY);
    domain_id_ = domain_id;

    // Custom params
    ret_.construct(alloc, 1);
  }

  /** Duplicate message */
  void Dup(hipc::Allocator *alloc, MdTask &other) {
    task_dup(other);
  }

  /** Process duplicate message output */
  void DupEnd(u32 replica, MdTask &dup_task) {
  }

  /** Begin replication */
  void ReplicateStart(u32 count) {
    ret_.resize(count);
  }

  /** Finalize replication */
  void ReplicateEnd() {}

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
    ar(ret_[replica]);
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

/**
 * A custom task in small_message
 * */
struct MdPushTask : public Task, TaskFlags<TF_SRL_SYM | TF_REPLICA> {
  OUT hipc::pod_array<int, 1> ret_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  MdPushTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  MdPushTask(hipc::Allocator *alloc,
             const TaskNode &task_node,
             const DomainId &domain_id,
             TaskStateId &state_id) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kMd;
    task_flags_.SetBits(TASK_LOW_LATENCY);
    domain_id_ = domain_id;

    // Custom params
    ret_.construct(alloc, 1);
  }

  /** Duplicate message */
  void Dup(hipc::Allocator *alloc, MdPushTask &other) {
    task_dup(other);
  }

  /** Process duplicate message output */
  void DupEnd(u32 replica, MdPushTask &dup_task) {
  }

  /** Begin replication */
  void ReplicateStart(u32 count) {
    ret_.resize(count);
  }

  /** Finalize replication */
  void ReplicateEnd() {}

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
    ar(ret_[replica]);
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

/**
 * A custom task in small_message
 * */
struct IoTask : public Task, TaskFlags<TF_SRL_ASYM_START | TF_SRL_SYM_END> {
  static inline int const DATA_SIZE = 4096;
  IN char data_[DATA_SIZE];
  OUT int ret_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  IoTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  IoTask(hipc::Allocator *alloc,
         const TaskNode &task_node,
         const DomainId &domain_id,
         TaskStateId &state_id) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 3;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kIo;
    task_flags_.SetBits(0);
    domain_id_ = domain_id;

    // Custom params
    memset(data_, 10, DATA_SIZE);
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SaveStart(Ar &ar) {
    DataTransfer xfer(DT_RECEIVER_READ, data_, DATA_SIZE, domain_id_);
    task_serialize<Ar>(ar);
    ar & xfer;
  }

  /** Deserialize message call */
  template<typename Ar>
  void LoadStart(Ar &ar) {
    DataTransfer xfer;
    task_serialize<Ar>(ar);
    ar & xfer;
    memcpy(data_, xfer.data_, xfer.data_size_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
    ar(ret_);
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

}  // namespace labstor

#endif  // LABSTOR_TASKS_SMALL_MESSAGE_INCLUDE_SMALL_MESSAGE_SMALL_MESSAGE_TASKS_H_
