//
// Created by lukemartinlogan on 8/14/23.
//

#ifndef HRUN_TASKS_BDEV_INCLUDE_BDEV_BDEV_TASKS_H_
#define HRUN_TASKS_BDEV_INCLUDE_BDEV_BDEV_TASKS_H_

#include <libaio.h>
#include "hrun/api/hrun_client.h"
#include "hrun/task_registry/task_lib.h"
#include "hrun_admin/hrun_admin.h"
#include "hrun/queue_manager/queue_manager_client.h"
#include "hermes/hermes_types.h"
#include "hermes/config_server.h"
#include "proc_queue/proc_queue.h"
#include "hermes/score_histogram.h"

namespace hermes::bdev {

#include "bdev_methods.h"
#include "hrun/hrun_namespace.h"

/**
 * A task to create hermes_mdm
 * */
using hrun::Admin::CreateTaskStateTask;
struct ConstructTask : public CreateTaskStateTask {
  IN DeviceInfo info_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  ConstructTask(hipc::Allocator *alloc) : CreateTaskStateTask(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  ConstructTask(hipc::Allocator *alloc,
                const TaskNode &task_node,
                const DomainId &domain_id,
                const std::string &state_name,
                const std::string &lib_name,
                const TaskStateId &id,
                const std::vector<PriorityInfo> &queue_info,
                DeviceInfo &info)
      : CreateTaskStateTask(alloc, task_node, domain_id, state_name,
                            lib_name, id, queue_info) {
    // Custom params
    info_ = info;
  }
};

/** A task to destroy hermes_mdm */
using hrun::Admin::DestroyTaskStateTask;
struct DestructTask : public DestroyTaskStateTask {
  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  DestructTask(hipc::Allocator *alloc) : DestroyTaskStateTask(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  DestructTask(hipc::Allocator *alloc,
               const TaskNode &task_node,
               TaskStateId &state_id,
               const DomainId &domain_id)
      : DestroyTaskStateTask(alloc, task_node, domain_id, state_id) {}

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

/**
 * A custom task in bdev
 * */
struct AllocateTask : public Task, TaskFlags<TF_LOCAL> {
  IN size_t size_;  /**< Size in buf */
  IN float score_;  /**< Score of the blob allocating stuff */
  OUT std::vector<BufferInfo> *buffers_;
  OUT size_t alloc_size_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  AllocateTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  AllocateTask(hipc::Allocator *alloc,
               const TaskNode &task_node,
               const DomainId &domain_id,
               const TaskStateId &state_id,
               size_t size,
               float score,
               std::vector<BufferInfo> *buffers) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kAllocate;
    task_flags_.SetBits(TASK_UNORDERED | TASK_REMOTE_DEBUG_MARK);
    domain_id_ = domain_id;

    // Free params
    size_ = size;
    score_ = score;
    buffers_ = buffers;
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

/**
 * A custom task in bdev
 * */
struct FreeTask : public Task, TaskFlags<TF_LOCAL> {
  IN std::vector<BufferInfo> buffers_;
  IN float score_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  FreeTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  FreeTask(hipc::Allocator *alloc,
           const TaskNode &task_node,
           const DomainId &domain_id,
           const TaskStateId &state_id,
           float score,
           const std::vector<BufferInfo> &buffers,
           bool fire_and_forget) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kFree;
    task_flags_.SetBits(0);
    if (fire_and_forget) {
      task_flags_.SetBits(TASK_FIRE_AND_FORGET | TASK_UNORDERED | TASK_REMOTE_DEBUG_MARK);
    }
    domain_id_ = domain_id;

    // Free params
    buffers_ = buffers;
    score_ = score;
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

/**
 * A custom task in bdev
 * */
struct WriteTask : public Task, TaskFlags<TF_LOCAL> {
  IN const char *buf_;    /**< Data in memory */
  IN size_t disk_off_;    /**< Offset on disk */
  IN size_t size_;        /**< Size in buf */
  TEMP int phase_ = 0;
  TEMP io_context_t ctx_ = 0;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  WriteTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  WriteTask(hipc::Allocator *alloc,
            const TaskNode &task_node,
            const DomainId &domain_id,
            const TaskStateId &state_id,
            const char *buf,
            size_t disk_off,
            size_t size) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = disk_off;
    prio_ = TaskPrio::kHighLatency;
    task_state_ = state_id;
    method_ = Method::kWrite;
    task_flags_.SetBits(TASK_UNORDERED | TASK_REMOTE_DEBUG_MARK);
    domain_id_ = domain_id;

    // Free params
    buf_ = buf;
    disk_off_ = disk_off;
    size_ = size;
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

/**
 * A custom task in bdev
 * */
struct ReadTask : public Task, TaskFlags<TF_LOCAL> {
  IN char *buf_;         /**< Data in memory */
  IN size_t disk_off_;   /**< Offset on disk */
  IN size_t size_;       /**< Size in disk buf */
  TEMP int phase_ = 0;
  TEMP io_context_t ctx_ = 0;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  ReadTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  ReadTask(hipc::Allocator *alloc,
           const TaskNode &task_node,
           const DomainId &domain_id,
           const TaskStateId &state_id,
           char *buf,
           size_t disk_off,
           size_t size) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = disk_off;
    if (size < KILOBYTES(8)) {
      prio_ = TaskPrio::kLowLatency;
    } else {
      prio_ = TaskPrio::kHighLatency;
    }
    task_state_ = state_id;
    method_ = Method::kRead;
    task_flags_.SetBits(TASK_UNORDERED | TASK_REMOTE_DEBUG_MARK);
    domain_id_ = domain_id;

    // Free params
    buf_ = buf;
    disk_off_ = disk_off;
    size_ = size;
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

/** A task to monitor bdev statistics */
struct StatBdevTask : public Task, TaskFlags<TF_LOCAL> {
  OUT size_t rem_cap_;  /**< Remaining capacity of the target */
  OUT Histogram score_hist_;  /**< Score distribution */

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  StatBdevTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  StatBdevTask(hipc::Allocator *alloc,
               const TaskNode &task_node,
               const DomainId &domain_id,
               const TaskStateId &state_id,
               size_t freq_ms,
               size_t rem_cap) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kLongRunning;
    task_state_ = state_id;
    method_ = Method::kStatBdev;
    task_flags_.SetBits(TASK_LONG_RUNNING | TASK_REMOTE_DEBUG_MARK);
    SetPeriodMs(freq_ms);
    domain_id_ = domain_id;

    // Custom
    rem_cap_ = rem_cap;
    score_hist_.Resize(10);
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

/** A task to monitor bdev statistics */
struct UpdateScoreTask : public Task, TaskFlags<TF_LOCAL> {
  OUT float old_score_;
  OUT float new_score_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  UpdateScoreTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  UpdateScoreTask(hipc::Allocator *alloc,
                  const TaskNode &task_node,
                  const DomainId &domain_id,
                  const TaskStateId &state_id,
                  float old_score, float new_score) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kUpdateScore;
    task_flags_.SetBits(TASK_LOW_LATENCY | TASK_FIRE_AND_FORGET | TASK_REMOTE_DEBUG_MARK);
    domain_id_ = domain_id;

    // Custom
    old_score_ = old_score;
    new_score_ = new_score;
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

}  // namespace hermes::bdev

#endif  // HRUN_TASKS_BDEV_INCLUDE_BDEV_BDEV_TASKS_H_
