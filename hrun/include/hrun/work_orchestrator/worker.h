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

#ifndef HRUN_INCLUDE_HRUN_WORK_ORCHESTRATOR_WORKER_H_
#define HRUN_INCLUDE_HRUN_WORK_ORCHESTRATOR_WORKER_H_

#include "hrun/hrun_types.h"
#include "hrun/queue_manager/queue_manager_runtime.h"
#include <thread>
#include <queue>
#include "affinity.h"
#include "hrun/network/rpc_thallium.h"

static inline pid_t GetLinuxTid() {
  return syscall(SYS_gettid);
}

namespace hrun {

#define WORKER_CONTINUOUS_POLLING BIT_OPT(u32, 0)

/** Uniquely identify a queue lane */
struct WorkEntry {
  u32 prio_;
  u32 lane_id_;
  u32 count_;
  Lane *lane_;
  LaneGroup *group_;
  MultiQueue *queue_;
  hshm::Timepoint last_monitor_;
  hshm::Timepoint cur_time_;
  double sample_epoch_;

  /** Default constructor */
  HSHM_ALWAYS_INLINE
  WorkEntry() = default;

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE
  WorkEntry(u32 prio, u32 lane_id, MultiQueue *queue)
  : prio_(prio), lane_id_(lane_id), queue_(queue) {
    group_ = &queue->GetGroup(prio);
    lane_ = &queue->GetLane(*group_, lane_id);
    count_ = 0;
    cur_time_.Now();
  }

  /** Copy constructor */
  HSHM_ALWAYS_INLINE
  WorkEntry(const WorkEntry &other) {
    prio_ = other.prio_;
    lane_id_ = other.lane_id_;
    lane_ = other.lane_;
    group_ = other.group_;
    queue_ = other.queue_;
    cur_time_.Now();
  }

  /** Copy assignment */
  HSHM_ALWAYS_INLINE
  WorkEntry& operator=(const WorkEntry &other) {
    if (this != &other) {
      prio_ = other.prio_;
      lane_id_ = other.lane_id_;
      lane_ = other.lane_;
      group_ = other.group_;
      queue_ = other.queue_;
      cur_time_.Now();
    }
    return *this;
  }

  /** Move constructor */
  HSHM_ALWAYS_INLINE
  WorkEntry(WorkEntry &&other) noexcept {
    prio_ = other.prio_;
    lane_id_ = other.lane_id_;
    lane_ = other.lane_;
    group_ = other.group_;
    queue_ = other.queue_;
    cur_time_.Now();
  }

  /** Move assignment */
  HSHM_ALWAYS_INLINE
  WorkEntry& operator=(WorkEntry &&other) noexcept {
    if (this != &other) {
      prio_ = other.prio_;
      lane_id_ = other.lane_id_;
      lane_ = other.lane_;
      group_ = other.group_;
      queue_ = other.queue_;
      cur_time_.Now();
    }
    return *this;
  }

  /** Check if null */
  [[nodiscard]]
  HSHM_ALWAYS_INLINE bool IsNull() const {
    return queue_->IsNull();
  }

  /** Equality operator */
  HSHM_ALWAYS_INLINE
  bool operator==(const WorkEntry &other) const {
    return queue_ == other.queue_ && lane_id_ == other.lane_id_ &&
        prio_ == other.prio_;
  }
};

}  // namespace hrun

namespace std {
/** Hash function for WorkEntry */
template<>
struct hash<hrun::WorkEntry> {
  HSHM_ALWAYS_INLINE
      std::size_t
  operator()(const hrun::WorkEntry &key) const {
    return std::hash<hrun::MultiQueue*>{}(key.queue_) +
        std::hash<u32>{}(key.lane_id_) + std::hash<u64>{}(key.prio_);
  }
};
}  // namespace std

namespace hrun {

class Worker {
 public:
  u32 id_;  /**< Unique identifier of this worker */
  std::unique_ptr<std::thread> thread_;  /**< The worker thread handle */
  int pthread_id_;      /**< The worker pthread handle */
  // ABT_thread tl_thread_;
  int pid_;             /**< The worker process id */
  u32 numa_node_;       // TODO(llogan): track NUMA affinity
  std::vector<WorkEntry> work_queue_;  /**< The set of queues to poll */
  /** A set of queues to begin polling in a worker */
  hshm::spsc_queue<std::vector<WorkEntry>> poll_queues_;
  /** A set of queues to stop polling in a worker */
  hshm::spsc_queue<std::vector<WorkEntry>> relinquish_queues_;
  size_t sleep_us_;     /** Time the worker should sleep after a run */
  u32 retries_;         /** The number of times to repeat the internal run loop before sleeping */
  bitfield32_t flags_;  /** Worker metadata flags */
  std::unordered_map<hshm::charbuf, TaskNode>
      group_map_;        /** Determine if a task can be executed right now */
  hshm::charbuf group_;  /** The current group */
  WorkPending flush_;    /** Info needed for flushing ops */


 public:
  /** Constructor */
  Worker(u32 id, ABT_xstream &xstream) {
    poll_queues_.Resize(1024);
    relinquish_queues_.Resize(1024);
    id_ = id;
    sleep_us_ = 0;
    EnableContinuousPolling();
    retries_ = 1;
    pid_ = 0;
    thread_ = std::make_unique<std::thread>(&Worker::Loop, this);
    pthread_id_ = thread_->native_handle();
    // TODO(llogan): implement reserve for group
    group_.resize(512);
    group_.resize(0);
    /* int ret = ABT_thread_create_on_xstream(xstream,
                                           [](void *args) { ((Worker*)args)->Loop(); }, this,
                                           ABT_THREAD_ATTR_NULL, &tl_thread_);
    if (ret != ABT_SUCCESS) {
      HELOG(kFatal, "Couldn't spawn worker");
    }*/
  }

  /** Constructor without threading */
  Worker(u32 id) {
    poll_queues_.Resize(1024);
    relinquish_queues_.Resize(1024);
    id_ = id;
    sleep_us_ = 0;
    EnableContinuousPolling();
    retries_ = 1;
    pid_ = 0;
    pthread_id_ = GetLinuxTid();
    // TODO(llogan): implement reserve for group
    group_.resize(512);
    group_.resize(0);
  }

  /**
   * Tell worker to poll a set of queues
   * */
  void PollQueues(const std::vector<WorkEntry> &queues) {
    poll_queues_.emplace(queues);
  }

  /**
   * Tell worker to start relinquishing some of its queues
   * This function must be called from a single thread (outside of worker)
   * */
  void RelinquishingQueues(const std::vector<WorkEntry> &queues) {
    relinquish_queues_.emplace(queues);
  }

  /** Check if worker is still stealing queues */
  bool IsRelinquishingQueues() {
    return relinquish_queues_.size() > 0;
  }

  /** Set the sleep cycle */
  void SetPollingFrequency(size_t sleep_us, u32 num_retries) {
    sleep_us_ = sleep_us;
    retries_ = num_retries;
    flags_.UnsetBits(WORKER_CONTINUOUS_POLLING);
  }

  /** Enable continuous polling */
  void EnableContinuousPolling() {
    flags_.SetBits(WORKER_CONTINUOUS_POLLING);
  }

  /** Disable continuous polling */
  void DisableContinuousPolling() {
    flags_.UnsetBits(WORKER_CONTINUOUS_POLLING);
  }

  /** Set the CPU affinity of this worker */
  void SetCpuAffinity(int cpu_id) {
    ProcessAffiner::SetCpuAffinity(pid_, cpu_id);
  }

  /** The work loop */
  void Loop();

  /** Worker yields for a period of time */
  void Yield() {
    if (flags_.Any(WORKER_CONTINUOUS_POLLING)) {
      return;
    }
    if (sleep_us_ > 0) {
      HERMES_THREAD_MODEL->SleepForUs(sleep_us_);
    } else {
      HERMES_THREAD_MODEL->Yield();
    }
  }

  /** Worker merges the set of queues to poll */
  void _PollQueues() {
    std::vector<WorkEntry> work_queue;
    while (!poll_queues_.pop(work_queue).IsNull()) {
      for (const WorkEntry &entry : work_queue) {
        // HILOG(kDebug, "Scheduled queue {} (lane {})", entry.queue_->id_, entry.lane_);
        work_queue_.emplace_back(entry);
      }
    }
  }

  /** Relinquish queues. Called from wihtin Worker */
  void _RelinquishQueues() {
    std::vector<WorkEntry> work_queue;
    while (!poll_queues_.pop(work_queue).IsNull()) {
      for (auto &entry : work_queue) {
        work_queue_.erase(std::find(work_queue_.begin(),
                                    work_queue_.end(), entry));
      }
    }
  }

  /**
   * Poll work queue
   *
   * @return true if work was found, false otherwise
   * */
  void Run();

  HSHM_ALWAYS_INLINE
  bool CheckTaskGroup(Task *task, TaskState *exec,
                      u32 lane_id,
                      TaskNode node, const bool &is_remote) {
    if (is_remote || task->IsStarted() || task->IsLaneAll()) {
      return true;
    }
    int ret = exec->GetGroup(task->method_, task, group_);
    if (ret == TASK_UNORDERED || task->IsUnordered()) {
//      HILOG(kDebug, "(node {}) Task {} is unordered, so count remains 0 worker={}",
//            HRUN_CLIENT->node_id_, task->task_node_, id_);
      return true;
    }

#ifdef DEBUG
    // TODO(llogan): remove
    std::stringstream ss;
    for (int i = 0; i < group_.size(); ++i) {
      ss << std::to_string((int)group_[i]);
    }
#endif

    // Ensure that concurrent requests are not serialized
    LocalSerialize srl(group_);
    srl << lane_id;

    auto it = group_map_.find(group_);
    if (it == group_map_.end()) {
      node.node_depth_ = 1;
      group_map_.emplace(group_, node);
//      HILOG(kDebug, "(node {}) Increasing depth of group to {} worker={}",
//            HRUN_CLIENT->node_id_, node.node_depth_, id_);
      return true;
    }
    TaskNode &node_cmp = it->second;
    if (node_cmp.root_ == node.root_) {
      node_cmp.node_depth_ += 1;
//      HILOG(kDebug, "(node {}) Increasing depth of group to {} worker={}",
//            HRUN_CLIENT->node_id_, node_cmp.node_depth_, id_);
      return true;
    }
    return false;
  }

  HSHM_ALWAYS_INLINE
  void RemoveTaskGroup(Task *task, TaskState *exec,
                       u32 lane_id, const bool &is_remote) {
    if (is_remote) {
      return;
    }
    int ret = exec->GetGroup(task->method_, task, group_);
    if (ret == TASK_UNORDERED || task->IsUnordered()) {
//      HILOG(kDebug, "(node {}) Decreasing depth of group remains 0 (task_node={} worker={})",
//            HRUN_CLIENT->node_id_, task->task_node_, id_);
      return;
    }

#ifdef DEBUG
    // TODO(llogan): remove
    std::stringstream ss;
    for (int i = 0; i < group_.size(); ++i) {
      ss << std::to_string((int)group_[i]);
    }
#endif
    // Ensure that concurrent requests are not serialized
    LocalSerialize srl(group_);
    srl << lane_id;

    TaskNode &node_cmp = group_map_[group_];
    if (node_cmp.node_depth_ == 0) {
      HELOG(kFatal, "(node {}) Group {} depth is already 0 (task_node={} worker={})",
            HRUN_CLIENT->node_id_, task->task_node_, id_);
    }
    node_cmp.node_depth_ -= 1;
//    HILOG(kDebug, "(node {}) Decreasing depth of to {} (task_node={} worker={})",
//          HRUN_CLIENT->node_id_, node_cmp.node_depth_, task->task_node_, id_);
    if (node_cmp.node_depth_ == 0) {
      group_map_.erase(group_);
    }
  }

  HSHM_ALWAYS_INLINE
  void EndTask(Lane *lane, TaskState *exec, Task *task, int &off) {
    PopTask(lane, off);
    if (exec && task->IsFireAndForget()) {
      HRUN_CLIENT->DelTask<TaskState>(exec, task);
    } else {
      task->SetComplete();
    }
  }

  HSHM_ALWAYS_INLINE
  void PopTask(Lane *lane, int &off) {
    if (off == 0) {
      lane->pop();
    } else {
      off += 1;
    }
  }

  void PollGrouped(WorkEntry &entry);
  static void RunCoroutine(bctx::transfer_t t);
  static void RunPreemptive(void *data);
};

}  // namespace hrun

#endif  // HRUN_INCLUDE_HRUN_WORK_ORCHESTRATOR_WORKER_H_
