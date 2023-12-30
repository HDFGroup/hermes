//
// Created by lukemartinlogan on 6/29/23.
//

#ifndef HRUN_bdev_H_
#define HRUN_bdev_H_

#include "bdev_tasks.h"
#include "hermes/score_histogram.h"

namespace hermes::bdev {

/**
 * BDEV Client API
 * */
class Client : public TaskLibClient {
 public:
  DomainId domain_id_;
  StatBdevTask *monitor_task_;
  size_t max_cap_;      /**< maximum capacity of the target */
  double bandwidth_;    /**< the bandwidth of the device */
  double latency_;      /**< the latency of the device */
  float score_;         /**< Relative importance of this tier */
  float bw_score_;       /**< Relative importance of this tier */
  f32 borg_min_thresh_;  /**< Capacity percentage too low */
  f32 borg_max_thresh_;  /**< Capacity percentage too high */

 public:
  Client() : score_(0) {}

  /** Copy dev info */
  void CopyDevInfo(DeviceInfo &dev_info) {
    max_cap_ = dev_info.capacity_;
    bandwidth_ = dev_info.bandwidth_;
    latency_ = dev_info.latency_;
    score_ = 0;
    borg_min_thresh_ = dev_info.borg_min_thresh_;
    borg_max_thresh_ = dev_info.borg_max_thresh_;
  }

  /** Async create task state */
  HSHM_ALWAYS_INLINE
  LPointer<ConstructTask> AsyncCreate(const TaskNode &task_node,
                                      const DomainId &domain_id,
                                      const std::string &state_name,
                                      const std::string &lib_name,
                                      DeviceInfo &dev_info) {
    domain_id_ = domain_id;
    id_ = TaskStateId::GetNull();
    CopyDevInfo(dev_info);
    QueueManagerInfo &qm = HRUN_CLIENT->server_config_.queue_manager_;
    std::vector<PriorityInfo> queue_info;
    return HRUN_ADMIN->AsyncCreateTaskState<ConstructTask>(
        task_node, domain_id, state_name, lib_name, id_,
        queue_info, dev_info);
  }
  void AsyncCreateComplete(ConstructTask *task) {
    if (task->IsModuleComplete()) {
      id_ = task->id_;
      Init(id_, HRUN_ADMIN->queue_id_);
      monitor_task_ = AsyncStatBdev(task->task_node_ + 1, 100).ptr_;
      HRUN_CLIENT->DelTask(task);
    }
  }
  HRUN_TASK_NODE_ROOT(AsyncCreateTaskState);
  template<typename ...Args>
  HSHM_ALWAYS_INLINE
  void CreateRoot(Args&& ...args) {
    LPointer<ConstructTask> task =
        AsyncCreateRoot(std::forward<Args>(args)...);
    task->Wait();
    AsyncCreateComplete(task.ptr_);
  }

  /** Destroy task state + queue */
  HSHM_ALWAYS_INLINE
  void DestroyRoot(const std::string &state_name) {
    HRUN_ADMIN->DestroyTaskStateRoot(domain_id_, id_);
    monitor_task_->SetModuleComplete();
  }

  /** BDEV monitoring task */
  HSHM_ALWAYS_INLINE
  void AsyncStatBdevConstruct(StatBdevTask *task,
                             const TaskNode &task_node,
                             size_t freq_ms) {
    HRUN_CLIENT->ConstructTask<StatBdevTask>(
        task, task_node, domain_id_, id_, freq_ms, max_cap_);
  }
  HRUN_TASK_NODE_PUSH_ROOT(StatBdev);

  /** Get bdev remaining capacity */
  HSHM_ALWAYS_INLINE
  size_t GetRemCap() const {
    return monitor_task_->rem_cap_;
  }

  /** Allocate buffers from the bdev */
  HSHM_ALWAYS_INLINE
  void AsyncAllocateConstruct(AllocateTask *task,
                              const TaskNode &task_node,
                              size_t size,
                              float score,
                              std::vector<BufferInfo> &buffers) {
    HRUN_CLIENT->ConstructTask<AllocateTask>(
        task, task_node, domain_id_, id_, score, size, &buffers);
  }
  HRUN_TASK_NODE_PUSH_ROOT(Allocate);

  /** Free data */
  HSHM_ALWAYS_INLINE
  void AsyncFreeConstruct(FreeTask *task,
                          const TaskNode &task_node,
                          float score,
                          const std::vector<BufferInfo> &buffers,
                          bool fire_and_forget) {
    HRUN_CLIENT->ConstructTask<FreeTask>(
        task, task_node, domain_id_, id_, score, buffers, fire_and_forget);
  }
  HRUN_TASK_NODE_PUSH_ROOT(Free);

  /** Allocate buffers from the bdev */
  HSHM_ALWAYS_INLINE
  void AsyncWriteConstruct(WriteTask *task,
                           const TaskNode &task_node,
                           const char *data, size_t off, size_t size) {
    HRUN_CLIENT->ConstructTask<WriteTask>(
        task, task_node, domain_id_, id_, data, off, size);
  }
  HRUN_TASK_NODE_PUSH_ROOT(Write);

  /** Allocate buffers from the bdev */
  HSHM_ALWAYS_INLINE
  void AsyncReadConstruct(ReadTask *task,
                          const TaskNode &task_node,
                          char *data, size_t off, size_t size) {
    HRUN_CLIENT->ConstructTask<ReadTask>(
        task, task_node, domain_id_, id_, data, off, size);
  }
  HRUN_TASK_NODE_PUSH_ROOT(Read);

  /** Update blob scores */
  HSHM_ALWAYS_INLINE
  void AsyncUpdateScoreConstruct(UpdateScoreTask *task,
                                 const TaskNode &task_node,
                                 float old_score, float new_score) {
    HRUN_CLIENT->ConstructTask<UpdateScoreTask>(
        task, task_node, domain_id_, id_,
        old_score, new_score);
  }
  HRUN_TASK_NODE_PUSH_ROOT(UpdateScore);
};

class Server {
 public:
  ssize_t rem_cap_;       /**< Remaining capacity */
  Histogram score_hist_;  /**< Score distribution */

 public:
  /** Update the blob score in this tier */
  void UpdateScore(UpdateScoreTask *task, RunContext &ctx) {
    if (task->old_score_ >= 0) {
      score_hist_.Decrement(task->old_score_);
    }
    score_hist_.Increment(task->new_score_);
  }
  void MonitorUpdateScore(u32 mode, UpdateScoreTask *task, RunContext &ctx) {
  }

  /** Stat capacity and scores */
  void StatBdev(StatBdevTask *task, RunContext &ctx) {
    task->rem_cap_ = rem_cap_;
    task->score_hist_ = score_hist_;
  }
  void MonitorStatBdev(u32 mode, StatBdevTask *task, RunContext &ctx) {
  }
};

}  // namespace hermes::bdev

namespace hermes {
typedef bdev::Client TargetInfo;

struct TargetStats {
 public:
  TargetId tgt_id_;
  u32 node_id_;
  size_t rem_cap_;      /**< Current remaining capacity */
  size_t max_cap_;      /**< maximum capacity of the target */
  double bandwidth_;    /**< the bandwidth of the device */
  double latency_;      /**< the latency of the device */
  float score_;         /**< Relative importance of this tier */

 public:
  /** Serialize */
  template<typename Ar>
  void serialize(Ar &ar) {
    ar(tgt_id_, node_id_, max_cap_, bandwidth_,
       latency_, score_, rem_cap_);
  }
};
}  // namespace hermes

#endif  // HRUN_bdev_H_
