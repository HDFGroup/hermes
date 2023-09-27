//
// Created by lukemartinlogan on 6/29/23.
//

#ifndef LABSTOR_bdev_H_
#define LABSTOR_bdev_H_

#include "bdev_tasks.h"

namespace hermes::bdev {

/**
 * BDEV Client API
 * */
class Client : public TaskLibClient {
 public:
  DomainId domain_id_;
  MonitorTask *monitor_task_;
  size_t max_cap_;      /**< maximum capacity of the target */
  double bandwidth_;    /**< the bandwidth of the device */
  double latency_;      /**< the latency of the device */
  float score_;         /**< Relative importance of this tier */

 public:
  /** Copy dev info */
  void CopyDevInfo(DeviceInfo &dev_info) {
    max_cap_ = dev_info.capacity_;
    bandwidth_ = dev_info.bandwidth_;
    latency_ = dev_info.latency_;
    score_ = 1;
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
    QueueManagerInfo &qm = LABSTOR_CLIENT->server_config_.queue_manager_;
    std::vector<PriorityInfo> queue_info = {
        {1, 1, qm.queue_depth_, 0},
        {1, 1, qm.queue_depth_, QUEUE_LONG_RUNNING},
        {4, 4, qm.queue_depth_, QUEUE_LOW_LATENCY}
    };
    return LABSTOR_ADMIN->AsyncCreateTaskState<ConstructTask>(
        task_node, domain_id, state_name, lib_name, id_,
        queue_info, dev_info);
  }
  void AsyncCreateComplete(ConstructTask *task) {
    if (task->IsComplete()) {
      id_ = task->id_;
      queue_id_ = QueueId(id_);
      monitor_task_ = AsyncMonitor(task->task_node_, 100).ptr_;
      // LABSTOR_CLIENT->DelTask(task);
    }
  }
  LABSTOR_TASK_NODE_ROOT(AsyncCreateTaskState);
  template<typename ...Args>
  HSHM_ALWAYS_INLINE
  void CreateRoot(Args&& ...args) {
    auto *task = AsyncCreateRoot(std::forward<Args>(args)...);
    task->Wait();
    AsyncCreateComplete(task);
  }

  /** Destroy task state + queue */
  HSHM_ALWAYS_INLINE
  void DestroyRoot(const std::string &state_name) {
    LABSTOR_ADMIN->DestroyTaskStateRoot(domain_id_, id_);
    monitor_task_->SetModuleComplete();
  }

  /** BDEV monitoring task */
  HSHM_ALWAYS_INLINE
  void AsyncMonitorConstruct(MonitorTask *task,
                             const TaskNode &task_node,
                             size_t freq_ms) {
    LABSTOR_CLIENT->ConstructTask<MonitorTask>(
        task, task_node, domain_id_, id_, freq_ms, max_cap_);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(Monitor);

  /** Update bdev capacity */
  void AsyncUpdateCapacityConstruct(UpdateCapacityTask *task,
                                    const TaskNode &task_node,
                                    ssize_t size) {
    LABSTOR_CLIENT->ConstructTask<UpdateCapacityTask>(
        task, task_node, domain_id_, id_, size);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(UpdateCapacity);

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
                              std::vector<BufferInfo> &buffers) {
    LABSTOR_CLIENT->ConstructTask<AllocateTask>(
        task, task_node, domain_id_, id_, size, &buffers);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(Allocate);

  /** Free data */
  HSHM_ALWAYS_INLINE
  void AsyncFreeConstruct(FreeTask *task,
                          const TaskNode &task_node,
                          const std::vector<BufferInfo> &buffers,
                          bool fire_and_forget) {
    LABSTOR_CLIENT->ConstructTask<FreeTask>(
        task, task_node, domain_id_, id_, buffers, fire_and_forget);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(Free);

  /** Allocate buffers from the bdev */
  HSHM_ALWAYS_INLINE
  void AsyncWriteConstruct(WriteTask *task,
                           const TaskNode &task_node,
                           const char *data, size_t off, size_t size) {
    LABSTOR_CLIENT->ConstructTask<WriteTask>(
        task, task_node, domain_id_, id_, data, off, size);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(Write);

  /** Allocate buffers from the bdev */
  HSHM_ALWAYS_INLINE
  void AsyncReadConstruct(ReadTask *task,
                          const TaskNode &task_node,
                          char *data, size_t off, size_t size) {
    LABSTOR_CLIENT->ConstructTask<ReadTask>(
        task, task_node, domain_id_, id_, data, off, size);
  }
  LABSTOR_TASK_NODE_PUSH_ROOT(Read);
};

class Server {
 public:
  ssize_t rem_cap_;

 public:
  void UpdateCapacity(UpdateCapacityTask *task) {
    rem_cap_ += task->diff_;
  }

  void Monitor(MonitorTask *task) {
    task->rem_cap_ = rem_cap_;
  }
};

}  // namespace hermes::bdev

namespace hermes {
typedef bdev::Client TargetInfo;
}  // namespace hermes

#endif  // LABSTOR_bdev_H_
