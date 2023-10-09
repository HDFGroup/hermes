//
// Created by lukemartinlogan on 6/29/23.
//

#include "labstor_admin/labstor_admin.h"
#include "labstor/api/labstor_runtime.h"
#include "worch_queue_round_robin/worch_queue_round_robin.h"

namespace labstor::worch_queue_round_robin {

class Server : public TaskLib {
 public:
  u32 count_;

 public:
  void Construct(ConstructTask *task, RunContext &rctx) {
    count_ = 0;
    task->SetModuleComplete();
  }

  void Destruct(DestructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }

  void Schedule(ScheduleTask *task, RunContext &rctx) {
    // Check if any new queues need to be scheduled
    for (MultiQueue &queue : *LABSTOR_QM_RUNTIME->queue_map_) {
      if (queue.id_.IsNull()) {
        continue;
      }
      for (LaneGroup &lane_group : *queue.groups_) {
        // NOTE(llogan): Assumes a minimum of two workers, admin on worker 0.
        if (lane_group.IsLowPriority()) {
          for (u32 lane_id = lane_group.num_scheduled_; lane_id < lane_group.num_lanes_; ++lane_id) {
            // HILOG(kDebug, "Scheduling the queue {} (lane {})", queue.id_, lane_id);
            Worker &worker = LABSTOR_WORK_ORCHESTRATOR->workers_[0];
            worker.PollQueues({WorkEntry(lane_group.prio_, lane_id, &queue)});
          }
          lane_group.num_scheduled_ = lane_group.num_lanes_;
        } else {
          for (u32 lane_id = lane_group.num_scheduled_; lane_id < lane_group.num_lanes_; ++lane_id) {
            // HILOG(kDebug, "Scheduling the queue {} (lane {})", queue.id_, lane_id);
            u32 worker_id = (count_ % (LABSTOR_WORK_ORCHESTRATOR->workers_.size() - 1)) + 1;
            Worker &worker = LABSTOR_WORK_ORCHESTRATOR->workers_[worker_id];
            worker.PollQueues({WorkEntry(lane_group.prio_, lane_id, &queue)});
            count_ += 1;
          }
          lane_group.num_scheduled_ = lane_group.num_lanes_;
        }
      }
    }
  }

#include "worch_queue_round_robin/worch_queue_round_robin_lib_exec.h"
};

}  // namespace labstor

LABSTOR_TASK_CC(labstor::worch_queue_round_robin::Server, "worch_queue_round_robin");
