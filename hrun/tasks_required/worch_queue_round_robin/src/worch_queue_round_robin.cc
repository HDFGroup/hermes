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

#include "hrun_admin/hrun_admin.h"
#include "hrun/api/hrun_runtime.h"
#include "worch_queue_round_robin/worch_queue_round_robin.h"

namespace hrun::worch_queue_round_robin {

class Server : public TaskLib {
 public:
  u32 count_lowlat_;
  u32 count_highlat_;

 public:
  /** Construct work orchestrator queue scheduler */
  void Construct(ConstructTask *task, RunContext &rctx) {
    count_lowlat_ = 0;
    count_highlat_ = 0;
    task->SetModuleComplete();
  }
  void MonitorConstruct(u32 mode, ConstructTask *task, RunContext &rctx) {
  }

  /** Destroy work orchestrator queue scheduler */
  void Destruct(DestructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorDestruct(u32 mode, DestructTask *task, RunContext &rctx) {
  }

  /** Schedule work orchestrator queues */
  void Schedule(ScheduleTask *task, RunContext &rctx) {
    // Check if any new queues need to be scheduled
    for (MultiQueue &queue : *HRUN_QM_RUNTIME->queue_map_) {
      if (queue.id_.IsNull() || !queue.flags_.Any(QUEUE_READY)) {
        continue;
      }
      for (LaneGroup &lane_group : *queue.groups_) {
        // NOTE(llogan): Assumes a minimum of three workers, admin on worker 0.
        if (lane_group.IsLowPriority()) {
          for (u32 lane_id = lane_group.num_scheduled_; lane_id < lane_group.num_lanes_; ++lane_id) {
            // HILOG(kDebug, "Scheduling the queue {} (lane {})", queue.id_, lane_id);
            Worker &worker = *HRUN_WORK_ORCHESTRATOR->workers_[0];
            worker.PollQueues({WorkEntry(lane_group.prio_, lane_id, &queue)});
          }
          lane_group.num_scheduled_ = lane_group.num_lanes_;
        } else {
          u32 rem_workers = HRUN_WORK_ORCHESTRATOR->workers_.size() - 1;
          u32 off_lowlat = 1;
          u32 count_lowlat = rem_workers / 2;
          rem_workers -= count_lowlat;
          u32 off_highlat = off_lowlat + count_lowlat;
          u32 count_highlat = rem_workers;
          for (u32 lane_id = lane_group.num_scheduled_; lane_id < lane_group.num_lanes_; ++lane_id) {
            if (lane_group.IsLowLatency()) {
              u32 worker_id = (count_lowlat_ % count_lowlat) + off_lowlat;
              count_lowlat_ += 1;
              Worker &worker = *HRUN_WORK_ORCHESTRATOR->workers_[worker_id];
              worker.PollQueues({WorkEntry(lane_group.prio_, lane_id, &queue)});
              HILOG(kDebug, "Scheduling the queue {} (lane {}, worker {})", queue.id_, lane_id, worker_id);
            } else {
              u32 worker_id = (count_highlat_ % count_highlat) + off_highlat;
              count_highlat_ += 1;
              Worker &worker = *HRUN_WORK_ORCHESTRATOR->workers_[worker_id];
              worker.PollQueues({WorkEntry(lane_group.prio_, lane_id, &queue)});
              HILOG(kDebug, "Scheduling the queue {} (lane {}, worker {})", queue.id_, lane_id, worker_id);
            }
          }
          lane_group.num_scheduled_ = lane_group.num_lanes_;
        }
      }
    }
  }
  void MonitorSchedule(u32 mode, ScheduleTask *task, RunContext &rctx) {
  }

#include "worch_queue_round_robin/worch_queue_round_robin_lib_exec.h"
};

}  // namespace hrun

HRUN_TASK_CC(hrun::worch_queue_round_robin::Server, "worch_queue_round_robin");
