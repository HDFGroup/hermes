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
        u32 num_lanes = lane_group.num_lanes_;
        if (lane_group.IsTethered()) {
          LaneGroup &tether_group = queue.GetGroup(lane_group.tether_);
          num_lanes = tether_group.num_scheduled_;
        }
        for (u32 lane_id = lane_group.num_scheduled_; lane_id < num_lanes; ++lane_id) {
          Lane &lane = lane_group.GetLane(lane_id);
          if (lane_group.IsTethered()) {
            LaneGroup &tether_group = queue.GetGroup(lane_group.tether_);
            Lane &tether_lane = tether_group.GetLane(lane_id);
            Worker &worker = *HRUN_WORK_ORCHESTRATOR->workers_[tether_lane.worker_id_];
            worker.PollQueues({WorkEntry(lane_group.prio_, lane_id, &queue)});
            lane.worker_id_ = worker.id_;
            HILOG(kDebug, "(node {}) Scheduling the queue {} (prio {}, lane {}, worker {})",
                  HRUN_CLIENT->node_id_, queue.id_, lane_group.prio_, lane_id, worker.id_);
          } else if (lane_group.IsLowLatency()) {
            u32 worker_off = count_lowlat_ % HRUN_WORK_ORCHESTRATOR->dworkers_.size();
            count_lowlat_ += 1;
            Worker &worker = *HRUN_WORK_ORCHESTRATOR->dworkers_[worker_off];
            worker.PollQueues({WorkEntry(lane_group.prio_, lane_id, &queue)});
            lane.worker_id_ = worker.id_;
            HILOG(kDebug, "(node {}) Scheduling the queue {} (prio {}, lane {}, worker {})",
                  HRUN_CLIENT->node_id_, queue.id_, lane_group.prio_, lane_id, worker.id_);
          } else {
            u32 worker_off = count_highlat_ % HRUN_WORK_ORCHESTRATOR->oworkers_.size();
            count_highlat_ += 1;
            Worker &worker = *HRUN_WORK_ORCHESTRATOR->oworkers_[worker_off];
            worker.PollQueues({WorkEntry(lane_group.prio_, lane_id, &queue)});
            HILOG(kDebug, "(node {}) Scheduling the queue {} (prio {}, lane {}, worker {})",
                  HRUN_CLIENT->node_id_, queue.id_, lane_group.prio_, lane_id, worker_off);
            lane.worker_id_ = worker.id_;
          }
        }
        lane_group.num_scheduled_ = num_lanes;
      }
    }
  }
  void MonitorSchedule(u32 mode, ScheduleTask *task, RunContext &rctx) {
  }

#include "worch_queue_round_robin/worch_queue_round_robin_lib_exec.h"
};

}  // namespace hrun

HRUN_TASK_CC(hrun::worch_queue_round_robin::Server, "worch_queue_round_robin");
