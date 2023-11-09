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

#include "hrun/work_orchestrator/work_orchestrator.h"
#include "hrun/work_orchestrator/worker.h"

namespace hrun {

void WorkOrchestrator::ServerInit(ServerConfig *config, QueueManager &qm) {
  config_ = config;

  // Initialize argobots
  ABT_init(0, nullptr);

  // Create argobots xstream
  int ret = ABT_xstream_create(ABT_SCHED_NULL, &xstream_);
  if (ret != ABT_SUCCESS) {
    HELOG(kFatal, "Could not create argobots xstream");
  }

  // Spawn workers on the stream
  size_t num_workers = config_->wo_.max_workers_;
  workers_.reserve(num_workers);
  for (u32 worker_id = 0; worker_id < num_workers; ++worker_id) {
    workers_.emplace_back(std::make_unique<Worker>(worker_id, xstream_));
    Worker &worker = *workers_.back();
    worker.SetCpuAffinity(worker_id % HERMES_SYSTEM_INFO->ncpu_);
  }
  stop_runtime_ = false;
  kill_requested_ = false;

  // Schedule admin queue on worker 0
  MultiQueue *admin_queue = qm.GetQueue(qm.admin_queue_);
  LaneGroup *admin_group = &admin_queue->GetGroup(0);
  for (u32 lane_id = 0; lane_id < admin_group->num_lanes_; ++lane_id) {
    Worker &worker = *workers_[0];
    worker.PollQueues({WorkEntry(0, lane_id, admin_queue)});
  }
  admin_group->num_scheduled_ = admin_group->num_lanes_;

  HILOG(kInfo, "Started {} workers", num_workers);
}

void WorkOrchestrator::Join() {
  kill_requested_.store(true);
  for (std::unique_ptr<Worker> &worker : workers_) {
    worker->thread_->join();
    ABT_xstream_join(xstream_);
    ABT_xstream_free(&xstream_);
  }
}

/** Get worker with this id */
Worker& WorkOrchestrator::GetWorker(u32 worker_id) {
  return *workers_[worker_id];
}

/** Get the number of workers */
size_t WorkOrchestrator::GetNumWorkers() {
  return workers_.size();
}

}  // namespace hrun
