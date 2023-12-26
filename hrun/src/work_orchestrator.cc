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
  size_t num_workers = config_->wo_.max_dworkers_ + config->wo_.max_oworkers_ + 1;
  workers_.reserve(num_workers);
  int worker_id = 0;
  // Spawn admin worker
  workers_.emplace_back(std::make_unique<Worker>(worker_id, 0, xstream_));
  admin_worker_ = workers_.back().get();
  ++worker_id;
  // Spawn dedicated workers (dworkers)
  u32 num_dworkers = config_->wo_.max_dworkers_;
  for (; worker_id < num_dworkers; ++worker_id) {
    int cpu_id = worker_id;
    workers_.emplace_back(std::make_unique<Worker>(worker_id, cpu_id, xstream_));
    Worker &worker = *workers_.back();
    worker.EnableContinuousPolling();
    dworkers_.emplace_back(&worker);
  }
  // Spawn overlapped workers (oworkers)
  for (; worker_id < num_workers; ++worker_id) {
    int cpu_id = (int)(num_dworkers + (worker_id - num_dworkers) / config->wo_.owork_per_core_);
    workers_.emplace_back(std::make_unique<Worker>(worker_id, cpu_id, xstream_));
    Worker &worker = *workers_.back();
    worker.DisableContinuousPolling();
    oworkers_.emplace_back(&worker);
  }
  stop_runtime_ = false;
  kill_requested_ = false;

  // Wait for pids to become non-zero
  while (true) {
    bool all_pids_nonzero = true;
    for (std::unique_ptr<Worker> &worker : workers_) {
      if (worker->pid_ == 0) {
        all_pids_nonzero = false;
        break;
      }
    }
    if (all_pids_nonzero) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }

  // Schedule admin queue on first overlapping worker
  MultiQueue *admin_queue = qm.GetQueue(qm.admin_queue_);
  LaneGroup *admin_group = &admin_queue->GetGroup(0);
  for (u32 lane_id = 0; lane_id < admin_group->num_lanes_; ++lane_id) {
    admin_worker_->PollQueues({WorkEntry(0, lane_id, admin_queue)});
  }
  admin_group->num_scheduled_ = admin_group->num_lanes_;

  // Dedicate CPU cores to this runtime
  DedicateCores();

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

/** Get all PIDs of active workers */
std::vector<int> WorkOrchestrator::GetWorkerPids() {
  std::vector<int> pids;
  pids.reserve(workers_.size());
  for (std::unique_ptr<Worker> &worker : workers_) {
    pids.push_back(worker->pid_);
  }
  return pids;
}

/** Get the complement of worker cores */
std::vector<int> WorkOrchestrator::GetWorkerCoresComplement() {
  std::vector<int> cores;
  cores.reserve(HERMES_SYSTEM_INFO->ncpu_);
  for (int i = 0; i < HERMES_SYSTEM_INFO->ncpu_; ++i) {
    cores.push_back(i);
  }
  for (std::unique_ptr<Worker> &worker : workers_) {
    cores.erase(std::remove(cores.begin(), cores.end(), worker->affinity_), cores.end());
  }
  return cores;
}

/** Dedicate cores */
void WorkOrchestrator::DedicateCores() {
  ProcessAffiner affiner;
  std::vector<int> worker_pids = GetWorkerPids();
  std::vector<int> cpu_ids = GetWorkerCoresComplement();
  affiner.IgnorePids(worker_pids);
  affiner.SetCpus(cpu_ids);
  int count = affiner.AffineAll();
  // HILOG(kInfo, "Affining {} processes to {} cores", count, cpu_ids.size());
}

}  // namespace hrun
