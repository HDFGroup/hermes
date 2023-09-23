//
// Created by lukemartinlogan on 6/27/23.
//

#include "labstor/api/labstor_runtime.h"
#include "labstor/work_orchestrator/worker.h"
#include "labstor/work_orchestrator/work_orchestrator.h"

namespace labstor {

void Worker::Loop() {
  pid_ = GetLinuxTid();
  WorkOrchestrator *orchestrator = LABSTOR_WORK_ORCHESTRATOR;
  while (orchestrator->IsAlive()) {
    try {
      Run();
    } catch (hshm::Error &e) {
      HELOG(kFatal, "(node {}) Worker {} caught an error: {}", LABSTOR_CLIENT->node_id_, id_, e.what());
    }
    // Yield();
  }
  Run();
}

void Worker::Run() {
  if (poll_queues_.size() > 0) {
    _PollQueues();
  }
  if (relinquish_queues_.size() > 0) {
    _RelinquishQueues();
  }
  for (WorkEntry &work_entry : work_queue_) {
    if (!work_entry.lane_->flags_.Any(QUEUE_LOW_LATENCY)) {
      work_entry.count_ += 1;
      if (work_entry.count_ % 4096 != 0) {
        continue;
      }
    }
    PollGrouped(work_entry);
  }
}

void Worker::PollGrouped(WorkEntry &work_entry) {
  Lane *lane = work_entry.lane_;
  Task *task;
  LaneData *entry;
  int off = 0;
  RunContext ctx;
  ctx.lane_id_ = work_entry.lane_id_;
  for (int i = 0; i < 1024; ++i) {
    // Get the task message
    if (lane->peek(entry, off).IsNull()) {
      return;
    }
    if (entry->complete_) {
      PopTask(lane, off);
      continue;
    }
    task = LABSTOR_CLIENT->GetPrivatePointer<Task>(entry->p_);
    // Get the task state
    TaskState *exec = LABSTOR_TASK_REGISTRY->GetTaskState(task->task_state_);
    if (!exec) {
      HELOG(kFatal, "(node {}) Could not find the task state: {}",
            LABSTOR_CLIENT->node_id_, task->task_state_);
      entry->complete_ = true;
      EndTask(lane, task, off);
      continue;
    }
    // Attempt to run the task if it's ready and runnable
    bool is_remote = task->domain_id_.IsRemote(LABSTOR_RPC->GetNumHosts(), LABSTOR_CLIENT->node_id_);
    if (!task->IsRunDisabled() && CheckTaskGroup(task, exec, work_entry.lane_id_, task->task_node_, is_remote)) {
      // Execute or schedule task
      if (is_remote) {
        auto ids = LABSTOR_RUNTIME->ResolveDomainId(task->domain_id_);
        LABSTOR_REMOTE_QUEUE->Disperse(task, exec, ids);
        task->DisableRun();
        task->SetUnordered();
      } else if (task->IsBlocking()) {
        task->DisableRun();
        task->SetStarted();
        entry->thread_ = LABSTOR_WORK_ORCHESTRATOR->SpawnAsyncThread(&Worker::RunBlocking, task);
      } else {
        task->SetStarted();
        exec->Run(task->method_, task, ctx);
      }
    }
    // Cleanup on task completion
    if (task->IsModuleComplete()) {
//      HILOG(kDebug, "(node {}) Ending task: task_node={} task_state={} lane={} queue={} worker={}",
//            LABSTOR_CLIENT->node_id_, task->task_node_, task->task_state_, lane_id, queue->id_, id_);
      entry->complete_ = true;
      if (task->IsBlocking()) {
        ABT_thread_join(entry->thread_);
      }
      RemoveTaskGroup(task, exec, work_entry.lane_id_, is_remote);
      EndTask(lane, task, off);
    } else {
      off += 1;
    }
  }
}

void Worker::RunBlocking(void *data) {
  Task *task = reinterpret_cast<Task *>(data);
  TaskState *exec = LABSTOR_TASK_REGISTRY->GetTaskState(task->task_state_);
  RunContext ctx(0);
  exec->Run(task->method_, task, ctx);
}

}  // namespace labstor