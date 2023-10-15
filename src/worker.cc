//
// Created by lukemartinlogan on 6/27/23.
//

#include "hrun/api/hrun_runtime.h"
#include "hrun/work_orchestrator/worker.h"
#include "hrun/work_orchestrator/work_orchestrator.h"

namespace hrun {

void Worker::Loop() {
  pid_ = GetLinuxTid();
  WorkOrchestrator *orchestrator = HRUN_WORK_ORCHESTRATOR;
  while (orchestrator->IsAlive()) {
    try {
      flush_.pending_ = 0;
      Run();
      if (flush_.flushing_ && flush_.pending_ == 0) {
        flush_.flushing_ = false;
      }
    } catch (hshm::Error &e) {
      HELOG(kFatal, "(node {}) Worker {} caught an error: {}", HRUN_CLIENT->node_id_, id_, e.what());
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
  hshm::Timepoint now;
  now.Now();
  for (WorkEntry &work_entry : work_queue_) {
//    if (!work_entry.lane_->flags_.Any(QUEUE_LOW_LATENCY)) {
//      work_entry.count_ += 1;
//      if (work_entry.count_ % 4096 != 0) {
//        continue;
//      }
//    }
    work_entry.cur_time_ = now;
    PollGrouped(work_entry);
  }
}

void Worker::PollGrouped(WorkEntry &work_entry) {
  int off = 0;
  Lane *&lane = work_entry.lane_;
  Task *task;
  LaneData *entry;
  for (int i = 0; i < 1024; ++i) {
    // Get the task message
    if (lane->peek(entry, off).IsNull()) {
      return;
    }
    if (entry->complete_) {
      PopTask(lane, off);
      continue;
    }
    task = HRUN_CLIENT->GetPrivatePointer<Task>(entry->p_);
    RunContext &rctx = task->ctx_;
    rctx.lane_id_ = work_entry.lane_id_;
    rctx.flush_ = &flush_;
    // Get the task state
    TaskState *&exec = rctx.exec_;
    exec = HRUN_TASK_REGISTRY->GetTaskState(task->task_state_);
    if (!exec) {
      HELOG(kFatal, "(node {}) Could not find the task state: {}",
            HRUN_CLIENT->node_id_, task->task_state_);
      entry->complete_ = true;
      EndTask(lane, exec, task, off);
      continue;
    }
    // Attempt to run the task if it's ready and runnable
    bool is_remote = task->domain_id_.IsRemote(HRUN_RPC->GetNumHosts(), HRUN_CLIENT->node_id_);
    if (!task->IsRunDisabled() &&
        CheckTaskGroup(task, exec, work_entry.lane_id_, task->task_node_, is_remote) &&
        task->ShouldRun(work_entry.cur_time_, flush_.flushing_)) {
#ifdef REMOTE_DEBUG
      if (task->task_state_ != HRUN_QM_CLIENT->admin_task_state_ &&
          !task->task_flags_.Any(TASK_REMOTE_DEBUG_MARK) &&
          task->method_ != TaskMethod::kConstruct &&
          HRUN_RUNTIME->remote_created_) {
        is_remote = true;
      }
      task->task_flags_.SetBits(TASK_REMOTE_DEBUG_MARK);
#endif
      // Execute or schedule task
      if (is_remote) {
        auto ids = HRUN_RUNTIME->ResolveDomainId(task->domain_id_);
        HRUN_REMOTE_QUEUE->Disperse(task, exec, ids);
        task->SetDisableRun();
        task->SetUnordered();
        task->UnsetCoroutine();
      } else if (task->IsLaneAll()) {
        HRUN_REMOTE_QUEUE->DisperseLocal(task, exec, work_entry.queue_, work_entry.group_);
        task->SetDisableRun();
        task->SetUnordered();
        task->UnsetCoroutine();
        task->UnsetLaneAll();
      } else if (task->IsCoroutine()) {
        if (!task->IsStarted()) {
          rctx.stack_ptr_ = malloc(rctx.stack_size_);
          if (rctx.stack_ptr_ == nullptr) {
            HILOG(kFatal, "The stack pointer of size {} is NULL",
                  rctx.stack_size_, rctx.stack_ptr_);
          }
          rctx.jmp_.fctx = bctx::make_fcontext(
              (char*)rctx.stack_ptr_ + rctx.stack_size_,
              rctx.stack_size_, &RunCoroutine);
          task->SetStarted();
        }
        rctx.jmp_ = bctx::jump_fcontext(rctx.jmp_.fctx, task);
        if (!task->IsStarted()) {
          rctx.jmp_.fctx = bctx::make_fcontext(
              (char*)rctx.stack_ptr_ + rctx.stack_size_,
              rctx.stack_size_, &RunCoroutine);
          task->SetStarted();
        }
      } else if (task->IsPreemptive()) {
        task->SetDisableRun();
        entry->thread_ = HRUN_WORK_ORCHESTRATOR->SpawnAsyncThread(&Worker::RunPreemptive, task);
      } else {
        task->SetStarted();
        exec->Run(task->method_, task, rctx);
      }
      task->DidRun(work_entry.cur_time_);
      if (!task->IsModuleComplete() && !task->IsFlush()) {
        flush_.pending_ += 1;
      }
    }
    // Cleanup on task completion
    if (task->IsModuleComplete()) {
//      HILOG(kDebug, "(node {}) Ending task: task_node={} task_state={} lane={} queue={} worker={}",
//            HRUN_CLIENT->node_id_, task->task_node_, task->task_state_, lane_id, queue->id_, id_);
      entry->complete_ = true;
      if (task->IsCoroutine()) {
        free(rctx.stack_ptr_);
      } else if (task->IsPreemptive()) {
        ABT_thread_join(entry->thread_);
      }
      RemoveTaskGroup(task, exec, work_entry.lane_id_, is_remote);
      EndTask(lane, exec, task, off);
    } else {
      off += 1;
    }
  }
}

void Worker::RunCoroutine(bctx::transfer_t t) {
  Task *task = reinterpret_cast<Task*>(t.data);
  RunContext &rctx = task->ctx_;
  TaskState *&exec = rctx.exec_;
  rctx.jmp_ = t;
  exec->Run(task->method_, task, rctx);
  task->UnsetStarted();
  if (task->IsLongRunning()) {
    rctx.flush_->pending_ -= 1;
  }
  task->Yield<TASK_YIELD_CO>();
}

void Worker::RunPreemptive(void *data) {
  Task *task = reinterpret_cast<Task *>(data);
  TaskState *exec = HRUN_TASK_REGISTRY->GetTaskState(task->task_state_);
  RunContext &real_rctx = task->ctx_;
  RunContext rctx(0);
  do {
    hshm::Timepoint now;
    now.Now();
    if (task->ShouldRun(now, real_rctx.flush_->flushing_)) {
      exec->Run(task->method_, task, rctx);
      task->DidRun(now);
    }
    if (task->IsLongRunning()) {
      real_rctx.flush_->pending_ -= 1;
    }
    task->Yield<TASK_YIELD_ABT>();
  } while(!task->IsModuleComplete());
}

}  // namespace hrun