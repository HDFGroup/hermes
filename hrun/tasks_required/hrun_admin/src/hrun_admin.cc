//
// Created by lukemartinlogan on 6/29/23.
//

#include "hrun_admin/hrun_admin.h"
#include "hrun/api/hrun_runtime.h"
#include "hrun/work_orchestrator/scheduler.h"

namespace hrun::Admin {

class Server : public TaskLib {
 public:
  Task *queue_sched_;
  Task *proc_sched_;

 public:
  Server() : queue_sched_(nullptr), proc_sched_(nullptr) {}

  void RegisterTaskLib(RegisterTaskLibTask *task, RunContext &rctx) {
    std::string lib_name = task->lib_name_->str();
    HRUN_TASK_REGISTRY->RegisterTaskLib(lib_name);
    task->SetModuleComplete();
  }

  void DestroyTaskLib(DestroyTaskLibTask *task, RunContext &rctx) {
    std::string lib_name = task->lib_name_->str();
    HRUN_TASK_REGISTRY->DestroyTaskLib(lib_name);
    task->SetModuleComplete();
  }

  void GetOrCreateTaskStateId(GetOrCreateTaskStateIdTask *task, RunContext &rctx) {
    std::string state_name = task->state_name_->str();
    task->id_ = HRUN_TASK_REGISTRY->GetOrCreateTaskStateId(state_name);
    task->SetModuleComplete();
  }

  void CreateTaskState(CreateTaskStateTask *task, RunContext &rctx) {
    std::string lib_name = task->lib_name_->str();
    std::string state_name = task->state_name_->str();
    // Check local registry for task state
    TaskState *task_state = HRUN_TASK_REGISTRY->GetTaskState(state_name, task->id_);
    if (task_state) {
      task->id_ = task_state->id_;
      task->SetModuleComplete();
      return;
    }
    // Check global registry for task state
    if (task->id_.IsNull()) {
      DomainId domain = DomainId::GetNode(1);
      LPointer<GetOrCreateTaskStateIdTask> get_id =
          HRUN_ADMIN->AsyncGetOrCreateTaskStateId(task->task_node_ + 1, domain, state_name);
      get_id->Wait<TASK_YIELD_CO>(task);
      task->id_ = get_id->id_;
      HRUN_CLIENT->DelTask(get_id);
    }
    // Create the task state
    HILOG(kInfo, "(node {}) Creating task state {} with id {} (task_node={})",
          HRUN_CLIENT->node_id_, state_name, task->id_, task->task_node_);
    if (task->id_.IsNull()) {
      HELOG(kError, "(node {}) The task state {} with id {} is NULL.",
            HRUN_CLIENT->node_id_, state_name, task->id_);
      task->SetModuleComplete();
      return;
    }
    // Verify the state doesn't exist
    if (HRUN_TASK_REGISTRY->TaskStateExists(task->id_)) {
      HILOG(kInfo, "(node {}) The task state {} with id {} exists",
            HRUN_CLIENT->node_id_, state_name, task->id_);
      task->SetModuleComplete();
      return;
    }
    // Create the task queue for the state
    QueueId qid(task->id_);
    MultiQueue *queue = HRUN_QM_RUNTIME->CreateQueue(
        qid, task->queue_info_->vec());
    // Run the task state's submethod
    task->method_ = Method::kConstruct;
    bool ret = HRUN_TASK_REGISTRY->CreateTaskState(
        lib_name.c_str(),
        state_name.c_str(),
        task->id_,
        task);
    queue->flags_.SetBits(QUEUE_READY);
    task->SetModuleComplete();
    HILOG(kInfo, "(node {}) Allocated task state {} with id {}",
          HRUN_CLIENT->node_id_, state_name, task->task_state_);
  }

  void GetTaskStateId(GetTaskStateIdTask *task, RunContext &rctx) {
    std::string state_name = task->state_name_->str();
    task->id_ = HRUN_TASK_REGISTRY->GetTaskStateId(state_name);
    task->SetModuleComplete();
  }

  void DestroyTaskState(DestroyTaskStateTask *task, RunContext &rctx) {
    HRUN_TASK_REGISTRY->DestroyTaskState(task->id_);
    task->SetModuleComplete();
  }

  void StopRuntime(StopRuntimeTask *task, RunContext &rctx) {
    HILOG(kInfo, "Stopping (server mode)");
    HRUN_WORK_ORCHESTRATOR->FinalizeRuntime();
    HRUN_THALLIUM->StopThisDaemon();
    task->SetModuleComplete();
  }

  void SetWorkOrchQueuePolicy(SetWorkOrchQueuePolicyTask *task, RunContext &rctx) {
    if (queue_sched_) {
      queue_sched_->SetModuleComplete();
    }
    if (queue_sched_ && !queue_sched_->IsComplete()) {
      return;
    }
    auto queue_sched = HRUN_CLIENT->NewTask<ScheduleTask>(
        task->task_node_, DomainId::GetLocal(), task->policy_id_);
    queue_sched_ = queue_sched.ptr_;
    MultiQueue *queue = HRUN_CLIENT->GetQueue(queue_id_);
    queue->Emplace(0, 0, queue_sched.shm_);
    task->SetModuleComplete();
  }

  void SetWorkOrchProcPolicy(SetWorkOrchProcPolicyTask *task, RunContext &rctx) {
    if (proc_sched_) {
      proc_sched_->SetModuleComplete();
    }
    if (proc_sched_ && !proc_sched_->IsComplete()) {
      return;
    }
    auto proc_sched = HRUN_CLIENT->NewTask<ScheduleTask>(
        task->task_node_, DomainId::GetLocal(), task->policy_id_);
    proc_sched_ = proc_sched.ptr_;
    MultiQueue *queue = HRUN_CLIENT->GetQueue(queue_id_);
    queue->Emplace(0, 0, proc_sched.shm_);
    task->SetModuleComplete();
  }

  void Flush(FlushTask *task, RunContext &rctx) {
    HILOG(kDebug, "Beginning to flush runtime");
    for (Worker &worker : HRUN_WORK_ORCHESTRATOR->workers_) {
      worker.flush_.flushing_ = true;
    }
    while (true) {
      int count = 0;
      for (Worker &worker : HRUN_WORK_ORCHESTRATOR->workers_) {
        if (worker.flush_.flushing_) {
          count += 1;
          break;
        }
      }
      if (count) {
        task->Yield<TASK_YIELD_CO>();
      } else {
        break;
      }
    }
    HILOG(kDebug, "Flushing completed");
    task->SetModuleComplete();
  }

 public:
#include "hrun_admin/hrun_admin_lib_exec.h"
};

}  // namespace hrun

HRUN_TASK_CC(hrun::Admin::Server, "hrun_admin");
