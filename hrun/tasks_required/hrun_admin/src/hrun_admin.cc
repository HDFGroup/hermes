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
#include "hrun/work_orchestrator/scheduler.h"

namespace hrun::Admin {

class Server : public TaskLib {
 public:
  Task *queue_sched_;
  Task *proc_sched_;

 public:
  Server() : queue_sched_(nullptr), proc_sched_(nullptr) {}

  /** Register a task library dynamically */
  void RegisterTaskLib(RegisterTaskLibTask *task, RunContext &rctx) {
    std::string lib_name = task->lib_name_->str();
    HRUN_TASK_REGISTRY->RegisterTaskLib(lib_name);
    task->SetModuleComplete();
  }
  void MonitorRegisterTaskLib(u32 mode, RegisterTaskLibTask *task, RunContext &rctx) {
  }

  /** Destroy a task library */
  void DestroyTaskLib(DestroyTaskLibTask *task, RunContext &rctx) {
    std::string lib_name = task->lib_name_->str();
    HRUN_TASK_REGISTRY->DestroyTaskLib(lib_name);
    task->SetModuleComplete();
  }
  void MonitorDestroyTaskLib(u32 mode, DestroyTaskLibTask *task, RunContext &rctx) {
  }

  /** Get a task state ID if it exists, and create otherwise */
  void GetOrCreateTaskStateId(GetOrCreateTaskStateIdTask *task, RunContext &rctx) {
    std::string state_name = task->state_name_->str();
    task->id_ = HRUN_TASK_REGISTRY->GetOrCreateTaskStateId(state_name);
    task->SetModuleComplete();
  }
  void MonitorGetOrCreateTaskStateId(u32 mode, GetOrCreateTaskStateIdTask *task, RunContext &rctx) {
  }

  /** Create a task state */
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
    // Allocate the task state
    task->method_ = Method::kConstruct;
    HRUN_TASK_REGISTRY->CreateTaskState(
        lib_name.c_str(),
        state_name.c_str(),
        task->id_,
        task);
    queue->flags_.SetBits(QUEUE_READY);
    task->method_ = Method::kCreateTaskState;
    task->SetModuleComplete();
  }
  void MonitorCreateTaskState(u32 mode, CreateTaskStateTask *task, RunContext &rctx) {
  }

  /** Get task state id, fail if DNE */
  void GetTaskStateId(GetTaskStateIdTask *task, RunContext &rctx) {
    std::string state_name = task->state_name_->str();
    task->id_ = HRUN_TASK_REGISTRY->GetTaskStateId(state_name);
    task->SetModuleComplete();
  }
  void MonitorGetTaskStateId(u32 mode, GetTaskStateIdTask *task, RunContext &rctx) {
  }

  /** Destroy a task state */
  void DestroyTaskState(DestroyTaskStateTask *task, RunContext &rctx) {
    HRUN_TASK_REGISTRY->DestroyTaskState(task->id_);
    task->SetModuleComplete();
  }
  void MonitorDestroyTaskState(u32 mode, DestroyTaskStateTask *task, RunContext &rctx) {
  }

  /** Stop this runtime */
  void StopRuntime(StopRuntimeTask *task, RunContext &rctx) {
    HILOG(kInfo, "Stopping (server mode)");
    HRUN_WORK_ORCHESTRATOR->FinalizeRuntime();
    HRUN_THALLIUM->StopThisDaemon();
    task->SetModuleComplete();
  }
  void MonitorStopRuntime(u32 mode, StopRuntimeTask *task, RunContext &rctx) {
  }

  /** Set work orchestrator policy */
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
  void MonitorSetWorkOrchQueuePolicy(u32 mode, SetWorkOrchQueuePolicyTask *task, RunContext &rctx) {
  }

  /** Set work orchestration policy */
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
  void MonitorSetWorkOrchProcPolicy(u32 mode, SetWorkOrchProcPolicyTask *task, RunContext &rctx) {
  }

  /** Flush the runtime */
  void Flush(FlushTask *task, RunContext &rctx) {
    HILOG(kDebug, "Beginning to flush runtime");
    while (true) {
      // Make all workers flush locally
      int count = 0;
      for (int i = 0; i < 2; ++i) {
        for (std::unique_ptr<Worker>
              &worker : HRUN_WORK_ORCHESTRATOR->workers_) {
          worker->flush_.count_ = 0;
          worker->flush_.flushing_ = true;
          while (worker->flush_.flushing_) {
            task->Yield<TASK_YIELD_CO>();
          }
          count += worker->flush_.count_;
        }
      }
      if (!count) {
        break;
      }
    }
    HILOG(kDebug, "Flushing completed");
    task->SetModuleComplete();
  }
  void MonitorFlush(u32 mode, FlushTask *task, RunContext &rctx) {
  }

 public:
#include "hrun_admin/hrun_admin_lib_exec.h"
};

}  // namespace hrun

HRUN_TASK_CC(hrun::Admin::Server, "hrun_admin");
