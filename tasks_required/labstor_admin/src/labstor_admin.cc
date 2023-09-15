//
// Created by lukemartinlogan on 6/29/23.
//

#include "labstor_admin/labstor_admin.h"
#include "labstor/api/labstor_runtime.h"
#include "labstor/work_orchestrator/scheduler.h"

namespace labstor::Admin {

class Server : public TaskLib {
 public:
  Task *queue_sched_;
  Task *proc_sched_;

 public:
  Server() : queue_sched_(nullptr), proc_sched_(nullptr) {}

  void RegisterTaskLib(RegisterTaskLibTask *task) {
    std::string lib_name = task->lib_name_->str();
    LABSTOR_TASK_REGISTRY->RegisterTaskLib(lib_name);
    task->SetModuleComplete();
  }

  void DestroyTaskLib(DestroyTaskLibTask *task) {
    std::string lib_name = task->lib_name_->str();
    LABSTOR_TASK_REGISTRY->DestroyTaskLib(lib_name);
    task->SetModuleComplete();
  }

  void GetOrCreateTaskStateId(GetOrCreateTaskStateIdTask *task) {
    std::string state_name = task->state_name_->str();
    task->id_ = LABSTOR_TASK_REGISTRY->GetOrCreateTaskStateId(state_name);
    task->SetModuleComplete();
  }

  void CreateTaskState(CreateTaskStateTask *task) {
    switch (task->phase_) {
      case CreateTaskStatePhase::kIdAllocStart: {
        std::string lib_name = task->lib_name_->str();
        std::string state_name = task->state_name_->str();
        // Check local registry for task state
        TaskState *task_state = LABSTOR_TASK_REGISTRY->GetTaskState(state_name, task->id_);
        if (task_state) {
          task->id_ = task_state->id_;
          task->SetModuleComplete();
          return;
        }
        // Check global registry for task state
        if (task->id_.IsNull()) {
//          if (task->domain_id_ == DomainId::GetLocal()) {
//            HILOG(kDebug, "Domain ID is local for {} (task_node={})", state_name, task->task_node_);
//            task->id_ = LABSTOR_TASK_REGISTRY->GetOrCreateTaskStateId(state_name);
//            task->phase_ = CreateTaskStatePhase::kStateCreate;
//          } else {
            HILOG(kDebug, "Domain ID is global for {} (task_node={})", state_name, task->task_node_);
            DomainId domain = DomainId::GetNode(1);
            task->get_id_task_ = LABSTOR_ADMIN->AsyncGetOrCreateTaskStateId(
                task->task_node_ + 1, domain, state_name).ptr_;
            task->phase_ = CreateTaskStatePhase::kIdAllocWait;
//          }
        } else {
          HILOG(kDebug, "Domain ID is given as {} for {} (task_node={})", task->id_, state_name, task->task_node_);
          task->phase_ = CreateTaskStatePhase::kStateCreate;
        }
        return;
      }
      case CreateTaskStatePhase::kIdAllocWait: {
        if (!task->get_id_task_->IsComplete()) {
          return;
        }
        task->id_ = task->get_id_task_->id_;
        task->phase_ = CreateTaskStatePhase::kStateCreate;
        LABSTOR_CLIENT->DelTask(task->get_id_task_);
      }
      case CreateTaskStatePhase::kStateCreate: {
        std::string lib_name = task->lib_name_->str();
        std::string state_name = task->state_name_->str();
        HILOG(kInfo, "(node {}) Creating task state {} with id {} (task_node={})",
              LABSTOR_CLIENT->node_id_, state_name, task->id_, task->task_node_);

        // Verify the state isn't NULL
        if (task->id_.IsNull()) {
          HELOG(kError, "(node {}) The task state {} with id {} is NULL.",
                LABSTOR_CLIENT->node_id_, state_name, task->id_);
          task->SetModuleComplete();
          return;
        }

        // Verify the state doesn't exist
        if (LABSTOR_TASK_REGISTRY->TaskStateExists(task->id_)) {
          HILOG(kInfo, "(node {}) The task state {} with id {} exists",
                LABSTOR_CLIENT->node_id_, state_name, task->id_);
          task->SetModuleComplete();
          return;
        }

        // The state is being created
        // NOTE(llogan): this does NOT return since task creations can have phases
        task->method_ = Method::kConstruct;

        // Create the task queue for the state
        QueueId qid(task->id_);
        LABSTOR_QM_RUNTIME->CreateQueue(
            qid, task->queue_info_->vec());

        // Begin creating the task state
        task->phase_ = 0;
        task->task_state_ = task->id_;
        bool ret = LABSTOR_TASK_REGISTRY->CreateTaskState(
            lib_name.c_str(),
            state_name.c_str(),
            task->id_,
            task);
        if (!ret) {
          task->SetModuleComplete();
          return;
        }
        HILOG(kInfo, "(node {}) Allocated task state {} with id {}",
              LABSTOR_CLIENT->node_id_, state_name, task->task_state_);
      }
    }
  }

  void GetTaskStateId(GetTaskStateIdTask *task) {
    std::string state_name = task->state_name_->str();
    task->id_ = LABSTOR_TASK_REGISTRY->GetTaskStateId(state_name);
    task->SetModuleComplete();
  }

  void DestroyTaskState(DestroyTaskStateTask *task) {
    LABSTOR_TASK_REGISTRY->DestroyTaskState(task->id_);
    task->SetModuleComplete();
  }

  void StopRuntime(StopRuntimeTask *task) {
    HILOG(kInfo, "Stopping (server mode)");
    LABSTOR_WORK_ORCHESTRATOR->FinalizeRuntime();
    LABSTOR_THALLIUM->StopThisDaemon();
    task->SetModuleComplete();
  }

  void SetWorkOrchQueuePolicy(SetWorkOrchQueuePolicyTask *task) {
    if (queue_sched_) {
      queue_sched_->SetModuleComplete();
    }
    if (queue_sched_ && !queue_sched_->IsComplete()) {
      return;
    }
    auto queue_sched = LABSTOR_CLIENT->NewTask<ScheduleTask>(
        task->task_node_, DomainId::GetLocal(), task->policy_id_);
    queue_sched_ = queue_sched.ptr_;
    MultiQueue *queue = LABSTOR_CLIENT->GetQueue(queue_id_);
    queue->Emplace(0, 0, queue_sched.shm_);
    task->SetModuleComplete();
  }

  void SetWorkOrchProcPolicy(SetWorkOrchProcPolicyTask *task) {
    if (proc_sched_) {
      proc_sched_->SetModuleComplete();
    }
    if (proc_sched_ && !proc_sched_->IsComplete()) {
      return;
    }
    auto proc_sched = LABSTOR_CLIENT->NewTask<ScheduleTask>(
        task->task_node_, DomainId::GetLocal(), task->policy_id_);
    proc_sched_ = proc_sched.ptr_;
    MultiQueue *queue = LABSTOR_CLIENT->GetQueue(queue_id_);
    queue->Emplace(0, 0, proc_sched.shm_);
    task->SetModuleComplete();
  }

 public:
#include "labstor_admin/labstor_admin_lib_exec.h"
};

}  // namespace labstor

LABSTOR_TASK_CC(labstor::Admin::Server, "labstor_admin");
