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
#include "proc_queue/proc_queue.h"

namespace hrun::proc_queue {

class Server : public TaskLib {
 public:
  Server() = default;

  /** Construct proc queue */
  void Construct(ConstructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorConstruct(u32 mode, ConstructTask *task, RunContext &rctx) {
  }

  /** Destroy proc queue */
  void Destruct(DestructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorDestruct(u32 mode, DestructTask *task, RunContext &rctx) {
  }

  /** Push a task onto the process queue */
  void Push(PushTask *task, RunContext &rctx) {
    switch (task->phase_) {
      case PushTaskPhase::kSchedule: {
        task->sub_run_.shm_ = task->sub_cli_.shm_;
        task->sub_run_.ptr_ = HRUN_CLIENT->GetMainPointer<Task>(task->sub_cli_.shm_);
        Task *&ptr = task->sub_run_.ptr_;
//        HILOG(kDebug, "Scheduling task {} on state {} tid {}",
//              ptr->task_node_, ptr->task_state_, GetLinuxTid());
        if (ptr->IsFireAndForget()) {
          ptr->UnsetFireAndForget();
          task->is_fire_forget_ = true;
        }
        MultiQueue *real_queue = HRUN_CLIENT->GetQueue(QueueId(ptr->task_state_));
        task->SetModuleComplete();
        return;
        real_queue->Emplace(ptr->prio_, ptr->lane_hash_, task->sub_run_.shm_);
        task->phase_ = PushTaskPhase::kWaitSchedule;
      }
      case PushTaskPhase::kWaitSchedule: {
        Task *&ptr = task->sub_run_.ptr_;
        if (!ptr->IsComplete()) {
          return;
        }
        if (task->is_fire_forget_) {
          TaskState *exec = HRUN_TASK_REGISTRY->GetTaskState(ptr->task_state_);
          exec->Del(ptr->method_, ptr);
        }
        task->SetModuleComplete();
      }
    }
  }
  void MonitorPush(u32 mode, PushTask *task, RunContext &rctx) {
  }

 public:
#include "proc_queue/proc_queue_lib_exec.h"
};

}  // namespace hrun::proc_queue

HRUN_TASK_CC(hrun::proc_queue::Server, "proc_queue");
