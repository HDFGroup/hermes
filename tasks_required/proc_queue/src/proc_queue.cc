//
// Created by lukemartinlogan on 6/29/23.
//

#include "labstor_admin/labstor_admin.h"
#include "labstor/api/labstor_runtime.h"
#include "proc_queue/proc_queue.h"

namespace labstor::proc_queue {

class Server : public TaskLib {
 public:
  Server() = default;

  void Construct(ConstructTask *task) {
    task->SetModuleComplete();
  }

  void Destruct(DestructTask *task) {
    task->SetModuleComplete();
  }

  void Push(PushTask *task) {
    switch (task->phase_) {
      case PushTaskPhase::kSchedule: {
        task->ptr_ = LABSTOR_CLIENT->GetPrivatePointer<Task>(task->subtask_);
        HILOG(kDebug, "Scheduling task {} on state {} tid {}",
              task->ptr_->task_node_, task->ptr_->task_state_, gettid());
        if (task->ptr_->IsFireAndForget()) {
          task->ptr_->UnsetFireAndForget();
        }
        MultiQueue *real_queue = LABSTOR_CLIENT->GetQueue(QueueId(task->ptr_->task_state_));
        real_queue->Emplace(task->ptr_->prio_, task->ptr_->lane_hash_, task->subtask_);
        task->phase_ = PushTaskPhase::kWaitSchedule;
      }
      case PushTaskPhase::kWaitSchedule: {
        if (!task->ptr_->IsComplete()) {
          return;
        }
        LABSTOR_CLIENT->DelTask(task->ptr_);
        task->SetModuleComplete();
      }
    }
  }

 public:
#include "proc_queue/proc_queue_lib_exec.h"
};

}  // namespace labstor::proc_queue

LABSTOR_TASK_CC(labstor::proc_queue::Server, "proc_queue");
