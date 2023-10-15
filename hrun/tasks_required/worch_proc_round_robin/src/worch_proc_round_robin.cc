//
// Created by lukemartinlogan on 6/29/23.
//

#include "hrun_admin/hrun_admin.h"
#include "hrun/api/hrun_runtime.h"
#include "worch_proc_round_robin/worch_proc_round_robin.h"

namespace hrun::worch_proc_round_robin {

class Server : public TaskLib {
 public:
  void Construct(ConstructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }

  void Destruct(DestructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }

  void Schedule(ScheduleTask *task, RunContext &rctx) {
    int rr = 0;
    for (Worker &worker : HRUN_WORK_ORCHESTRATOR->workers_) {
      worker.SetCpuAffinity(rr % HERMES_SYSTEM_INFO->ncpu_);
      ++rr;
    }
  }

#include "worch_proc_round_robin/worch_proc_round_robin_lib_exec.h"
};

}  // namespace hrun

HRUN_TASK_CC(hrun::worch_proc_round_robin::Server, "worch_proc_round_robin");
