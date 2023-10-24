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
#include "worch_proc_round_robin/worch_proc_round_robin.h"

namespace hrun::worch_proc_round_robin {

class Server : public TaskLib {
 public:
  /** Construct the work orchestrator process scheduler */
  void Construct(ConstructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorConstruct(u32 mode, ConstructTask *task, RunContext &rctx) {
  }

  /** Destroy the work orchestrator process queue */
  void Destruct(DestructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorDestruct(u32 mode, DestructTask *task, RunContext &rctx) {
  }

  /** Schedule running processes */
  void Schedule(ScheduleTask *task, RunContext &rctx) {
    int rr = 0;
    for (Worker &worker : HRUN_WORK_ORCHESTRATOR->workers_) {
      worker.SetCpuAffinity(rr % HERMES_SYSTEM_INFO->ncpu_);
      ++rr;
    }
  }
  void MonitorSchedule(u32 mode, ScheduleTask *task, RunContext &rctx) {
  }

#include "worch_proc_round_robin/worch_proc_round_robin_lib_exec.h"
};

}  // namespace hrun

HRUN_TASK_CC(hrun::worch_proc_round_robin::Server, "worch_proc_round_robin");
