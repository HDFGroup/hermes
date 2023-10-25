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
#include "TASK_NAME/TASK_NAME.h"

namespace hrun::TASK_NAME {

class Server : public TaskLib {
 public:
  Server() = default;

  /** Construct TASK_NAME */
  void Construct(ConstructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorConstruct(u32 mode, ConstructTask *task, RunContext &rctx) {
  }

  /** Destroy TASK_NAME */
  void Destruct(DestructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorDestruct(u32 mode, DestructTask *task, RunContext &rctx) {
  }

  /** A custom method */
  void Custom(CustomTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorCustom(u32 mode, CustomTask *task, RunContext &rctx) {
  }
 public:
#include "TASK_NAME/TASK_NAME_lib_exec.h"
};

}  // namespace hrun::TASK_NAME

HRUN_TASK_CC(hrun::TASK_NAME::Server, "TASK_NAME");
