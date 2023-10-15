//
// Created by lukemartinlogan on 6/29/23.
//

#include "hrun_admin/hrun_admin.h"
#include "hrun/api/hrun_runtime.h"
#include "hermes_adapters/hermes_adapters.h"

namespace hrun::hermes_adapters {

class Server : public TaskLib {
 public:
  Server() = default;

  void Construct(ConstructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }

  void Destruct(DestructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }

  void Custom(CustomTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }

  void FileStageIn() {
  }

  void FileStageOut() {
  }

 public:
#include "hermes_adapters/hermes_adapters_lib_exec.h"
};

}  // namespace hrun::hermes_adapters

HRUN_TASK_CC(hrun::hermes_adapters::Server, "hermes_adapters");
