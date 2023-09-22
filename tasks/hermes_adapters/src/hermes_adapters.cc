//
// Created by lukemartinlogan on 6/29/23.
//

#include "labstor_admin/labstor_admin.h"
#include "labstor/api/labstor_runtime.h"
#include "hermes_adapters/hermes_adapters.h"

namespace labstor::hermes_adapters {

class Server : public TaskLib {
 public:
  Server() = default;

  void Construct(ConstructTask *task, RunContext &ctx) {
    task->SetModuleComplete();
  }

  void Destruct(DestructTask *task, RunContext &ctx) {
    task->SetModuleComplete();
  }

  void Custom(CustomTask *task, RunContext &ctx) {
    task->SetModuleComplete();
  }

  void FileStageIn() {
  }

  void FileStageOut() {
  }

 public:
#include "hermes_adapters/hermes_adapters_lib_exec.h"
};

}  // namespace labstor::hermes_adapters

LABSTOR_TASK_CC(labstor::hermes_adapters::Server, "hermes_adapters");
