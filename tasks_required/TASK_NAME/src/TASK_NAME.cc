//
// Created by lukemartinlogan on 6/29/23.
//

#include "labstor_admin/labstor_admin.h"
#include "labstor/api/labstor_runtime.h"
#include "TASK_NAME/TASK_NAME.h"

namespace labstor::TASK_NAME {

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

 public:
#include "TASK_NAME/TASK_NAME_lib_exec.h"
};

}  // namespace labstor::TASK_NAME

LABSTOR_TASK_CC(labstor::TASK_NAME::Server, "TASK_NAME");
