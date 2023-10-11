//
// Created by lukemartinlogan on 6/29/23.
//

#include "labstor_admin/labstor_admin.h"
#include "labstor/api/labstor_runtime.h"
#include "op_min_max/op_min_max.h"

namespace labstor::op_min_max {

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
#include "op_min_max/op_min_max_lib_exec.h"
};

}  // namespace labstor::op_min_max

LABSTOR_TASK_CC(labstor::op_min_max::Server, "op_min_max");
