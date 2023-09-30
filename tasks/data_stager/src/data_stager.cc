//
// Created by lukemartinlogan on 6/29/23.
//

#include "labstor_admin/labstor_admin.h"
#include "labstor/api/labstor_runtime.h"
#include "data_stager/data_stager.h"

namespace labstor::data_stager {

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

 public:
#include "data_stager/data_stager_lib_exec.h"
};

}  // namespace labstor::data_stager

LABSTOR_TASK_CC(labstor::data_stager::Server, "data_stager");
