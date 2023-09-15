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

  void Construct(ConstructTask *task) {
    task->SetModuleComplete();
  }

  void Destruct(DestructTask *task) {
    task->SetModuleComplete();
  }

  void Custom(CustomTask *task) {
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
