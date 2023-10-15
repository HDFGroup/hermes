//
// Created by lukemartinlogan on 6/29/23.
//

#include "hrun_admin/hrun_admin.h"
#include "hrun/api/hrun_runtime.h"
#include "small_message/small_message.h"

namespace hrun::small_message {

class Server : public TaskLib {
 public:
  int count_ = 0;

 public:
  void Construct(ConstructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }

  void Destruct(DestructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }

  void Md(MdTask *task, RunContext &rctx) {
    task->ret_[0] = 1;
    task->SetModuleComplete();
  }

  void MdPush(MdPushTask *task, RunContext &rctx) {
    task->ret_[0] = 1;
    task->SetModuleComplete();
  }

  void Io(IoTask *task, RunContext &rctx) {
    task->ret_ = 1;
    for (int i = 0; i < 256; ++i) {
      if (task->data_[i] != 10) {
        task->ret_ = 0;
        break;
      }
    }
    task->SetModuleComplete();
  }

 public:
#include "small_message/small_message_lib_exec.h"
};

}  // namespace hrun

HRUN_TASK_CC(hrun::small_message::Server, "small_message");
