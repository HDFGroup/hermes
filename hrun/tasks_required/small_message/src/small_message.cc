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
#include "small_message/small_message.h"

namespace hrun::small_message {

class Server : public TaskLib {
 public:
  int count_ = 0;

 public:
  /** Construct small_message */
  void Construct(ConstructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorConstruct(u32 mode, ConstructTask *task, RunContext &rctx) {
  }

  /** Destroy small_message */
  void Destruct(DestructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorDestruct(u32 mode, DestructTask *task, RunContext &rctx) {
  }

  /** A metadata operation */
  void Md(MdTask *task, RunContext &rctx) {
    task->ret_[0] = 1;
    task->SetModuleComplete();
  }
  void MonitorMd(u32 mode, MdTask *task, RunContext &rctx) {
  }

  /** A metadata operation + push task */
  void MdPush(MdPushTask *task, RunContext &rctx) {
    task->ret_[0] = 1;
    task->SetModuleComplete();
  }
  void MonitorMdPush(u32 mode, MdPushTask *task, RunContext &rctx) {
  }

  /** An I/O task */
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
  void MonitorIo(u32 mode, IoTask *task, RunContext &rctx) {
  }

 public:
#include "small_message/small_message_lib_exec.h"
};

}  // namespace hrun

HRUN_TASK_CC(hrun::small_message::Server, "small_message");
