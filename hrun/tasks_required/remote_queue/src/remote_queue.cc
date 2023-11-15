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
#include "remote_queue/remote_queue.h"

namespace thallium {

/** Serialize I/O type enum */
SERIALIZE_ENUM(hrun::IoType);

}  // namespace thallium

namespace hrun::remote_queue {

class Server;

struct AbtWorkerEntry {
  int id_;
  Server *server_;
  ABT_thread thread_;
  PushTask *task_;
  RunContext *rctx_;

  AbtWorkerEntry() = default;

  AbtWorkerEntry(int id, Server *server)
  : id_(id), server_(server), task_(nullptr), rctx_(nullptr) {}
};

class Server : public TaskLib {
 public:
  hipc::uptr<hipc::mpsc_queue<AbtWorkerEntry*>> threads_;

 public:
  Server() = default;

  /** Construct remote queue */
  void Construct(ConstructTask *task, RunContext &rctx) {
    HILOG(kInfo, "(node {}) Constructing remote queue (task_node={}, task_state={}, method={})",
          HRUN_CLIENT->node_id_, task->task_node_, task->task_state_, task->method_);
    threads_ = hipc::make_uptr<hipc::mpsc_queue<AbtWorkerEntry*>>(
        HRUN_RPC->num_threads_ + 16);
    for (int i = 0; i < HRUN_RPC->num_threads_; ++i) {
      AbtWorkerEntry *entry = new AbtWorkerEntry(i, this);
      entry->thread_ = HRUN_WORK_ORCHESTRATOR->SpawnAsyncThread(
          &Server::RunPreemptive, entry);
      threads_->emplace(entry);
    }
    HRUN_THALLIUM->RegisterRpc("RpcPushSmall", [this](const tl::request &req,
                                                         TaskStateId state_id,
                                                         u32 method,
                                                         std::string &params) {
      this->RpcPushSmall(req, state_id, method, params);
    });
    HRUN_THALLIUM->RegisterRpc("RpcPushBulk", [this](const tl::request &req,
                                                        const tl::bulk &bulk,
                                                        TaskStateId state_id,
                                                        u32 method,
                                                        std::string &params,
                                                        size_t data_size,
                                                        IoType io_type) {
      this->RpcPushBulk(req, state_id, method, params, bulk, data_size, io_type);
    });
    task->SetModuleComplete();
  }
  void MonitorConstruct(u32 mode, ConstructTask *task, RunContext &rctx) {
  }

  /** Destroy remote queue */
  void Destruct(DestructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorDestruct(u32 mode, DestructTask *task, RunContext &rctx) {
  }

  /** Push operation called on client */
  void Push(PushTask *task, RunContext &rctx) {
    AbtWorkerEntry *entry;
    if (!task->started_) {
//      task->server_ = this;
//      task->started_ = true;
//      HRUN_WORK_ORCHESTRATOR->SpawnAsyncThread(
//          &Server::RunOnce, task);
      if (threads_->pop(entry).IsNull()) {
        return;
      }
      HILOG(kDebug, "Starting task {} on {}",
            task->task_node_, entry->id_);
      task->started_ = true;
      entry->rctx_ = &rctx;
      entry->task_ = task;
    }
    // HILOG(kDebug, "Continuing task {}", task->task_node_)
  }
  void MonitorPush(u32 mode, PushTask *task, RunContext &rctx) {
  }

  /** Duplicate operations */
  void Dup(DupTask *task, RunContext &ctx) {
    Task *orig_task = task->orig_task_;
    if (!orig_task->IsFireAndForget()) {
      for (size_t i = 0; i < task->dups_.size(); ++i) {
        LPointer<Task> &dup = task->dups_[i];
        dup->Wait<TASK_YIELD_CO>(task);
        task->exec_->DupEnd(dup->method_, i, task->orig_task_, dup.ptr_);
        HRUN_CLIENT->DelTask(task->exec_, dup.ptr_);
      }
      task->exec_->ReplicateEnd(task->exec_method_, task->orig_task_);
      if (!orig_task->IsLongRunning()) {
        orig_task->SetModuleComplete();
      } else {
        orig_task->UnsetStarted();
      }
    }
    task->SetModuleComplete();
  }
  void MonitorDup(u32 mode, DupTask *task, RunContext &rctx) {
  }

 private:
  /** An ABT thread */
  static void RunOnce(void *data) {
    PushTask *task = (PushTask *) data;
    Server *server = (Server *) task->server_;
    server->PushPreemptive(task);
  }

  /** An ABT thread */
  static void RunPreemptive(void *data) {
    AbtWorkerEntry *entry = (AbtWorkerEntry *) data;
    Server *server = entry->server_;
    WorkOrchestrator *orchestrator = HRUN_WORK_ORCHESTRATOR;
    while (orchestrator->IsAlive()) {
      if (entry->task_) {
        HILOG(kDebug, "Preempt task {}", entry->task_->task_node_)
        server->PushPreemptive(entry->task_);
        entry->task_ = nullptr;
        server->threads_->emplace(entry);
      }
      ABT_thread_yield();
    }
  }

  /** PUSH using thallium */
  void PushPreemptive(PushTask *task) {
    std::vector<DataTransfer> &xfer = task->xfer_;
    switch (xfer.size()) {
      case 1: {
        HILOG(kDebug, "Pushing to small {} vs {}", task->task_node_, task->orig_task_->task_node_)
        SyncClientSmallPush(xfer, task);
        break;
      }
      case 2: {
        HILOG(kDebug, "Pushing to io {}", task->task_node_, task->orig_task_->task_node_)
        SyncClientIoPush(xfer, task);
        break;
      }
      default: {
        HELOG(kFatal, "The task {}/{} does not support remote calls", task->task_state_, task->method_);
      }
    }
    ClientHandlePushReplicaEnd(task);
  }

  /** Handle output from replica PUSH */
  static void ClientHandlePushReplicaOutput(int replica, std::string &ret, PushTask *task) {
    std::vector<DataTransfer> xfer(1);
    xfer[0].data_ = ret.data();
    xfer[0].data_size_ = ret.size();
    HILOG(kDebug, "Wait got {} bytes of data (task_node={}, task_state={}, method={})",
          xfer[0].data_size_,
          task->orig_task_->task_node_,
          task->orig_task_->task_state_,
          task->orig_task_->method_);
    BinaryInputArchive<false> ar(xfer);
    task->exec_->LoadEnd(replica, task->exec_method_, ar, task->orig_task_);
  }

  /** Handle finalization of PUSH replicate */
  static void ClientHandlePushReplicaEnd(PushTask *task) {
    task->exec_->ReplicateEnd(task->orig_task_->method_, task->orig_task_);
    HILOG(kDebug, "Completing task (task_node={}, task_state={}, method={})",
          task->orig_task_->task_node_,
          task->orig_task_->task_state_,
          task->orig_task_->method_);
    if (!task->orig_task_->IsLongRunning()) {
      task->orig_task_->SetModuleComplete();
    } else {
      task->orig_task_->UnsetStarted();
      task->orig_task_->UnsetDisableRun();
    }
    task->SetModuleComplete();
  }

  /** Sync Push for small message */
  void SyncClientSmallPush(std::vector<DataTransfer> &xfer, PushTask *task) {
    std::string params = std::string((char *) xfer[0].data_, xfer[0].data_size_);
    for (int replica = 0; replica < task->domain_ids_.size(); ++replica) {
      DomainId domain_id = task->domain_ids_[replica];
      HILOG(kDebug, "(SM) Transferring {} bytes of data (task_node={}, task_state={}, method={}, from={}, to={})",
            params.size(),
            task->orig_task_->task_node_,
            task->orig_task_->task_state_,
            task->orig_task_->method_,
            HRUN_CLIENT->node_id_,
            domain_id.id_);
      std::string ret = HRUN_THALLIUM->SyncCall<std::string>(domain_id.id_,
                                                             "RpcPushSmall",
                                                             task->exec_->id_,
                                                             task->exec_method_,
                                                             params);
      HILOG(kDebug, "(SM) Finished {} bytes of data (task_node={}, task_state={}, method={}, from={}, to={})",
            params.size(),
            task->orig_task_->task_node_,
            task->orig_task_->task_state_,
            task->orig_task_->method_,
            HRUN_CLIENT->node_id_,
            domain_id.id_);
      ClientHandlePushReplicaOutput(replica, ret, task);
    }
  }

  /** Sync Push for I/O message */
  void SyncClientIoPush(std::vector<DataTransfer> &xfer, PushTask *task) {
    std::string params = std::string((char *) xfer[1].data_, xfer[1].data_size_);
    IoType io_type = IoType::kRead;
    if (xfer[0].flags_.Any(DT_RECEIVER_READ)) {
      io_type = IoType::kWrite;
    }
    for (int replica = 0; replica < task->domain_ids_.size(); ++replica) {
      DomainId domain_id = task->domain_ids_[replica];
      char *data = (char*)xfer[0].data_;
      size_t data_size = xfer[0].data_size_;
      HILOG(kDebug, "(IO) Transferring {} bytes of data (task_node={}, task_state={}, method={}, from={}, to={}, type={})",
            data_size,
            task->orig_task_->task_node_,
            task->orig_task_->task_state_,
            task->orig_task_->method_,
            HRUN_CLIENT->node_id_,
            domain_id.id_,
            static_cast<int>(io_type));
      std::string ret;
      if (data_size > 0) {
        ret = HRUN_THALLIUM->SyncIoCall<std::string>(domain_id.id_,
                                                     "RpcPushBulk",
                                                     io_type,
                                                     data,
                                                     data_size,
                                                     task->exec_->id_,
                                                     task->exec_method_,
                                                     params,
                                                     data_size,
                                                     io_type);
      } else {
        HILOG(kFatal, "(IO) Thallium can't handle 0-sized I/O")
      }
      HILOG(kDebug, "(IO) Finished transferring {} bytes of data (task_node={}, task_state={}, method={}, from={}, to={}, type={})",
            data_size,
            task->orig_task_->task_node_,
            task->orig_task_->task_state_,
            task->orig_task_->method_,
            HRUN_CLIENT->node_id_,
            domain_id.id_,
            static_cast<int>(io_type));
      ClientHandlePushReplicaOutput(replica, ret, task);
    }
  }

  /** Push for small message */
  void AsyncClientSmallPush(std::vector<DataTransfer> &xfer, PushTask *task) {
    std::string params = std::string((char *) xfer[0].data_, xfer[0].data_size_);
    std::vector<thallium::async_response> waiters;
    for (int replica = 0; replica < task->domain_ids_.size(); ++replica) {
      DomainId domain_id = task->domain_ids_[replica];
      HILOG(kDebug, "(SM) Transferring {} bytes of data (task_node={}, task_state={}, method={}, from={}, to={})",
            params.size(),
            task->orig_task_->task_node_,
            task->orig_task_->task_state_,
            task->orig_task_->method_,
            HRUN_CLIENT->node_id_,
            domain_id.id_);
      thallium::async_response async =
          HRUN_THALLIUM->AsyncCall(domain_id.id_,
                                   "RpcPushSmall",
                                   task->exec_->id_,
                                   task->exec_method_,
                                   params);
      HILOG(kDebug, "(SM) Submitted {} bytes of data (task_node={}, task_state={}, method={}, from={}, to={})",
            params.size(),
            task->orig_task_->task_node_,
            task->orig_task_->task_state_,
            task->orig_task_->method_,
            HRUN_CLIENT->node_id_,
            domain_id.id_);
      std::string ret = HRUN_THALLIUM->Wait<std::string>(async);
      ClientHandlePushReplicaOutput(replica, ret, task);
    }
  }

  /** Push for I/O message */
  void AsyncClientIoPush(std::vector<DataTransfer> &xfer, PushTask *task) {
    std::string params = std::string((char *) xfer[1].data_, xfer[1].data_size_);
    IoType io_type = IoType::kRead;
    if (xfer[0].flags_.Any(DT_RECEIVER_READ)) {
      io_type = IoType::kWrite;
    }
    for (int replica = 0; replica < task->domain_ids_.size(); ++replica) {
      DomainId domain_id = task->domain_ids_[replica];
      char *data = (char*)xfer[0].data_;
      size_t data_size = xfer[0].data_size_;
      HILOG(kDebug, "(IO) Transferring {} bytes of data (task_node={}, task_state={}, method={}, from={}, to={}, type={})",
            data_size,
            task->orig_task_->task_node_,
            task->orig_task_->task_state_,
            task->orig_task_->method_,
            HRUN_CLIENT->node_id_,
            domain_id.id_,
            static_cast<int>(io_type));
      thallium::async_response async =
          HRUN_THALLIUM->AsyncIoCall(domain_id.id_,
                                     "RpcPushBulk",
                                     io_type,
                                     data,
                                     data_size,
                                     task->exec_->id_,
                                     task->exec_method_,
                                     params,
                                     data_size,
                                     io_type);
      HILOG(kDebug, "(IO) Submitted transfer {} bytes of data (task_node={}, task_state={}, method={}, from={}, to={}, type={})",
            data_size,
            task->orig_task_->task_node_,
            task->orig_task_->task_state_,
            task->orig_task_->method_,
            HRUN_CLIENT->node_id_,
            domain_id.id_,
            static_cast<int>(io_type));
      std::string ret = HRUN_THALLIUM->Wait<std::string>(async);
      ClientHandlePushReplicaOutput(replica, ret, task);
    }
  }

  /** The RPC for processing a small message */
  void RpcPushSmall(const tl::request &req,
                    TaskStateId state_id,
                    u32 method,
                    std::string &params) {
    // Create the input data transfer object
    std::vector<DataTransfer> xfer(1);
    xfer[0].data_ = params.data();
    xfer[0].data_size_ = params.size();
    HILOG(kDebug, "Received small message of size {} "
                  "(task_state={}, method={})", xfer[0].data_size_, state_id, method);

    // Process the message
    TaskState *exec;
    Task *orig_task;
    RpcExec(req, state_id, method, params, xfer, orig_task, exec);
    RpcComplete(req, method, orig_task, exec, state_id);
  }

  /** The RPC for processing a message with data */
  void RpcPushBulk(const tl::request &req,
                   TaskStateId state_id,
                   u32 method,
                   std::string &params,
                   const tl::bulk &bulk,
                   size_t data_size,
                   IoType io_type) {
    LPointer<char> data =
        HRUN_CLIENT->AllocateBufferServer<TASK_YIELD_ABT>(data_size);

    // Create the input data transfer object
    std::vector<DataTransfer> xfer(2);
    xfer[0].data_ = data.ptr_;
    xfer[0].data_size_ = data_size;
    xfer[1].data_ = params.data();
    xfer[1].data_size_ = params.size();

    HILOG(kDebug, "Received large message of size {} "
                  "(task_state={}, method={})", xfer[0].data_size_, state_id, method);

    // Process the message
    if (io_type == IoType::kWrite) {
      HRUN_THALLIUM->IoCallServer(req, bulk, io_type, data.ptr_, data_size);
    }
    TaskState *exec;
    Task *orig_task;
    RpcExec(req, state_id, method, params, xfer, orig_task, exec);
    if (io_type == IoType::kRead) {
      HRUN_THALLIUM->IoCallServer(req, bulk, io_type, data.ptr_, data_size);
    }
    HRUN_CLIENT->FreeBuffer(data);

    // Return
    RpcComplete(req, method, orig_task, exec, state_id);
  }

  /** Push operation called at the remote server */
  void RpcExec(const tl::request &req,
               const TaskStateId &state_id,
               u32 method,
               std::string &params,
               std::vector<DataTransfer> &xfer,
               Task *&orig_task, TaskState *&exec) {
    size_t data_size = xfer[0].data_size_;
    BinaryInputArchive<true> ar(xfer);

    // Deserialize task
    exec = HRUN_TASK_REGISTRY->GetTaskState(state_id);
    if (exec == nullptr) {
      HELOG(kFatal, "(node {}) Could not find the task state {}",
            HRUN_CLIENT->node_id_, state_id);
      req.respond(std::string());
      return;
    } else {
      HILOG(kDebug, "(node {}) Found task state {}",
            HRUN_CLIENT->node_id_,
            state_id);
    }
    TaskPointer task_ptr = exec->LoadStart(method, ar);
    orig_task = task_ptr.ptr_;
    orig_task->domain_id_ = DomainId::GetNode(HRUN_CLIENT->node_id_);

    // Unset task flags
    // NOTE(llogan): Remote tasks are executed to completion and
    // return values sent back to the remote host. This is
    // for things like long-running monitoring tasks.
    orig_task->UnsetFireAndForget();
    orig_task->UnsetStarted();
    orig_task->UnsetDataOwner();
    orig_task->UnsetLongRunning();

    // Execute task
    MultiQueue *queue = HRUN_CLIENT->GetQueue(QueueId(state_id));
    queue->Emplace(orig_task->prio_, orig_task->lane_hash_, task_ptr.shm_);
    HILOG(kDebug,
          "(node {}) Executing task (task_node={}, task_state={}/{}, state_name={}, method={}, size={}, lane_hash={})",
          HRUN_CLIENT->node_id_,
          orig_task->task_node_,
          orig_task->task_state_,
          state_id,
          exec->name_,
          method,
          data_size,
          orig_task->lane_hash_);
    orig_task->Wait<TASK_YIELD_ABT>();
  }

  void RpcComplete(const tl::request &req,
                   u32 method, Task *orig_task,
                   TaskState *exec, TaskStateId state_id) {
    BinaryOutputArchive<false> ar(DomainId::GetNode(HRUN_CLIENT->node_id_));
    std::vector<DataTransfer> out_xfer = exec->SaveEnd(method, ar, orig_task);
    HILOG(kDebug, "(node {}) Returning {} bytes of data (task_node={}, task_state={}/{}, method={})",
          HRUN_CLIENT->node_id_,
          out_xfer[0].data_size_,
          orig_task->task_node_,
          orig_task->task_state_,
          state_id,
          method);

    HRUN_CLIENT->DelTask(exec, orig_task);
    if (out_xfer.size() > 0 && out_xfer[0].data_size_ > 0) {
      req.respond(std::string((char *) out_xfer[0].data_, out_xfer[0].data_size_));
    } else {
      req.respond(std::string());
    }
  }

 public:
#include "remote_queue/remote_queue_lib_exec.h"
};
}  // namespace hrun

HRUN_TASK_CC(hrun::remote_queue::Server, "remote_queue");
