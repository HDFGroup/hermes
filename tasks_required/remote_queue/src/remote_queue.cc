//
// Created by lukemartinlogan on 6/29/23.
//

#include "labstor_admin/labstor_admin.h"
#include "labstor/api/labstor_runtime.h"
#include "remote_queue/remote_queue.h"

namespace thallium {

/** Serialize I/O type enum */
SERIALIZE_ENUM(labstor::IoType);

}  // namespace thallium

namespace labstor::remote_queue {

class Server : public TaskLib {
 public:
  labstor::remote_queue::Client client_;

 public:
  Server() = default;

  /** Construct remote queue */
  void Construct(ConstructTask *task, RunContext &ctx) {
    HILOG(kInfo, "(node {}) Constructing remote queue (task_node={}, task_state={}, method={})",
          LABSTOR_CLIENT->node_id_, task->task_node_, task->task_state_, task->method_);
    LABSTOR_THALLIUM->RegisterRpc("RpcPushSmall", [this](const tl::request &req,
                                                         TaskStateId state_id,
                                                         u32 method,
                                                         std::string &params) {
      this->RpcPushSmall(req, state_id, method, params);
    });
    LABSTOR_THALLIUM->RegisterRpc("RpcPushBulk", [this](const tl::request &req,
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

  /** Destroy remote queue */
  void Destruct(DestructTask *task, RunContext &ctx) {
    task->SetModuleComplete();
  }

  /** Handle output from replica PUSH */
  static void HandlePushReplicaOutput(int replica, std::string &ret, PushTask *task) {
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
  static void HandlePushReplicaEnd(PushTask *task) {
    task->exec_->ReplicateEnd(task->orig_task_->method_, task->orig_task_);
    task->orig_task_->SetModuleComplete();
    HILOG(kDebug, "Completing task (task_node={}, task_state={}, method={})",
          task->orig_task_->task_node_,
          task->orig_task_->task_state_,
          task->orig_task_->method_);
    task->SetModuleComplete();
  }

  /** Push for small message */
  void ClientSmallPush(std::vector<DataTransfer> &xfer, PushTask *task) {
    task->params_ = std::string((char *) xfer[0].data_, xfer[0].data_size_);
    for (int replica = 0; replica < task->domain_ids_.size(); ++replica) {
      DomainId domain_id = task->domain_ids_[replica];
      HILOG(kDebug, "(SM) Transferring {} bytes of data (task_node={}, task_state={}, method={}, from={}, to={})",
            task->params_.size(),
            task->orig_task_->task_node_,
            task->orig_task_->task_state_,
            task->orig_task_->method_,
            LABSTOR_CLIENT->node_id_,
            domain_id.id_);
      std::string ret = LABSTOR_THALLIUM->SyncCall<std::string>(domain_id.id_,
                                                                "RpcPushSmall",
                                                                task->exec_->id_,
                                                                task->exec_method_,
                                                                task->params_);
      HILOG(kDebug, "(SM) Finished {} bytes of data (task_node={}, task_state={}, method={}, from={}, to={})",
            task->params_.size(),
            task->orig_task_->task_node_,
            task->orig_task_->task_state_,
            task->orig_task_->method_,
            LABSTOR_CLIENT->node_id_,
            domain_id.id_);
      HandlePushReplicaOutput(replica, ret, task);
    }
  }

  /** Push for I/O message */
  void ClientIoPush(std::vector<DataTransfer> &xfer, PushTask *task) {
    task->params_ = std::string((char *) xfer[1].data_, xfer[1].data_size_);
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
            LABSTOR_CLIENT->node_id_,
            domain_id.id_,
            static_cast<int>(io_type));
      std::string ret = LABSTOR_THALLIUM->SyncIoCall<std::string>(domain_id.id_,
                                                                  "RpcPushBulk",
                                                                  io_type,
                                                                  data,
                                                                  data_size,
                                                                  task->exec_->id_,
                                                                  task->exec_method_,
                                                                  task->params_,
                                                                  data_size,
                                                                  io_type);
      HILOG(kDebug, "(IO) Finished transferring {} bytes of data (task_node={}, task_state={}, method={}, from={}, to={}, type={})",
            data_size,
            task->orig_task_->task_node_,
            task->orig_task_->task_state_,
            task->orig_task_->method_,
            LABSTOR_CLIENT->node_id_,
            domain_id.id_,
            static_cast<int>(io_type));
      HandlePushReplicaOutput(replica, ret, task);
    }
  }

  /** Push operation called on client */
  void Push(PushTask *task, RunContext &ctx) {
    std::vector<DataTransfer> &xfer = task->xfer_;
    switch (xfer.size()) {
      case 1: {
        ClientSmallPush(xfer, task);
        break;
      }
      case 2: {
        ClientIoPush(xfer, task);
        break;
      }
      default: {
        HELOG(kFatal, "The task {}/{} does not support remote calls", task->task_state_, task->method_);
      }
    }
    HandlePushReplicaEnd(task);
  }

 private:
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
    RpcExec(req, state_id, method, xfer, orig_task, exec);
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
    hshm::charbuf data(data_size);

    // Create the input data transfer object
    std::vector<DataTransfer> xfer(2);
    xfer[0].data_ = data.data();
    xfer[0].data_size_ = data.size();
    xfer[1].data_ = params.data();
    xfer[1].data_size_ = params.size();

    HILOG(kDebug, "Received large message of size {} "
                  "(task_state={}, method={})", xfer[0].data_size_, state_id, method);

    // Process the message
    if (io_type == IoType::kWrite) {
      LABSTOR_THALLIUM->IoCallServer(req, bulk, io_type, data.data(), data_size);
      HILOG(kDebug, "(node {}) Write blob integer: {}", LABSTOR_CLIENT->node_id_, (int)data[0])
    }
    TaskState *exec;
    Task *orig_task;
    RpcExec(req, state_id, method, xfer, orig_task, exec);
    if (io_type == IoType::kRead) {
      HILOG(kDebug, "(node {}) Read blob integer: {}", LABSTOR_CLIENT->node_id_, (int)data[0])
      LABSTOR_THALLIUM->IoCallServer(req, bulk, io_type, data.data(), data_size);
    }

    // Return
    RpcComplete(req, method, orig_task, exec, state_id);
  }

  /** Push operation called at the remote server */
  void RpcExec(const tl::request &req,
               const TaskStateId &state_id,
               u32 method,
               std::vector<DataTransfer> &xfer,
               Task *&orig_task, TaskState *&exec) {
    size_t data_size = xfer[0].data_size_;
    BinaryInputArchive<true> ar(xfer);

    // Deserialize task
    exec = LABSTOR_TASK_REGISTRY->GetTaskState(state_id);
    if (exec == nullptr) {
      HELOG(kFatal, "(node {}) Could not find the task state {}",
            LABSTOR_CLIENT->node_id_, state_id);
      req.respond(std::string());
      return;
    } else {
      HILOG(kDebug, "(node {}) Found task state {}",
            LABSTOR_CLIENT->node_id_,
            state_id);
    }
    TaskPointer task_ptr = exec->LoadStart(method, ar);
    orig_task = task_ptr.ptr_;
    hipc::Pointer &p = task_ptr.shm_;
    orig_task->domain_id_ = DomainId::GetNode(LABSTOR_CLIENT->node_id_);

    // Execute task
    MultiQueue *queue = LABSTOR_CLIENT->GetQueue(QueueId(state_id));
    orig_task->UnsetFireAndForget();
    orig_task->UnsetStarted();
    orig_task->UnsetMarked();
    orig_task->UnsetDataOwner();
    orig_task->UnsetLongRunning();
    queue->Emplace(orig_task->prio_, orig_task->lane_hash_, p);
    HILOG(kDebug,
          "(node {}) Executing task (task_node={}, task_state={}/{}, state_name={}, method={}, size={}, lane_hash={})",
          LABSTOR_CLIENT->node_id_,
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
    BinaryOutputArchive<false> ar(DomainId::GetNode(LABSTOR_CLIENT->node_id_));
    std::vector<DataTransfer> out_xfer = exec->SaveEnd(method, ar, orig_task);
    HILOG(kDebug, "(node {}) Returning {} bytes of data (task_node={}, task_state={}/{}, method={})",
          LABSTOR_CLIENT->node_id_,
          out_xfer[0].data_size_,
          orig_task->task_node_,
          orig_task->task_state_,
          state_id,
          method);
    // LABSTOR_CLIENT->DelTask(exec, orig_task);
    req.respond(std::string((char *) out_xfer[0].data_, out_xfer[0].data_size_));
  }

 public:
#include "remote_queue/remote_queue_lib_exec.h"
};
}  // namespace labstor

LABSTOR_TASK_CC(labstor::remote_queue::Server, "remote_queue");
