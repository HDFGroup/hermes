//
// Created by lukemartinlogan on 6/27/23.
//

#ifndef LABSTOR_INCLUDE_LABSTOR_CLIENT_LABSTOR_SERVER_H_
#define LABSTOR_INCLUDE_LABSTOR_CLIENT_LABSTOR_SERVER_H_

#include "labstor/task_registry/task_registry.h"
#include "labstor/work_orchestrator/work_orchestrator.h"
#include "labstor/queue_manager/queue_manager_runtime.h"
#include "labstor_admin/labstor_admin.h"
#include "remote_queue/remote_queue.h"
#include "labstor_client.h"
#include "manager.h"
#include "labstor/network/rpc.h"
#include "labstor/network/rpc_thallium.h"

// Singleton macros
#define LABSTOR_RUNTIME hshm::Singleton<labstor::Runtime>::GetInstance()
#define LABSTOR_RUNTIME_T labstor::Runtime*
#define LABSTOR_REMOTE_QUEUE (&LABSTOR_RUNTIME->remote_queue_)
#define LABSTOR_THALLIUM (&LABSTOR_RUNTIME->thallium_)
#define LABSTOR_RPC (&LABSTOR_RUNTIME->rpc_)

namespace labstor {

class Runtime : public ConfigurationManager {
 public:
  int data_;
  TaskRegistry task_registry_;
  WorkOrchestrator work_orchestrator_;
  QueueManagerRuntime queue_manager_;
  remote_queue::Client remote_queue_;
  RpcContext rpc_;
  ThalliumRpc thallium_;

 public:
  /** Default constructor */
  Runtime() = default;

  /** Create the server-side API */
  Runtime* Create(std::string server_config_path = "") {
    hshm::ScopedMutex lock(lock_, 1);
    if (is_initialized_) {
      return this;
    }
    mode_ = LabstorMode::kServer;
    is_being_initialized_ = true;
    ServerInit(std::move(server_config_path));
    is_initialized_ = true;
    is_being_initialized_ = false;
    return this;
  }

 private:
  /** Initialize */
  void ServerInit(std::string server_config_path) {
    LoadServerConfig(server_config_path);
    InitSharedMemory();
    rpc_.ServerInit(&server_config_);
    thallium_.ServerInit(&rpc_);
    header_->node_id_ = rpc_.node_id_;
    header_->unique_ = 0;
    header_->num_nodes_ = server_config_.rpc_.host_names_.size();
    task_registry_.ServerInit(&server_config_, rpc_.node_id_, header_->unique_);
    // Queue manager + client must be initialized before Work Orchestrator
    queue_manager_.ServerInit(main_alloc_,
                              rpc_.node_id_,
                              &server_config_,
                              header_->queue_manager_);
    LABSTOR_CLIENT->Create(server_config_path, "", true);
    HERMES_THREAD_MODEL->SetThreadModel(hshm::ThreadType::kPthread);
    work_orchestrator_.ServerInit(&server_config_, queue_manager_);
    hipc::mptr<Admin::CreateTaskStateTask> admin_task;

    // Create the admin library
    LABSTOR_CLIENT->MakeTaskStateId();
    admin_task = hipc::make_mptr<Admin::CreateTaskStateTask>();
    task_registry_.RegisterTaskLib("labstor_admin");
    task_registry_.CreateTaskState(
        "labstor_admin",
        "labstor_admin",
        LABSTOR_QM_CLIENT->admin_task_state_,
        admin_task.get());

    // Create the process queue
    LABSTOR_CLIENT->MakeTaskStateId();
    admin_task = hipc::make_mptr<Admin::CreateTaskStateTask>();
    task_registry_.RegisterTaskLib("proc_queue");
    task_registry_.CreateTaskState(
        "proc_queue",
        "proc_queue",
        LABSTOR_QM_CLIENT->process_queue_,
        admin_task.get());

    // Create the work orchestrator queue scheduling library
    TaskStateId queue_sched_id = LABSTOR_CLIENT->MakeTaskStateId();
    admin_task = hipc::make_mptr<Admin::CreateTaskStateTask>();
    task_registry_.RegisterTaskLib("worch_queue_round_robin");
    task_registry_.CreateTaskState(
        "worch_queue_round_robin",
        "worch_queue_round_robin",
        queue_sched_id,
        admin_task.get());

    // Create the work orchestrator process scheduling library
    TaskStateId proc_sched_id = LABSTOR_CLIENT->MakeTaskStateId();
    admin_task = hipc::make_mptr<Admin::CreateTaskStateTask>();
    task_registry_.RegisterTaskLib("worch_proc_round_robin");
    task_registry_.CreateTaskState(
        "worch_proc_round_robin",
        "worch_proc_round_robin",
        proc_sched_id,
        admin_task.get());

    // Set the work orchestrator queue scheduler
    LABSTOR_ADMIN->SetWorkOrchQueuePolicyRoot(labstor::DomainId::GetLocal(), queue_sched_id);
    LABSTOR_ADMIN->SetWorkOrchProcPolicyRoot(labstor::DomainId::GetLocal(), proc_sched_id);

    // Create the remote queue library
    task_registry_.RegisterTaskLib("remote_queue");
    remote_queue_.CreateRoot(DomainId::GetLocal(), "remote_queue",
                             LABSTOR_CLIENT->MakeTaskStateId());
  }

 public:
  /** Initialize shared-memory between daemon and client */
  void InitSharedMemory() {
    // Create shared-memory allocator
    auto mem_mngr = HERMES_MEMORY_MANAGER;
    if (server_config_.queue_manager_.shm_size_ == 0) {
      server_config_.queue_manager_.shm_size_ =
          hipc::MemoryManager::GetDefaultBackendSize();
    }
    mem_mngr->CreateBackend<hipc::PosixShmMmap>(
        server_config_.queue_manager_.shm_size_,
        server_config_.queue_manager_.shm_name_);
    // hipc::ScalablePageAllocator
    main_alloc_ =
        mem_mngr->CreateAllocator<hipc::StackAllocator>(
            server_config_.queue_manager_.shm_name_,
            main_alloc_id_,
            sizeof(LabstorShm));
    header_ = main_alloc_->GetCustomHeader<LabstorShm>();
  }

  /** Finalize Hermes explicitly */
  void Finalize() {}

  /** Run the Hermes core Daemon */
  void RunDaemon() {
    thallium_.RunDaemon();
    HILOG(kInfo, "Daemon is running")
//    while (LABSTOR_WORK_ORCHESTRATOR->IsRuntimeAlive()) {
//      // Scheduler callbacks?
//      HERMES_THREAD_MODEL->SleepForUs(1000);
//    }
    HILOG(kInfo, "Finishing up last requests")
    LABSTOR_WORK_ORCHESTRATOR->Join();
    HILOG(kInfo, "Daemon is exiting")
  }

  /** Stop the Hermes core Daemon */
  void StopDaemon() {
    LABSTOR_WORK_ORCHESTRATOR->FinalizeRuntime();
  }

  /** Get the set of DomainIds */
  std::vector<DomainId> ResolveDomainId(const DomainId &domain_id) {
    std::vector<DomainId> ids;
    if (domain_id.IsGlobal()) {
      ids.reserve(rpc_.hosts_.size());
      for (HostInfo &host_info : rpc_.hosts_) {
        ids.push_back(DomainId::GetNode(host_info.node_id_));
      }
    } else if (domain_id.IsNode()) {
      ids.reserve(1);
      ids.push_back(domain_id);
    }
    // TODO(llogan): handle named domain ID sets
    return ids;
  }
};

}  // namespace labstor

#endif  // LABSTOR_INCLUDE_LABSTOR_CLIENT_LABSTOR_SERVER_H_
