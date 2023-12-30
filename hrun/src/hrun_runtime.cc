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

#include "hermes_shm/util/singleton.h"
#include "hrun/api/hrun_runtime.h"

namespace hrun {

/** Create the server-side API */
Runtime* Runtime::Create(std::string server_config_path) {
  hshm::ScopedMutex lock(lock_, 1);
  if (is_initialized_) {
    return this;
  }
  mode_ = HrunMode::kServer;
  is_being_initialized_ = true;
  ServerInit(std::move(server_config_path));
  is_initialized_ = true;
  is_being_initialized_ = false;
  return this;
}

/** Initialize */
void Runtime::ServerInit(std::string server_config_path) {
  LoadServerConfig(server_config_path);
  HILOG(kInfo, "Initializing shared memory")
  InitSharedMemory();
  HILOG(kInfo, "Initializing RPC")
  rpc_.ServerInit(&server_config_);
  HILOG(kInfo, "Initializing thallium")
  thallium_.ServerInit(&rpc_);
  HILOG(kInfo, "Initializing queues + workers")
  header_->node_id_ = rpc_.node_id_;
  header_->unique_ = 0;
  header_->num_nodes_ = server_config_.rpc_.host_names_.size();
  task_registry_.ServerInit(&server_config_, rpc_.node_id_, header_->unique_);
  // Queue manager + client must be initialized before Work Orchestrator
  queue_manager_.ServerInit(main_alloc_,
                            rpc_.node_id_,
                            &server_config_,
                            header_->queue_manager_);
  HRUN_CLIENT->Create(server_config_path, "", true);
  HERMES_THREAD_MODEL->SetThreadModel(hshm::ThreadType::kPthread);
  work_orchestrator_.ServerInit(&server_config_, queue_manager_);
  hipc::mptr<Admin::CreateTaskStateTask> admin_task;

  // Create the admin library
  HRUN_CLIENT->MakeTaskStateId();
  admin_task = hipc::make_mptr<Admin::CreateTaskStateTask>();
  task_registry_.RegisterTaskLib("hrun_admin");
  task_registry_.CreateTaskState(
      "hrun_admin",
      "hrun_admin",
      HRUN_QM_CLIENT->admin_task_state_,
      admin_task.get());

  // Create the process queue
  HRUN_CLIENT->MakeTaskStateId();
  admin_task = hipc::make_mptr<Admin::CreateTaskStateTask>();
  task_registry_.RegisterTaskLib("proc_queue");
  task_registry_.CreateTaskState(
      "proc_queue",
      "proc_queue",
      HRUN_QM_CLIENT->process_queue_,
      admin_task.get());

  // Create the work orchestrator queue scheduling library
  TaskStateId queue_sched_id = HRUN_CLIENT->MakeTaskStateId();
  admin_task = hipc::make_mptr<Admin::CreateTaskStateTask>();
  task_registry_.RegisterTaskLib("worch_queue_round_robin");
  task_registry_.CreateTaskState(
      "worch_queue_round_robin",
      "worch_queue_round_robin",
      queue_sched_id,
      admin_task.get());

  // Create the work orchestrator process scheduling library
  TaskStateId proc_sched_id = HRUN_CLIENT->MakeTaskStateId();
  admin_task = hipc::make_mptr<Admin::CreateTaskStateTask>();
  task_registry_.RegisterTaskLib("worch_proc_round_robin");
  task_registry_.CreateTaskState(
      "worch_proc_round_robin",
      "worch_proc_round_robin",
      proc_sched_id,
      admin_task.get());

  // Set the work orchestrator queue scheduler
  HRUN_ADMIN->SetWorkOrchQueuePolicyRoot(hrun::DomainId::GetLocal(), queue_sched_id);
  HRUN_ADMIN->SetWorkOrchProcPolicyRoot(hrun::DomainId::GetLocal(), proc_sched_id);

  // Create the remote queue library
  task_registry_.RegisterTaskLib("remote_queue");
  remote_queue_.CreateRoot(DomainId::GetLocal(), "remote_queue",
                           HRUN_CLIENT->MakeTaskStateId());
  remote_created_ = true;
}

/** Initialize shared-memory between daemon and client */
void Runtime::InitSharedMemory() {
  // Create shared-memory allocator
  config::QueueManagerInfo &qm = server_config_.queue_manager_;
  auto mem_mngr = HERMES_MEMORY_MANAGER;
  if (qm.shm_size_ == 0) {
    qm.shm_size_ =
        hipc::MemoryManager::GetDefaultBackendSize();
  }
  // Create general allocator
  mem_mngr->CreateBackend<hipc::PosixShmMmap>(
      qm.shm_size_,
      qm.shm_name_);
  main_alloc_ =
      mem_mngr->CreateAllocator<hipc::ScalablePageAllocator>(
          qm.shm_name_,
          main_alloc_id_,
          sizeof(HrunShm));
  header_ = main_alloc_->GetCustomHeader<HrunShm>();
  // Create separate data allocator
  mem_mngr->CreateBackend<hipc::PosixShmMmap>(
      qm.data_shm_size_,
      qm.data_shm_name_);
  data_alloc_ =
      mem_mngr->CreateAllocator<hipc::ScalablePageAllocator>(
          qm.data_shm_name_,
          data_alloc_id_, 0);
  // Create separate runtime data allocator
  mem_mngr->CreateBackend<hipc::PosixShmMmap>(
      qm.rdata_shm_size_,
      qm.rdata_shm_name_);
  rdata_alloc_ =
      mem_mngr->CreateAllocator<hipc::ScalablePageAllocator>(
          qm.rdata_shm_name_,
          rdata_alloc_id_, 0);
}

/** Finalize Hermes explicitly */
void Runtime::Finalize() {}

/** Run the Hermes core Daemon */
void Runtime::RunDaemon() {
  thallium_.RunDaemon();
  HILOG(kInfo, "Daemon is running")
//    while (HRUN_WORK_ORCHESTRATOR->IsRuntimeAlive()) {
//      // Scheduler callbacks?
//      HERMES_THREAD_MODEL->SleepForUs(1000);
//    }
  HILOG(kInfo, "Finishing up last requests")
  HRUN_WORK_ORCHESTRATOR->Join();
  HILOG(kInfo, "Daemon is exiting")
}

/** Stop the Hermes core Daemon */
void Runtime::StopDaemon() {
  HRUN_WORK_ORCHESTRATOR->FinalizeRuntime();
}

/** Get the set of DomainIds */
std::vector<DomainId>
Runtime::ResolveDomainId(const DomainId &domain_id) {
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

}  // namespace hrun

/** Runtime singleton */
DEFINE_SINGLETON_CC(hrun::Runtime)