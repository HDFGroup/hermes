//
// Created by lukemartinlogan on 12/3/22.
//

#include "hermes.h"
#include "bucket.h"
#include "vbucket.h"

namespace hermes::api {

void Hermes::Init(HermesType mode,
                  std::string server_config_path,
                  std::string client_config_path) {
  mode_ = mode;
  switch (mode_) {
    case HermesType::kServer: {
      InitServer(std::move(server_config_path));
      break;
    }
    case HermesType::kClient: {
      InitClient(std::move(server_config_path),
                 std::move(client_config_path));
      break;
    }
    case HermesType::kColocated: {
      InitColocated(std::move(server_config_path),
                    std::move(client_config_path));
      break;
    }
  }
}

void Hermes::Finalize() {
  switch (mode_) {
    case HermesType::kServer: {
      FinalizeServer();
      break;
    }
    case HermesType::kClient: {
      FinalizeClient();
      break;
    }
    case HermesType::kColocated: {
      FinalizeColocated();
      break;
    }
  }
}

void Hermes::RunDaemon() {
  rpc_.RunDaemon();
}

void Hermes::StopDaemon() {
  rpc_.StopDaemon();
}

void Hermes::InitServer(std::string server_config_path) {
  LoadServerConfig(server_config_path);
  InitSharedMemory();
  rpc_.InitServer();
  rpc_.InitClient();
  mdm_.shm_init(&server_config_, &header_->mdm_);
  bpm_.shm_init(&header_->bpm_);
  borg_.shm_init();
}

void Hermes::InitClient(std::string server_config_path,
                        std::string client_config_path) {
  LoadServerConfig(server_config_path);
  LoadClientConfig(client_config_path);
  LoadSharedMemory();
  rpc_.InitClient();
  mdm_.shm_deserialize(&header_->mdm_);
  bpm_.shm_deserialize(&header_->bpm_);
  borg_.shm_deserialize();
}

void Hermes::InitColocated(std::string server_config_path,
                           std::string client_config_path) {
  LoadServerConfig(server_config_path);
  LoadClientConfig(client_config_path);
  InitSharedMemory();
  rpc_.InitColocated();
  mdm_.shm_init(&server_config_, &header_->mdm_);
  bpm_.shm_init(&header_->bpm_);
  borg_.shm_init();
}

void Hermes::LoadServerConfig(std::string config_path) {
  if (config_path.size() == 0) {
    config_path = GetEnvSafe(kHermesServerConf);
  }
  server_config_.LoadFromFile(config_path);
}

void Hermes::LoadClientConfig(std::string config_path) {
  if (config_path.size() == 0) {
    config_path = GetEnvSafe(kHermesClientConf);
  }
  client_config_.LoadFromFile(config_path);
}

void Hermes::InitSharedMemory() {
  // Create shared-memory allocator
  auto mem_mngr = LABSTOR_MEMORY_MANAGER;
  mem_mngr->CreateBackend<lipc::PosixShmMmap>(
      lipc::MemoryManager::kDefaultBackendSize,
      server_config_.shmem_name_);
  main_alloc_ =
      mem_mngr->CreateAllocator<lipc::StackAllocator>(
          server_config_.shmem_name_,
          main_alloc_id,
          sizeof(HermesShmHeader));
  header_ = main_alloc_->GetCustomHeader<HermesShmHeader>();
}

void Hermes::LoadSharedMemory() {
  // Load shared-memory allocator
  auto mem_mngr = LABSTOR_MEMORY_MANAGER;
  mem_mngr->AttachBackend(lipc::MemoryBackendType::kPosixShmMmap,
                          server_config_.shmem_name_);
  main_alloc_ = mem_mngr->GetAllocator(main_alloc_id);
  header_ = main_alloc_->GetCustomHeader<HermesShmHeader>();
}

void Hermes::FinalizeServer() {
  // NOTE(llogan): rpc_.Finalize() is called internally by daemon in this case
  // bpm_.shm_destroy();
  // mdm_.shm_destroy();
}

void Hermes::FinalizeClient() {
  if (client_config_.stop_daemon_) {
    StopDaemon();
  }
  rpc_.Finalize();
}

void Hermes::FinalizeColocated() {
  rpc_.Finalize();
}

std::shared_ptr<Bucket> Hermes::GetBucket(std::string name,
                                          Context ctx) {
  return std::make_shared<Bucket>(name, ctx);
}

std::shared_ptr<VBucket> Hermes::GetVBucket(std::string name,
                                            Context ctx) {
  return std::make_shared<VBucket>(name, ctx);
}

}  // namespace hermes::api