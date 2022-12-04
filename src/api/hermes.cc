//
// Created by lukemartinlogan on 12/3/22.
//

#include "hermes.h"

namespace hermes::api {

Hermes::Hermes(HermesType mode,
               std::string server_config_path,
               std::string client_config_path)
    : comm_(mode),
      rpc_(&comm_, &server_config_) {
  Init(mode, std::move(server_config_path), std::move(client_config_path));
}

void Hermes::Init(HermesType mode,
                  std::string server_config_path,
                  std::string client_config_path) {
  mode_ = mode;
  switch (mode) {
    case HermesType::kServer: {
      InitDaemon(std::move(server_config_path));
      break;
    }
    case HermesType::kClient: {
      InitClient(std::move(client_config_path));
      break;
    }
    case HermesType::kColocated: {
      InitColocated(std::move(server_config_path),
                    std::move(client_config_path));
      break;
    }
  }
}

void Hermes::Finalize() {}

void Hermes::RunDaemon() {}

void Hermes::InitDaemon(std::string server_config_path) {
  LoadServerConfig(server_config_path);
  InitSharedMemory();
}

void Hermes::InitColocated(std::string server_config_path,
                   std::string client_config_path) {
  LoadServerConfig(server_config_path);
  LoadClientConfig(client_config_path);
  InitSharedMemory();
}

void Hermes::InitClient(std::string client_config_path) {
  LoadClientConfig(client_config_path);
  LoadSharedMemory();
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
  mem_mngr->CreateBackend(lipc::MemoryBackendType::kPosixShmMmap,
                          server_config_.shmem_name_);
  main_alloc_ =
      mem_mngr->CreateAllocator(lipc::AllocatorType::kPageAllocator,
                                server_config_.shmem_name_, main_alloc_id);
}

void Hermes::LoadSharedMemory() {
  // Load shared-memory allocator
  auto mem_mngr = LABSTOR_MEMORY_MANAGER;
  mem_mngr->CreateBackend(lipc::MemoryBackendType::kPosixShmMmap,
                          server_config_.shmem_name_);
  mem_mngr->ScanBackends();
  main_alloc_ = mem_mngr->GetAllocator(main_alloc_id);
}

void Hermes::FinalizeDaemon() {}

void Hermes::FinalizeClient() {}

void Hermes::FinalizeColocated() {}

}  // namespace hermes::api