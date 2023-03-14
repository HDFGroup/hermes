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

#include "hermes.h"
#include "bucket.h"

namespace hermes::api {

void Hermes::Init(HermesType mode,
                  std::string server_config_path,
                  std::string client_config_path) {
  hermes_shm::ScopedMutex lock(lock_);
  if (is_initialized_) {
    return;
  }
  mode_ = mode;
  is_being_initialized_ = true;
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
  }
  is_initialized_ = true;
  is_being_initialized_ = false;
}

void Hermes::Finalize() {
  if (!is_initialized_ || is_terminated_) {
    return;
  }
  switch (mode_) {
    case HermesType::kServer: {
      FinalizeServer();
      break;
    }
    case HermesType::kClient: {
      FinalizeClient();
      break;
    }
    default: {
      throw std::logic_error("Invalid HermesType to launch in");
    }
  }
  // TODO(llogan): make re-initialization possible.
  is_initialized_ = false;
  is_terminated_ = true;
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
  comm_.Init(HermesType::kServer);
  rpc_.InitServer();
  rpc_.InitClient();
  mdm_.local_init();
  mdm_.shm_init(&server_config_, &header_->mdm_);
  bpm_ = hipc::make_mptr<BufferPool>(header_->bpm_, main_alloc_);
  bpm_->shm_init();
  borg_.shm_init();
}

void Hermes::InitClient(std::string server_config_path,
                        std::string client_config_path) {
  LoadServerConfig(server_config_path);
  LoadClientConfig(client_config_path);
  LoadSharedMemory();
  rpc_.InitClient();
  mdm_.local_init();
  mdm_.shm_deserialize(&header_->mdm_);
  bpm_ = hipc::manual_ptr(hipc::ShmDeserialize<BufferPool>(&header_->bpm_,
                                                           main_alloc_));
  borg_.shm_deserialize();
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
  auto mem_mngr = HERMES_MEMORY_MANAGER;
  mem_mngr->CreateBackend<hipc::PosixShmMmap>(
      hipc::MemoryManager::kDefaultBackendSize,
      server_config_.shmem_name_);
  main_alloc_ =
      mem_mngr->CreateAllocator<hipc::ScalablePageAllocator>(
      // mem_mngr->CreateAllocator<hipc::StackAllocator>(
          server_config_.shmem_name_,
          main_alloc_id,
          sizeof(HermesShmHeader));
  header_ = main_alloc_->GetCustomHeader<HermesShmHeader>();
}

void Hermes::LoadSharedMemory() {
  // Load shared-memory allocator
  auto mem_mngr = HERMES_MEMORY_MANAGER;
  mem_mngr->AttachBackend(hipc::MemoryBackendType::kPosixShmMmap,
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

std::shared_ptr<Bucket> Hermes::GetBucket(std::string name,
                                          Context ctx,
                                          size_t backend_size) {
  return std::make_shared<Bucket>(name, ctx, backend_size);
}

std::vector<BlobId> Hermes::GroupBy(TagId tag_id) {
  return mdm_.GlobalGroupByTag(tag_id);
}

void Hermes::Clear() {
  mdm_.GlobalClear();
}

}  // namespace hermes::api
