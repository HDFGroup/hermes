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

/**====================================
 * PRIVATE Init + Finalize Operations
 * ===================================*/

/** Internal initialization of Hermes */
void Hermes::Init(HermesType mode,
                  std::string server_config_path,
                  std::string client_config_path) {
  // Initialize hermes
  hshm::ScopedMutex lock(lock_, 1);
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
    case HermesType::kNone: {
      HELOG(kFatal, "Cannot have none HermesType")
    }
  }
  is_initialized_ = true;
  is_being_initialized_ = false;
}

/** Initialize Hermes as a server */
void Hermes::InitServer(std::string server_config_path) {
  HILOG(kDebug, "Initializing in Server mode")
  HERMES_THREAD_MODEL->SetThreadModel(hshm::ThreadType::kArgobots);
  LoadServerConfig(server_config_path);
  InitSharedMemory();
  HILOG(kDebug, "Initialized SHM")

  // Initialize RPC
  comm_.Init(HermesType::kServer);
  rpc_.InitServer();
  rpc_.InitClient();

  // Load the trait libraries
  traits_.Init();

  // Construct the reference objects
  mdm_.shm_init(header_->mdm_, main_alloc_, &server_config_);
  rpc_.InitClient();
  bpm_.shm_init(header_->bpm_, main_alloc_);
  borg_.shm_init(header_->borg_, main_alloc_);
  prefetch_.Init();
}

/** Initialize Hermes as a client to the daemon */
void Hermes::InitClient(std::string server_config_path,
                        std::string client_config_path) {
  LoadServerConfig(server_config_path);
  LoadClientConfig(client_config_path);
  LoadSharedMemory();

  // Initialize references to SHM types
  mdm_.shm_deserialize(header_->mdm_);
  rpc_.InitClient();
  bpm_.shm_deserialize(header_->bpm_);
  borg_.shm_deserialize(header_->borg_);
  prefetch_.Init();
  // mdm_.PrintDeviceInfo();

  // Load the trait libraries
  traits_.Init();
}

/** Load the server-side configuration */
void Hermes::LoadServerConfig(std::string config_path) {
  if (config_path.size() == 0) {
    config_path = GetEnvSafe(kHermesServerConf);
  }
  if (mode_ == HermesType::kServer) {
    HILOG(kInfo, "Loading server configuration: {}", config_path)
  }
  server_config_.LoadFromFile(config_path);
}

/** Load the client-side configuration */
void Hermes::LoadClientConfig(std::string config_path) {
  if (config_path.size() == 0) {
    config_path = GetEnvSafe(kHermesClientConf);
  }
  // HILOG(kInfo, "Loading client configuration: {}", config_path)
  client_config_.LoadFromFile(config_path);
}

/** Initialize shared-memory between daemon and client */
void Hermes::InitSharedMemory() {
  // Create shared-memory allocator
  auto mem_mngr = HERMES_MEMORY_MANAGER;
  mem_mngr->CreateBackend<hipc::PosixShmMmap>(
      hipc::MemoryManager::GetDefaultBackendSize(),
      server_config_.shmem_name_);
  main_alloc_ =
      mem_mngr->CreateAllocator<hipc::ScalablePageAllocator>(
          server_config_.shmem_name_,
          main_alloc_id,
          sizeof(HermesShm));
  header_ = main_alloc_->GetCustomHeader<HermesShm>();
}

/** Connect to a Daemon's shared memory */
void Hermes::LoadSharedMemory() {
  // Load shared-memory allocator
  auto mem_mngr = HERMES_MEMORY_MANAGER;
  mem_mngr->AttachBackend(hipc::MemoryBackendType::kPosixShmMmap,
                          server_config_.shmem_name_);
  main_alloc_ = mem_mngr->GetAllocator(main_alloc_id);
  header_ = main_alloc_->GetCustomHeader<HermesShm>();
}

/** Finalize Daemon mode */
void Hermes::FinalizeServer() {
  // NOTE(llogan): rpc_.Finalize() is called internally by daemon in this case
  // bpm_.shm_destroy();
  // mdm_.shm_destroy();
}

/** Finalize client mode */
void Hermes::FinalizeClient() {
  if (client_config_.stop_daemon_) {
    StopDaemon();
  }
  rpc_.Finalize();
}

/**====================================
 * PUBLIC Finalize Operations
 * ===================================*/

/** Finalize Hermes explicitly */
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
  is_initialized_ = false;
  is_terminated_ = true;
}

/** Run the Hermes core Daemon */
void Hermes::RunDaemon() {
  rpc_.RunDaemon();
}

/** Stop the Hermes core Daemon */
void Hermes::StopDaemon() {
  rpc_.StopDaemon();
}

/**====================================
 * PUBLIC Bucket Operations
 * ===================================*/

/** Get or create a Bucket in Hermes */
Bucket Hermes::GetBucket(std::string name,
                         Context ctx,
                         size_t backend_size) {
  return Bucket(name, ctx, backend_size);
}

/** Get an existing Bucket in Hermes */
Bucket Hermes::GetBucket(TagId tag_id) {
  return Bucket(tag_id);
}

/**====================================
 * PUBLIC I/O Operations
 * ===================================*/

/** Waits for all blobs to finish being flushed */
void Hermes::Flush() {
  borg_.GlobalWaitForFullFlush();
}

/** Destroy all buckets and blobs in this instance */
void Hermes::Clear() {
  mdm_.GlobalClear();
}

/**====================================
 * PUBLIC Tag Operations
 * ===================================*/

/** Locate all blobs with a tag */
std::vector<BlobId> Hermes::GroupBy(TagId tag_id) {
  return mdm_.GlobalGroupByTag(tag_id);
}



}  // namespace hermes::api
