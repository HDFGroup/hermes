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

#ifndef HERMES_SRC_API_HERMES_H_
#define HERMES_SRC_API_HERMES_H_

#include "config_client.h"
#include "config_server.h"
#include "constants.h"
#include "hermes_types.h"
#include "utils.h"

#include "communication_factory.h"
#include "rpc.h"
#include "metadata_manager.h"
#include "buffer_pool.h"
#include "buffer_organizer.h"
#include "hermes_shm/util/singleton.h"

// Singleton macros
#define HERMES hermes_shm::GlobalSingleton<hermes::api::Hermes>::GetInstance()
#define HERMES_T hermes::api::Hermes*

namespace hermes::api {

class Bucket;


/**
 * The Hermes shared-memory header
 * */
struct HermesShmHeader {
  hipc::Pointer ram_tier_;
  MetadataManagerShmHeader mdm_;
  hermes::ShmHeader<BufferPool> bpm_;
};

/**
 * An index into all Hermes-related data structures.
 * */
class Hermes {
 public:
  HermesType mode_;
  HermesShmHeader *header_;
  ServerConfig server_config_;
  ClientConfig client_config_;
  MetadataManager mdm_;
  hipc::manual_ptr<BufferPool> bpm_;
  BufferOrganizer borg_;
  COMM_TYPE comm_;
  RPC_TYPE rpc_;
  hipc::Allocator *main_alloc_;
  bool is_being_initialized_;
  bool is_initialized_;
  bool is_terminated_;
  bool is_transparent_;
  hermes_shm::Mutex lock_;

 public:
  /** Default constructor */
  Hermes() : is_being_initialized_(false),
             is_initialized_(false),
             is_transparent_(false),
             is_terminated_(false) {}

  /** Destructor */
  ~Hermes() {}

  /** Whether or not Hermes is currently being initialized */
  bool IsBeingInitialized() { return is_being_initialized_; }

  /** Whether or not Hermes is initialized */
  bool IsInitialized() { return is_initialized_; }

  /** Whether or not Hermes is finalized */
  bool IsTerminated() { return is_terminated_; }

  /** Initialize Hermes explicitly */
  static Hermes* Create(HermesType mode = HermesType::kClient,
                        std::string server_config_path = "",
                        std::string client_config_path = "") {
    auto hermes = HERMES;
    hermes->Init(mode, server_config_path, client_config_path);
    return hermes;
  }

 public:
  /** Finalize Hermes explicitly */
  void Finalize();

  /** Run the Hermes core Daemon */
  void RunDaemon();

  /** Stop the Hermes core Daemon */
  void StopDaemon();

 public:
  /** Create a Bucket in Hermes */
  std::shared_ptr<Bucket> GetBucket(std::string name,
                                    Context ctx = Context(),
                                    IoClientContext = IoClientContext());

  /** Locate all blobs with a tag */
  std::list<BlobId> GroupBy(std::string tag_name);

  /** Destroy all buckets and blobs in this instance */
  void Clear();

  /** Create a trait */
  template<typename TraitT, typename ...Args>
  TraitId RegisterTrait(const std::string &tag_uuid,
                        Args&& ...args) {
    TraitT obj(tag_uuid, std::forward<Args>(args)...);
    HERMES->mdm_.GlobalRegisterTrait(
        tag_uuid, obj);
  }

  /** Get trait id */
  TraitId GetTraitId(const std::string &tag_uuid);

  /** Get the trait */
  template<typename TraitT>
  TraitT* GetTrait(TraitId id) {
    // TODO(llogan)
  }

 private:
  /** Internal initialization of Hermes */
  void Init(HermesType mode = HermesType::kClient,
            std::string server_config_path = "",
            std::string client_config_path = "");

  /** Initialize Hermes as a server */
  void InitServer(std::string server_config_path);

  /** Initialize Hermes as a client to the daemon */
  void InitClient(std::string server_config_path,
                  std::string client_config_path);

  /** Load the server-side configuration */
  void LoadServerConfig(std::string config_path);

  /** Load the client-side configuration */
  void LoadClientConfig(std::string config_path);

  /** Initialize shared-memory between daemon and client */
  void InitSharedMemory();

  /** Connect to a Daemon's shared memory */
  void LoadSharedMemory();

  /** Finalize Daemon mode */
  void FinalizeServer();

  /** Finalize client mode */
  void FinalizeClient();
};

#define TRANSPARENT_HERMES\
  if (!HERMES->IsInitialized() && \
      !HERMES->IsBeingInitialized() && \
      !HERMES->IsTerminated()) {\
    HERMES->Create(hermes::HermesType::kClient);\
    HERMES->is_transparent_ = true;\
  }

}  // namespace hermes::api

#endif  // HERMES_SRC_API_HERMES_H_
