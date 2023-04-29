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
#include "hermes_types.h"
#include "utils.h"

#include "rpc.h"
#include "metadata_manager.h"
#include "buffer_pool.h"
#include "buffer_organizer.h"
#include "prefetcher.h"
#include "trait_manager.h"
#include "bucket.h"

#include "hermes_shm/util/singleton.h"

// Singleton macros
#define HERMES hshm::Singleton<hermes::api::Hermes>::GetInstance()
#define HERMES_T hermes::api::Hermes*

namespace hapi = hermes::api;

namespace hermes::api {

/**
 * The Hermes shared-memory header
 * */
struct HermesShm {
  hipc::Pointer ram_tier_;
  hipc::ShmArchive<MetadataManagerShm> mdm_;
  hipc::ShmArchive<BufferPoolShm> bpm_;
  hipc::ShmArchive<BufferOrganizerShm> borg_;
};

/**
 * An index into all Hermes-related data structures.
 * */
class Hermes {
 public:
  HermesType mode_;
  HermesShm *header_;
  ServerConfig server_config_;
  ClientConfig client_config_;
  MetadataManager mdm_;
  BufferPool bpm_;
  BufferOrganizer borg_;
  TraitManager traits_;
  Prefetcher prefetch_;
  RPC_TYPE rpc_;
  hipc::Allocator *main_alloc_;
  bool is_being_initialized_;
  bool is_initialized_;
  bool is_terminated_;
  bool is_transparent_;
  hshm::Mutex lock_;

 public:
  /**====================================
   * PUBLIC Init Operations
   * ===================================*/

  /** Default constructor */
  Hermes() : is_being_initialized_(false),
             is_initialized_(false),
             is_terminated_(false),
             is_transparent_(false) {}

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
  /**====================================
   * PUBLIC Finalize Operations
   * ===================================*/

  /** Finalize Hermes explicitly */
  void Finalize();

  /** Run the Hermes core Daemon */
  void RunDaemon();

  /** Stop the Hermes core Daemon */
  void StopDaemon();

 public:
  /**====================================
   * PUBLIC Bucket Operations
   * ===================================*/

  /** Get or create a Bucket in Hermes */
  Bucket GetBucket(std::string name,
                   Context ctx = Context(),
                   size_t backend_size = 0);

  /** Get an existing Bucket in Hermes */
  Bucket GetBucket(TagId bkt_id);

  /**====================================
   * PUBLIC I/O Operations
   * ===================================*/

  /** Waits for all blobs to finish being flushed */
  void Flush();

  /** Destroy all buckets and blobs in this instance */
  void Clear();

  /**====================================
   * PUBLIC Tag Operations
   * ===================================*/

  /** Create a generic tag in Hermes */
  TagId CreateTag(const std::string &tag_name) {
    std::vector<TraitId> traits;
    return mdm_.GlobalCreateTag(tag_name, false, traits);
  }

  /** Get the TagId  */
  TagId GetTagId(const std::string &tag_name) {
    return mdm_.GlobalGetTagId(tag_name);
  }

  /** Locate all blobs with a tag */
  std::vector<BlobId> GroupBy(TagId tag_id);

  /**====================================
   * PUBLIC Trait Operations
   * ===================================*/

  /** Create a trait */
  template<typename TraitT, typename ...Args>
  TraitId RegisterTrait(TraitT *trait) {
    TraitId id = GetTraitId(trait->GetTraitUuid());
    if (!id.IsNull()) {
      HILOG(kDebug, "Found existing trait trait: {}", trait->GetTraitUuid())
      return id;
    }
    HILOG(kDebug, "Registering a new trait: {}", trait->GetTraitUuid())
    id = HERMES->mdm_.GlobalRegisterTrait(TraitId::GetNull(),
                                          trait->trait_info_);
    HILOG(kDebug, "Giving trait {} id {}.{}",
          trait->GetTraitUuid(), id.node_id_, id.unique_)
    return id;
  }

  /** Create a trait */
  template<typename TraitT, typename ...Args>
  TraitId RegisterTrait(const std::string &trait_uuid,
                        Args&& ...args) {
    TraitId id = GetTraitId(trait_uuid);
    if (!id.IsNull()) {
      HILOG(kDebug, "Found existing trait trait: {}", trait_uuid)
      return id;
    }
    HILOG(kDebug, "Registering new trait: {}", trait_uuid)
    TraitT obj(trait_uuid, std::forward<Args>(args)...);
    id = HERMES->mdm_.GlobalRegisterTrait(TraitId::GetNull(),
                                           obj.trait_info_);
    HILOG(kDebug, "Giving trait \"{}\" id {}.{}",
          trait_uuid, id.node_id_, id.unique_)
    return id;
  }

  /** Get trait id */
  TraitId GetTraitId(const std::string &trait_uuid) {
    return HERMES->mdm_.GlobalGetTraitId(trait_uuid);
  }

  /** Get the trait */
  Trait* GetTrait(TraitId trait_id) {
    return mdm_.GlobalGetTrait(trait_id);
  }

  /** Attach a trait to a tag */
  void AttachTrait(TagId tag_id, TraitId trait_id) {
    HERMES->mdm_.GlobalTagAddTrait(tag_id, trait_id);
  }

  /** Get traits attached to tag */
  std::vector<Trait*> GetTraits(TagId tag_id,
                                uint32_t flags = ALL_BITS(uint32_t)) {
    // HILOG(kDebug, "Getting the traits for tag {}", tag_id)
    std::vector<TraitId> trait_ids = HERMES->mdm_.GlobalTagGetTraits(tag_id);
    std::vector<Trait*> traits;
    traits.reserve(trait_ids.size());
    for (TraitId &trait_id : trait_ids) {
      auto trait = GetTrait(trait_id);
      if (!trait) { continue; }
      if (trait->GetTraitFlags().Any(flags)) {
        traits.emplace_back(trait);
      }
    }
    return traits;
  }

 private:
  /**====================================
   * PRIVATE Init + Finalize Operations
   * ===================================*/

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
