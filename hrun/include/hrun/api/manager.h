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

#ifndef HRUN_INCLUDE_HRUN_MANAGER_MANAGER_H_
#define HRUN_INCLUDE_HRUN_MANAGER_MANAGER_H_

#include "hrun/hrun_types.h"
#include "hrun/hrun_constants.h"
#include "hrun/config/config_client.h"
#include "hrun/config/config_server.h"
#include "hrun/queue_manager/queue_manager.h"

namespace hrun {

/** Shared-memory header for HRUN */
struct HrunShm {
  u32 node_id_;
  QueueManagerShm queue_manager_;
  std::atomic<u64> unique_;
  u64 num_nodes_;
};

/** The configuration used inherited by runtime + client */
class ConfigurationManager {
 public:
  HrunMode mode_;
  HrunShm *header_;
  ClientConfig client_config_;
  ServerConfig server_config_;
  static inline const hipc::allocator_id_t main_alloc_id_ =
      hipc::allocator_id_t(0, 1);
  static inline const hipc::allocator_id_t data_alloc_id_ =
      hipc::allocator_id_t(1, 1);
  static inline const hipc::allocator_id_t rdata_alloc_id_ =
      hipc::allocator_id_t(2, 1);
  hipc::Allocator *main_alloc_;
  hipc::Allocator *data_alloc_;
  hipc::Allocator *rdata_alloc_;
  bool is_being_initialized_;
  bool is_initialized_;
  bool is_terminated_;
  bool is_transparent_;
  hshm::Mutex lock_;
  hshm::ThreadType thread_type_;

  /** Default constructor */
  ConfigurationManager() : is_being_initialized_(false),
                           is_initialized_(false),
                           is_terminated_(false),
                           is_transparent_(false) {}

  /** Destructor */
  ~ConfigurationManager() {}

  /** Whether or not Hrun is currently being initialized */
  bool IsBeingInitialized() { return is_being_initialized_; }

  /** Whether or not Hrun is initialized */
  bool IsInitialized() { return is_initialized_; }

  /** Whether or not Hrun is finalized */
  bool IsTerminated() { return is_terminated_; }

  /** Load the server-side configuration */
  void LoadServerConfig(std::string config_path) {
    if (config_path.empty()) {
      config_path = Constants::GetEnvSafe(Constants::kServerConfEnv);
    }
    HILOG(kInfo, "Loading server configuration: {}", config_path);
    server_config_.LoadFromFile(config_path);
  }

  /** Load the client-side configuration */
  void LoadClientConfig(std::string config_path) {
    if (config_path.empty()) {
      config_path = Constants::GetEnvSafe(Constants::kClientConfEnv);
    }
    HILOG(kDebug, "Loading client configuration: {}", config_path);
    client_config_.LoadFromFile(config_path);
  }

  /** Get number of nodes */
  HSHM_ALWAYS_INLINE int GetNumNodes() {
    return server_config_.rpc_.host_names_.size();
  }
};

}  // namespace hrun

#endif  // HRUN_INCLUDE_HRUN_MANAGER_MANAGER_H_
