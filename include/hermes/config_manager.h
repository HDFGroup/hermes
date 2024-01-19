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

#ifndef HRUN_TASKS_HERMES_INCLUDE_hermes_H_
#define HRUN_TASKS_HERMES_INCLUDE_hermes_H_

#include "hermes/hermes_types.h"
#include "hrun_admin/hrun_admin.h"
#include "hermes_mdm/hermes_mdm.h"
#include "hermes_bucket_mdm/hermes_bucket_mdm.h"
#include "hermes_blob_mdm/hermes_blob_mdm.h"
#include "hermes_data_op/hermes_data_op.h"
#include "hermes/config_client.h"
#include "hermes/config_server.h"
#include "data_stager/data_stager.h"

namespace hermes {

class ConfigurationManager {
 public:
  mdm::Client mdm_;
  bucket_mdm::Client bkt_mdm_;
  blob_mdm::Client blob_mdm_;
  data_stager::Client stager_mdm_;
  data_op::Client op_mdm_;
  ServerConfig server_config_;
  ClientConfig client_config_;
  bool is_initialized_ = false;

 public:
  ConfigurationManager() = default;

  void ClientInit() {
    if (is_initialized_) {
      return;
    }
    // Create connection to MDM
    std::string config_path = "";
    LoadClientConfig(config_path);
    LoadServerConfig(config_path);
    mdm_.CreateRoot(DomainId::GetGlobal(), "hermes_mdm");
    blob_mdm_.CreateRoot(DomainId::GetGlobal(), "hermes_blob_mdm");
    bkt_mdm_.CreateRoot(DomainId::GetGlobal(), "hermes_bkt_mdm");
    op_mdm_.CreateRoot(DomainId::GetGlobal(), "hermes_op_mdm",
                       bkt_mdm_.id_, blob_mdm_.id_);
    stager_mdm_.CreateRoot(DomainId::GetGlobal(),
                           "hermes_stager_mdm",
                           blob_mdm_.id_,
                           bkt_mdm_.id_);
    blob_mdm_.SetBucketMdmRoot(DomainId::GetGlobal(),
                               bkt_mdm_.id_,
                               stager_mdm_.id_, op_mdm_.id_);
    bkt_mdm_.SetBlobMdmRoot(DomainId::GetGlobal(),
                            blob_mdm_.id_, stager_mdm_.id_);
    is_initialized_ = true;
  }

  void ServerInit() {
    ClientInit();
  }

  void LoadClientConfig(std::string config_path) {
    // Load hermes config
    if (config_path.empty()) {
      config_path = GetEnvSafe(Constant::kHermesClientConf);
    }
    HILOG(kInfo, "Loading client configuration: {}", config_path)
    client_config_.LoadFromFile(config_path);
  }

  void LoadServerConfig(std::string config_path) {
    // Load hermes config
    if (config_path.empty()) {
      config_path = GetEnvSafe(Constant::kHermesServerConf);
    }
    HILOG(kInfo, "Loading server configuration: {}", config_path)
    server_config_.LoadFromFile(config_path);
  }
};

}  // namespace hermes

#define HERMES_CONF \
hshm::Singleton<::hermes::ConfigurationManager>::GetInstance()
#define HERMES_CLIENT_CONF \
HERMES_CONF->client_config_
#define HERMES_SERVER_CONF \
HERMES_CONF->server_config_

/** Initialize client-side Hermes transparently */
static inline bool TRANSPARENT_HERMES() {
  if (TRANSPARENT_HRUN()) {
    HERMES_CONF->ClientInit();
    return true;
  }
  return false;
}

#endif  // HRUN_TASKS_HERMES_INCLUDE_hermes_H_
