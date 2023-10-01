//
// Created by lukemartinlogan on 7/9/23.
//

#ifndef LABSTOR_TASKS_HERMES_INCLUDE_hermes_H_
#define LABSTOR_TASKS_HERMES_INCLUDE_hermes_H_

#include "hermes/hermes_types.h"
#include "labstor_admin/labstor_admin.h"
#include "hermes_mdm/hermes_mdm.h"
#include "hermes_bucket_mdm/hermes_bucket_mdm.h"
#include "hermes_blob_mdm/hermes_blob_mdm.h"
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
    stager_mdm_.CreateRoot(DomainId::GetGlobal(), "hermes_stager_mdm", blob_mdm_.id_);
    blob_mdm_.SetBucketMdmRoot(DomainId::GetGlobal(), bkt_mdm_.id_, stager_mdm_.id_);
    bkt_mdm_.SetBlobMdmRoot(DomainId::GetGlobal(), blob_mdm_.id_, stager_mdm_.id_);
    is_initialized_ = true;
  }

  void LoadClientConfig(std::string &config_path) {
    // Load hermes config
    if (config_path.empty()) {
      config_path = GetEnvSafe(Constant::kHermesClientConf);
    }
    HILOG(kInfo, "Loading client configuration: {}", config_path)
    client_config_.LoadFromFile(config_path);
  }

  void LoadServerConfig(std::string &config_path) {
    // Load hermes config
    if (config_path.empty()) {
      config_path = GetEnvSafe(Constant::kHermesServerConf);
    }
    HILOG(kInfo, "Loading server configuration: {}", config_path)
    server_config_.LoadFromFile(config_path);
  }
};

}  // namespace hermes

#define HERMES_CONF hshm::Singleton<::hermes::ConfigurationManager>::GetInstance()
#define HERMES_CLIENT_CONF HERMES_CONF->client_config_
#define HERMES_SERVER_CONF HERMES_CONF->server_config_

/** Initialize client-side Hermes transparently */
static inline bool TRANSPARENT_HERMES() {
  if (TRANSPARENT_LABSTOR()) {
    HERMES_CONF->ClientInit();
    return true;
  }
  return false;
}

#endif  // LABSTOR_TASKS_HERMES_INCLUDE_hermes_H_
