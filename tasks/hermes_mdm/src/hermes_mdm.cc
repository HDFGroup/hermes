//
// Created by lukemartinlogan on 6/29/23.
//

#include "labstor_admin/labstor_admin.h"
#include "labstor/api/labstor_runtime.h"
#include "hermes/config_server.h"
#include "hermes_mdm/hermes_mdm.h"
#include "hermes_blob_mdm/hermes_blob_mdm.h"
#include "hermes_bucket_mdm/hermes_bucket_mdm.h"
#include "hermes/dpe/dpe_factory.h"
#include "bdev/bdev.h"

namespace hermes::mdm {

class Server : public TaskLib {
 public:
  /**====================================
   * Configuration
   * ===================================*/
   u32 node_id_;
   blob_mdm::Client blob_mdm_;
   bucket_mdm::Client bkt_mdm_;

 public:
  Server() = default;

  void Construct(ConstructTask *task, RunContext &rctx) {
    HILOG(kDebug, "ConstructTaskPhase::kLoadConfig")
    std::string config_path = task->server_config_path_->str();
    HERMES_CONF->LoadServerConfig(config_path);
    node_id_ = LABSTOR_CLIENT->node_id_;
    task->SetModuleComplete();
  }

  void Destruct(DestructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }

 public:
#include "hermes_mdm/hermes_mdm_lib_exec.h"
};

}  // namespace labstor

LABSTOR_TASK_CC(hermes::mdm::Server, "hermes_mdm");
