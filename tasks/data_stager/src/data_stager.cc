//
// Created by lukemartinlogan on 6/29/23.
//

#include "hrun_admin/hrun_admin.h"
#include "hrun/api/hrun_runtime.h"
#include "data_stager/data_stager.h"
#include "hermes_adapters/mapper/abstract_mapper.h"
#include "hermes_adapters/posix/posix_api.h"
#include "hermes_blob_mdm/hermes_blob_mdm.h"
#include "data_stager/factory/stager_factory.h"

namespace hermes::data_stager {

class Server : public TaskLib {
 public:
  std::vector<std::unordered_map<hermes::BucketId, std::unique_ptr<AbstractStager>>> url_map_;
  blob_mdm::Client blob_mdm_;

 public:
  Server() = default;

  void Construct(ConstructTask *task, RunContext &rctx) {
    url_map_.resize(HRUN_QM_RUNTIME->max_lanes_);
    blob_mdm_.Init(task->blob_mdm_);
    task->SetModuleComplete();
  }

  void Destruct(DestructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }

  void RegisterStager(RegisterStagerTask *task, RunContext &rctx) {
    std::string url = task->url_->str();
    std::unique_ptr<AbstractStager> stager = StagerFactory::Get(url);
    stager->RegisterStager(task, rctx);
    url_map_[rctx.lane_id_].emplace(task->bkt_id_, std::move(stager));
    task->SetModuleComplete();
  }

  void UnregisterStager(UnregisterStagerTask *task, RunContext &rctx) {
    if (url_map_[rctx.lane_id_].find(task->bkt_id_) == url_map_[rctx.lane_id_].end()) {
      return;
    }
    url_map_[rctx.lane_id_].erase(task->bkt_id_);
    task->SetModuleComplete();
  }

  void StageIn(StageInTask *task, RunContext &rctx) {
    std::unordered_map<hermes::BucketId, std::unique_ptr<AbstractStager>>::iterator it =
        url_map_[rctx.lane_id_].find(task->bkt_id_);
    if (it == url_map_[rctx.lane_id_].end()) {
      HELOG(kError, "Could not find stager for bucket: {}", task->bkt_id_);
      task->SetModuleComplete();
      return;
    }
    std::unique_ptr<AbstractStager> &stager = it->second;
    HILOG(kInfo, "POINTER FAILING HERE?: {}", (size_t)stager.get());
    stager->StageIn(blob_mdm_, task, rctx);
    task->SetModuleComplete();
  }

  void StageOut(StageOutTask *task, RunContext &rctx) {
    std::unordered_map<hermes::BucketId, std::unique_ptr<AbstractStager>>::iterator it =
        url_map_[rctx.lane_id_].find(task->bkt_id_);
    if (it == url_map_[rctx.lane_id_].end()) {
      HELOG(kError, "Could not find stager for bucket: {}", task->bkt_id_);
      task->SetModuleComplete();
      return;
    }
    std::unique_ptr<AbstractStager> &stager = it->second;
    stager->StageOut(blob_mdm_, task, rctx);
    task->SetModuleComplete();
  }
 public:
#include "data_stager/data_stager_lib_exec.h"
};

}  // namespace hrun::data_stager

HRUN_TASK_CC(hermes::data_stager::Server, "data_stager");
