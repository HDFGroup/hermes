//
// Created by lukemartinlogan on 9/30/23.
//

#ifndef HERMES_TASKS_DATA_STAGER_SRC_ABSTRACT_STAGER_H_
#define HERMES_TASKS_DATA_STAGER_SRC_ABSTRACT_STAGER_H_

#include "../data_stager.h"
#include "hermes_bucket_mdm/hermes_bucket_mdm.h"

namespace hermes::data_stager {

class AbstractStager {
 public:
  std::string path_;
  std::string params_;

  AbstractStager() = default;
  ~AbstractStager() = default;

  virtual void RegisterStager(RegisterStagerTask *task, RunContext &rctx) = 0;
  virtual void StageIn(blob_mdm::Client &blob_mdm, StageInTask *task, RunContext &rctx) = 0;
  virtual void StageOut(blob_mdm::Client &blob_mdm, StageOutTask *task, RunContext &rctx) = 0;
  virtual void UpdateSize(bucket_mdm::Client &bkt_mdm, UpdateSizeTask *task, RunContext &rctx) = 0;
};

}  // namespace hermes

#endif  // HERMES_TASKS_DATA_STAGER_SRC_ABSTRACT_STAGER_H_
