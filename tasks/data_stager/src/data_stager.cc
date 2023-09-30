//
// Created by lukemartinlogan on 6/29/23.
//

#include "labstor_admin/labstor_admin.h"
#include "labstor/api/labstor_runtime.h"
#include "data_stager/data_stager.h"
#include "hermes_adapters/mapper/abstract_mapper.h"
#include "hermes_adapters/posix/posix_api.h"
#include "hermes_blob_mdm/hermes_blob_mdm.h"

namespace hermes::data_stager {

struct StagerInfo {
  std::string url_;
  int fd_;
  size_t page_size_;
};

class Server : public TaskLib {
 public:
  std::vector<std::unordered_map<hermes::BucketId, StagerInfo>> url_map_;
  blob_mdm::Client blob_mdm_;

 public:
  Server() = default;

  void Construct(ConstructTask *task, RunContext &rctx) {
    url_map_.resize(LABSTOR_QM_RUNTIME->max_lanes_);
    blob_mdm_.Init(task->blob_mdm_);
    task->SetModuleComplete();
  }

  void Destruct(DestructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }

  void StageIn(StageInTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }

  void StageOut(StageOutTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }

  void RegisterStager(RegisterStagerTask *task, RunContext &rctx) {
    BinaryRegisterStager(task, rctx);
  }

  void BinaryRegisterStager(RegisterStagerTask *task, RunContext &rctx) {
    url_map_[rctx.lane_id_].emplace(task->bkt_id_, StagerInfo());
    StagerInfo &stage_data = url_map_[rctx.lane_id_][task->bkt_id_];
    stage_data.url_ = task->url_->str();
    stage_data.fd_ = HERMES_POSIX_API->open(stage_data.url_.c_str(), O_RDWR);
    if (stage_data.fd_ < 0) {
      HELOG(kError, "Failed to open file {}", stage_data.url_);
    }
  }

  void BinaryStageIn(StageInTask *task, RunContext &rctx) {
    StagerInfo &stage_data = url_map_[rctx.lane_id_][task->bkt_id_];
    adapter::BlobPlacement plcmnt;
    plcmnt.DecodeBlobName(*task->blob_name_);
    HILOG(kDebug, "Attempting to stage {} bytes from the backend file {} at offset {}",
          stage_data.page_size_, stage_data.url_, plcmnt.bucket_off_);
    LPointer<char> blob = LABSTOR_CLIENT->AllocateBuffer(stage_data.page_size_);
    ssize_t real_size = HERMES_POSIX_API->pread(stage_data.fd_,
                                                blob.ptr_,
                                                stage_data.page_size_,
                                                (off_t)plcmnt.bucket_off_);
    if (real_size < 0) {
      HELOG(kError, "Failed to stage in {} bytes from {}",
            stage_data.page_size_, stage_data.url_);
    }
    memcpy(blob.ptr_ + plcmnt.blob_off_, blob.ptr_, real_size);
    HILOG(kDebug, "Staged {} bytes from the backend file {}",
          real_size, stage_data.url_);
    hapi::Context ctx;
    LPointer<blob_mdm::PutBlobTask> put_task =
        blob_mdm_.AsyncPutBlob(task->task_node_ + 1,
                           task->bkt_id_,
                           hshm::to_charbuf(*task->blob_name_),
                           hermes::BlobId::GetNull(),
                           0, real_size, blob.shm_, task->score_, bitfield32_t(0),
                           ctx, bitfield32_t(TASK_DATA_OWNER | TASK_LOW_LATENCY));
    put_task->Wait<TASK_YIELD_CO>(task);
    LABSTOR_CLIENT->DelTask(put_task);
  }

  void BinaryStageOut(StageOutTask *task, RunContext &rctx) {
    StagerInfo &stage_data = url_map_[rctx.lane_id_][task->bkt_id_];
    adapter::BlobPlacement plcmnt;
    plcmnt.DecodeBlobName(*task->blob_name_);
    HILOG(kDebug, "Attempting to stage {} bytes from the backend file {} at offset {}",
          stage_data.page_size_, stage_data.url_, plcmnt.bucket_off_);
    char *data = LABSTOR_CLIENT->GetPrivatePointer(task->data_);
    ssize_t real_size = HERMES_POSIX_API->pwrite(stage_data.fd_,
                                                 data,
                                                 task->data_size_,
                                                 (off_t)plcmnt.bucket_off_);
    if (real_size < 0) {
      HELOG(kError, "Failed to stage out {} bytes from {}",
            task->data_size_, stage_data.url_);
    }
    HILOG(kDebug, "Staged out {} bytes to the backend file {}",
          real_size, stage_data.url_);
  }
 public:
#include "data_stager/data_stager_lib_exec.h"
};

}  // namespace labstor::data_stager

LABSTOR_TASK_CC(hermes::data_stager::Server, "data_stager");
