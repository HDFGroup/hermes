//
// Created by lukemartinlogan on 9/30/23.
//

#ifndef HERMES_TASKS_DATA_STAGER_SRC_BINARY_STAGER_H_
#define HERMES_TASKS_DATA_STAGER_SRC_BINARY_STAGER_H_

#include "abstract_stager.h"
#include "hermes_adapters/mapper/abstract_mapper.h"

namespace hermes::data_stager {

class BinaryFileStager : public AbstractStager {
 public:
  size_t page_size_;
  std::string path_;
  bitfield32_t flags_;

 public:
  /** Default constructor */
  BinaryFileStager() = default;

  /** Destructor */
  ~BinaryFileStager() {}

  /** Build context for staging */
  static Context BuildContext(size_t page_size,
                              u32 flags = 0,
                              size_t elmt_size = 1) {
    Context ctx;
    ctx.flags_.SetBits(HERMES_SHOULD_STAGE);
    ctx.bkt_params_ = BuildFileParams(page_size, flags, elmt_size);
    return ctx;
  }

  /** Build serialized file parameter pack */
  static std::string BuildFileParams(size_t page_size,
                                     u32 flags = 0,
                                     size_t elmt_size = 1) {
    hshm::charbuf params(32);
    page_size = (page_size / elmt_size) * elmt_size;
    hrun::LocalSerialize srl(params);
    srl << std::string("file");
    srl << flags;
    srl << page_size;
    return params.str();
  }

  /** Create the data stager payload */
  void RegisterStager(RegisterStagerTask *task, RunContext &rctx) override {
    std::string params = task->params_->str();
    std::string protocol;
    hrun::LocalDeserialize srl(params);
    srl >> protocol;
    srl >> flags_.bits_;
    srl >> page_size_;
    path_ = task->tag_name_->str();
  }

  /** Stage data in from remote source */
  void StageIn(blob_mdm::Client &blob_mdm, StageInTask *task, RunContext &rctx) override {
    if (flags_.Any(HERMES_STAGE_NO_READ)) {
      return;
    }
    adapter::BlobPlacement plcmnt;
    plcmnt.DecodeBlobName(*task->blob_name_, page_size_);
    HILOG(kDebug, "Attempting to stage {} bytes from the backend file {} at offset {}",
          page_size_, path_, plcmnt.bucket_off_);
    LPointer<char> blob = HRUN_CLIENT->AllocateBufferServer<TASK_YIELD_STD>(page_size_);
    int fd = HERMES_POSIX_API->open(path_.c_str(), O_CREAT | O_RDWR, 0666);
    if (fd < 0) {
      HELOG(kError, "Failed to open file {}", path_);
      HRUN_CLIENT->FreeBuffer(blob);
      return;
    }
    ssize_t real_size = HERMES_POSIX_API->pread(fd,
                                                blob.ptr_,
                                                page_size_,
                                                (off_t)plcmnt.bucket_off_);
    HERMES_POSIX_API->close(fd);
    if (real_size < 0) {
//      HELOG(kError, "Failed to stage in {} bytes from {}",
//            page_size_, path_);
      HRUN_CLIENT->FreeBuffer(blob);
      return;
    } else if (real_size == 0) {
      HRUN_CLIENT->FreeBuffer(blob);
      return;
    }
    HILOG(kDebug, "Staged {} bytes from the backend file {}",
          real_size, path_);
    HILOG(kDebug, "Submitting put blob {} ({}) to blob mdm ({})",
          task->blob_name_->str(), task->bkt_id_, blob_mdm.id_)
    hapi::Context ctx;
    ctx.flags_.SetBits(HERMES_SHOULD_STAGE);
    LPointer<blob_mdm::PutBlobTask> put_task =
        blob_mdm.AsyncPutBlob(task->task_node_ + 1,
                              task->bkt_id_,
                              hshm::to_charbuf(*task->blob_name_),
                              hermes::BlobId::GetNull(),
                              0, real_size, blob.shm_, task->score_, 0,
                              ctx, TASK_DATA_OWNER | TASK_LOW_LATENCY);
    put_task->Wait<TASK_YIELD_CO>(task);
    HRUN_CLIENT->DelTask(put_task);
  }

  /** Stage data out to remote source */
  void StageOut(blob_mdm::Client &blob_mdm, StageOutTask *task, RunContext &rctx) override {
    if (flags_.Any(HERMES_STAGE_NO_WRITE)) {
      return;
    }
    adapter::BlobPlacement plcmnt;
    plcmnt.DecodeBlobName(*task->blob_name_, page_size_);
    HILOG(kDebug, "Attempting to stage {} bytes to the backend file {} at offset {}",
          page_size_, path_, plcmnt.bucket_off_);
    char *data = HRUN_CLIENT->GetDataPointer(task->data_);
    int fd = HERMES_POSIX_API->open(path_.c_str(), O_CREAT | O_RDWR, 0666);
    if (fd < 0) {
      HELOG(kError, "Failed to open file {}", path_);
      return;
    }
    ssize_t real_size = HERMES_POSIX_API->pwrite(fd,
                                                 data,
                                                 task->data_size_,
                                                 (off_t)plcmnt.bucket_off_);
    HERMES_POSIX_API->close(fd);
    if (real_size < 0) {
      HELOG(kError, "Failed to stage out {} bytes from {}",
            task->data_size_, path_);
    }
    HILOG(kDebug, "Staged out {} bytes to the backend file {}",
          real_size, path_);
  }

  void UpdateSize(bucket_mdm::Client &bkt_mdm, UpdateSizeTask *task, RunContext &rctx) override {
    adapter::BlobPlacement p;
    std::string blob_name_str = task->blob_name_->str();
    p.DecodeBlobName(blob_name_str, page_size_);
    bkt_mdm.AsyncUpdateSize(task->task_node_ + 1,
                             task->bkt_id_,
                             p.bucket_off_ + task->blob_off_ + task->data_size_,
                             bucket_mdm::UpdateSizeMode::kCap);
  }
};

}  // namespace hermes::data_stager

#endif  // HERMES_TASKS_DATA_STAGER_SRC_BINARY_STAGER_H_
