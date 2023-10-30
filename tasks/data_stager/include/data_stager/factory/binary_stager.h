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
  int fd_;
  size_t page_size_;
  std::string path_;

 public:
  /** Default constructor */
  BinaryFileStager() = default;

  /** Destructor */
  ~BinaryFileStager() {
    HERMES_POSIX_API->close(fd_);
  }

  /** Build file url */
  static hshm::charbuf BuildFileUrl(const std::string &path, size_t page_size) {
    std::stringstream ss;
    ss << "file://" << path << ":" << page_size;
    return hshm::charbuf(ss.str());
  }

  /** Parse file url */
  static void ParseFileUrl(const std::string &url, std::string &path, size_t &page_size) {
    // Parse url
    std::string protocol, action;
    std::vector<std::string> tokens;
    Client::GetUrlProtocolAndAction(url, protocol, action, tokens);
    // file://[path]:[page_size]
    if (protocol == "file") {
      path = tokens[0];
      page_size = std::stoul(tokens[1]);
    }
  }

  /** Create the data stager payload */
  void RegisterStager(RegisterStagerTask *task, RunContext &rctx) override {
    ParseFileUrl(task->url_->str(), path_, page_size_);
    fd_ = HERMES_POSIX_API->open(path_.c_str(), O_RDWR);
    if (fd_ < 0) {
      HELOG(kError, "Failed to open file {}", path_);
    }
  }

  /** Stage data in from remote source */
  void StageIn(blob_mdm::Client &blob_mdm, StageInTask *task, RunContext &rctx) override {
    adapter::BlobPlacement plcmnt;
    plcmnt.DecodeBlobName(*task->blob_name_, page_size_);
    HILOG(kDebug, "Attempting to stage {} bytes from the backend file {} at offset {}",
          page_size_, url_, plcmnt.bucket_off_);
    LPointer<char> blob = HRUN_CLIENT->AllocateBuffer<TASK_YIELD_STD>(page_size_);
    ssize_t real_size = HERMES_POSIX_API->pread(fd_,
                                                blob.ptr_,
                                                page_size_,
                                                (off_t)plcmnt.bucket_off_);
    if (real_size < 0) {
      HELOG(kError, "Failed to stage in {} bytes from {}",
            page_size_, url_);
      return;
    }
    // memcpy(blob.ptr_ + plcmnt.blob_off_, blob.ptr_, real_size);
    HILOG(kDebug, "Staged {} bytes from the backend file {}",
          real_size, url_);
    HILOG(kDebug, "Submitting put blob {} ({}) to blob mdm ({})",
          task->blob_name_->str(), task->bkt_id_, blob_mdm.id_)
    hapi::Context ctx;
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
    adapter::BlobPlacement plcmnt;
    plcmnt.DecodeBlobName(*task->blob_name_, page_size_);
    HILOG(kDebug, "Attempting to stage {} bytes to the backend file {} at offset {}",
          page_size_, url_, plcmnt.bucket_off_);
    char *data = HRUN_CLIENT->GetDataPointer(task->data_);
    ssize_t real_size = HERMES_POSIX_API->pwrite(fd_,
                                                 data,
                                                 task->data_size_,
                                                 (off_t)plcmnt.bucket_off_);
    if (real_size < 0) {
      HELOG(kError, "Failed to stage out {} bytes from {}",
            task->data_size_, url_);
    }
    HILOG(kDebug, "Staged out {} bytes to the backend file {}",
          real_size, url_);
  }
};

}  // namespace hermes::data_stager

#endif  // HERMES_TASKS_DATA_STAGER_SRC_BINARY_STAGER_H_
