//
// Created by lukemartinlogan on 6/29/23.
//
#include "labstor_admin/labstor_admin.h"
#include "labstor/api/labstor_runtime.h"
#include "hermes/config_server.h"
#include "hermes_blob_mdm/hermes_blob_mdm.h"
#include "hermes_adapters/mapper/mapper_factory.h"
#include "hermes/dpe/dpe_factory.h"
#include "hermes_adapters/posix/posix_api.h"
#include "bdev/bdev.h"

namespace hermes::blob_mdm {

/** Type name simplification for the various map types */
typedef std::unordered_map<hshm::charbuf, BlobId> BLOB_ID_MAP_T;
typedef std::unordered_map<BlobId, BlobInfo> BLOB_MAP_T;
typedef hipc::mpsc_queue<IoStat> IO_PATTERN_LOG_T;

class Server : public TaskLib {
 public:
  /**====================================
   * Configuration
   * ===================================*/
  u32 node_id_;

  /**====================================
   * Maps
   * ===================================*/
  BLOB_ID_MAP_T blob_id_map_;
  BLOB_MAP_T blob_map_;
  std::atomic<u64> id_alloc_;

  /**====================================
  * I/O pattern log
  * ===================================*/
  IO_PATTERN_LOG_T *io_pattern_log_;
  bool enable_io_tracing_;
  bool is_mpi_;

  /**====================================
   * Targets + devices
   * ===================================*/
  std::vector<bdev::ConstructTask*> target_tasks_;
  std::vector<bdev::Client> targets_;
  std::unordered_map<TargetId, TargetInfo*> target_map_;
  Client blob_mdm_;
  bucket_mdm::Client bkt_mdm_;

 public:
  Server() = default;

  void Construct(ConstructTask *task, RunContext &ctx) {
    id_alloc_ = 0;
    node_id_ = LABSTOR_CLIENT->node_id_;
    switch (task->phase_) {
      case ConstructTaskPhase::kCreateTaskStates: {
        target_tasks_.reserve(HERMES_SERVER_CONF.devices_.size());
        for (DeviceInfo &dev : HERMES_SERVER_CONF.devices_) {
          std::string dev_type;
          if (dev.mount_dir_.empty()) {
            dev_type = "ram_bdev";
            dev.mount_point_ = hshm::Formatter::format("{}/{}", dev.mount_dir_, dev.dev_name_);
          } else {
            dev_type = "posix_bdev";
          }
          targets_.emplace_back();
          bdev::Client &client = targets_.back();
          bdev::ConstructTask *create_task = client.AsyncCreate(
              task->task_node_ + 1,
              DomainId::GetLocal(),
              "hermes_" + dev.dev_name_,
              dev_type,
              dev).ptr_;
          target_tasks_.emplace_back(create_task);
        }
        task->phase_ = ConstructTaskPhase::kWaitForTaskStates;
      }

      case ConstructTaskPhase::kWaitForTaskStates: {
        for (int i = (int)target_tasks_.size() - 1; i >= 0; --i) {
          bdev::ConstructTask *tgt_task = target_tasks_[i];
          if (!tgt_task->IsComplete()) {
            return;
          }
          bdev::Client &client = targets_[i];
          client.AsyncCreateComplete(tgt_task);
          target_map_.emplace(client.id_, &client);
          target_tasks_.pop_back();
        }
        blob_mdm_.Init(id_);
      }
    }

    // Create targets
    HILOG(kInfo, "Created Blob MDM")
    task->SetModuleComplete();
  }

  void Destruct(DestructTask *task, RunContext &ctx) {
    task->SetModuleComplete();
  }

 private:
  /** Get the globally unique blob name */
  const hshm::charbuf GetBlobNameWithBucket(TagId tag_id, const hshm::charbuf &blob_name) {
    hshm::charbuf new_name(sizeof(TagId) + blob_name.size());
    labstor::LocalSerialize srl(new_name);
    srl << tag_id.node_id_;
    srl << tag_id.unique_;
    srl << blob_name;
    return new_name;
  }

 public:
  /**
   * Set the Bucket MDM
   * */
  void SetBucketMdm(SetBucketMdmTask *task, RunContext &ctx) {
    bkt_mdm_.Init(task->bkt_mdm_);
    task->SetModuleComplete();
  }

  /**
   * Create a blob's metadata
   * */
  void PutBlob(PutBlobTask *task, RunContext &ctx) {
    if (task->phase_ == PutBlobPhase::kCreate) {
      PutBlobCreatePhase(task, ctx);
    }
    if (task->phase_ == PutBlobPhase::kAllocate) {
      PutBlobAllocatePhase(task, ctx);
    }
    if (task->phase_ == PutBlobPhase::kWaitAllocate) {
      if (!task->cur_bdev_alloc_->IsComplete()){
        return;
      }
      PutBlobWaitAllocatePhase(task, ctx);
    }
    if (task->phase_ == PutBlobPhase::kModify) {
      PutBlobModifyPhase(task, ctx);
    }
    if (task->phase_ == PutBlobPhase::kWaitModify) {
      PutBlobWaitModifyPhase(task, ctx);
      if (task->phase_ == PutBlobPhase::kWaitModify) {
        return;
      }
    }
  }

  /** Create blob / update metadata for the PUT  */
  void PutBlobCreatePhase(PutBlobTask *task, RunContext &ctx) {
    HILOG(kDebug, "PutBlobPhase::kCreate {}", task->blob_id_);
    // Get the blob info data structure
    hshm::charbuf blob_name = hshm::to_charbuf(*task->blob_name_);
    auto it = blob_map_.find(task->blob_id_);
    if (it == blob_map_.end()) {
      task->flags_.SetBits(HERMES_BLOB_DID_CREATE);
    }
    if (task->flags_.Any(HERMES_BLOB_DID_CREATE)) {
      blob_map_.emplace(task->blob_id_, BlobInfo());
    }
    BlobInfo &blob_info = blob_map_[task->blob_id_];

    // Update the blob info
    if (task->flags_.Any(HERMES_BLOB_DID_CREATE)) {
      // Update blob info
      blob_info.name_ = std::move(blob_name);
      blob_info.blob_id_ = task->blob_id_;
      blob_info.tag_id_ = task->tag_id_;
      blob_info.blob_size_ = 0;
      blob_info.max_blob_size_ = 0;
      blob_info.score_ = task->score_;
      blob_info.mod_count_ = 0;
      blob_info.access_freq_ = 0;
      blob_info.last_flush_ = 0;
      blob_info.UpdateWriteStats();
    } else {
      // Modify existing blob
      blob_info.UpdateWriteStats();
    }
    if (task->flags_.Any(HERMES_BLOB_REPLACE)) {
      PutBlobFreeBuffersPhase(blob_info, task, ctx);
    }

    // Stage in blob data from FS
    task->data_ptr_.ptr_ = LABSTOR_CLIENT->GetPrivatePointer<char>(task->data_);
    task->data_ptr_.shm_ = task->data_;
    if (task->filename_->size() > 0) {
      adapter::BlobPlacement plcmnt;
      plcmnt.DecodeBlobName(*task->blob_name_);
      task->flags_.SetBits(HERMES_IS_FILE);
      task->data_off_ = plcmnt.bucket_off_ + task->blob_off_ + task->data_size_;
      if (blob_info.blob_size_ == 0 &&
          task->blob_off_ == 0 &&
          task->data_size_ < task->page_size_) {
        HILOG(kDebug, "Attempting to stage {} bytes from the backend file {} at offset {}",
              task->page_size_, task->filename_->str(), plcmnt.bucket_off_);
        LPointer<char> new_data_ptr = LABSTOR_CLIENT->AllocateBuffer(task->page_size_);
        int fd = HERMES_POSIX_API->open(task->filename_->c_str(), O_RDONLY);
        if (fd < 0) {
          HELOG(kError, "Failed to open file {}", task->filename_->str());
        }
        int ret = HERMES_POSIX_API->pread(fd, new_data_ptr.ptr_, task->page_size_, (off_t)plcmnt.bucket_off_);
        if (ret < 0) {
          // TODO(llogan): ret != page_size_ will require knowing file size before-hand
          HELOG(kError, "Failed to stage in {} bytes from {}", task->page_size_, task->filename_->str());
        }
        HERMES_POSIX_API->close(fd);
        memcpy(new_data_ptr.ptr_ + plcmnt.blob_off_, task->data_ptr_.ptr_, task->data_size_);
        task->data_ptr_ = new_data_ptr;
        task->blob_off_ = 0;
        if (ret < task->blob_off_ + task->data_size_) {
          task->data_size_ = task->blob_off_ + task->data_size_;
        } else {
          task->data_size_ = ret;
        }
        task->flags_.SetBits(HERMES_DID_STAGE_IN);
        HILOG(kDebug, "Staged {} bytes from the backend file {}",
              task->data_size_, task->filename_->str());
      }
    }

    // Determine amount of additional buffering space needed
    size_t needed_space = task->blob_off_ + task->data_size_;
    size_t size_diff = 0;
    if (needed_space > blob_info.max_blob_size_) {
      size_diff = needed_space - blob_info.max_blob_size_;
    }
    if (!task->flags_.Any(HERMES_IS_FILE)) {
      task->data_off_ = size_diff;
    }
    blob_info.blob_size_ += size_diff;
    HILOG(kDebug, "The size diff is {} bytes", size_diff)


    // Initialize archives
    HSHM_MAKE_AR0(task->schema_, nullptr);
    HSHM_MAKE_AR0(task->bdev_writes_, nullptr);

    // Use DPE
    if (size_diff > 0) {
      Context ctx;
      auto *dpe = DpeFactory::Get(ctx.dpe_);
      dpe->Placement({size_diff}, targets_, ctx, *task->schema_);
      task->phase_ = PutBlobPhase::kAllocate;
    } else {
      task->phase_ = PutBlobPhase::kModify;
    }
  }

  /** Release buffers */
  void PutBlobFreeBuffersPhase(BlobInfo &blob_info, PutBlobTask *task, RunContext &ctx) {
    for (BufferInfo &buf : blob_info.buffers_) {
      TargetInfo &target = *target_map_[buf.tid_];
      std::vector<BufferInfo> buf_vec = {buf};
      target.AsyncFree(task->task_node_ + 1, std::move(buf_vec), true);
    }
    blob_info.buffers_.clear();
    blob_info.max_blob_size_ = 0;
    blob_info.blob_size_ = 0;
  }

  /** Resolve the current sub-placement using BPM */
  void PutBlobAllocatePhase(PutBlobTask *task, RunContext &ctx) {
    BlobInfo &blob_info = blob_map_[task->blob_id_];
    PlacementSchema &schema = (*task->schema_)[task->plcmnt_idx_];
    SubPlacement &placement = schema.plcmnts_[task->sub_plcmnt_idx_];
    TargetInfo &bdev = *target_map_[placement.tid_];
    HILOG(kDebug, "Allocating {} bytes of blob {}", placement.size_, task->blob_id_);
    task->cur_bdev_alloc_ = bdev.AsyncAllocate(task->task_node_ + 1,
                                               placement.size_,
                                               blob_info.buffers_).ptr_;
    task->phase_ = PutBlobPhase::kWaitAllocate;
  }

  /** Wait for the current-subplacement to complete */
  void PutBlobWaitAllocatePhase(PutBlobTask *task, RunContext &ctx) {
    BlobInfo &blob_info = blob_map_[task->blob_id_];
    PlacementSchema &schema = (*task->schema_)[task->plcmnt_idx_];
    ++task->sub_plcmnt_idx_;
    if (task->sub_plcmnt_idx_ >= schema.plcmnts_.size()) {
      ++task->plcmnt_idx_;
      task->sub_plcmnt_idx_ = 0;
    }
    if (task->cur_bdev_alloc_->alloc_size_ < task->cur_bdev_alloc_->size_) {
      size_t diff = task->cur_bdev_alloc_->size_ - task->cur_bdev_alloc_->alloc_size_;
      PlacementSchema &schema = (*task->schema_)[task->plcmnt_idx_];
      SubPlacement &sub_plcmnt = schema.plcmnts_[task->sub_plcmnt_idx_];
      sub_plcmnt.size_ += diff;
      HILOG(kDebug, "Passing {} bytes to target {}", diff, sub_plcmnt.tid_);
    }
    LABSTOR_CLIENT->DelTask(task->cur_bdev_alloc_);
    if (task->plcmnt_idx_ < (*task->schema_).size()) {
      task->phase_ = PutBlobPhase::kAllocate;
      return;
    } else {
      task->phase_ = PutBlobPhase::kModify;
      task->bdev_writes_->reserve(blob_info.buffers_.size());
    }
  }

  /** Update the data on storage */
  void PutBlobModifyPhase(PutBlobTask *task, RunContext &ctx) {
    BlobInfo &blob_info = blob_map_[task->blob_id_];
    char *blob_buf = task->data_ptr_.ptr_;
    std::vector<bdev::WriteTask*> &write_tasks = *task->bdev_writes_;
    size_t blob_off = 0, buf_off = 0;
    HILOG(kDebug, "Number of buffers {}", blob_info.buffers_.size());
    for (BufferInfo &buf : blob_info.buffers_) {
      size_t blob_left = blob_off;
      size_t blob_right = blob_off + buf.t_size_;
      if (blob_left <= task->blob_off_ && task->blob_off_ < blob_right) {
        size_t rel_off = task->blob_off_ - blob_off;
        size_t tgt_off = buf.t_off_ + rel_off;
        size_t buf_size = buf.t_size_ - rel_off;
        if (blob_off + buf_size > task->blob_off_ + task->data_size_) {
          buf_size = task->blob_off_ + task->data_size_ - blob_off;
        }
        HILOG(kDebug, "Writing {} bytes at off {} from target {}", buf_size, tgt_off, buf.tid_)
        TargetInfo &target = *target_map_[buf.tid_];
        bdev::WriteTask *write_task = target.AsyncWrite(task->task_node_ + 1,
                                                        blob_buf + buf_off,
                                                        tgt_off, buf_size).ptr_;
        write_tasks.emplace_back(write_task);
        buf_off += buf_size;
      }
      blob_off += buf.t_size_;
    }
    if (blob_off < task->data_size_) {
      HELOG(kFatal, "Something annoying happened");
    }
    blob_info.max_blob_size_ = blob_off;
    task->phase_ = PutBlobPhase::kWaitModify;
    HILOG(kDebug, "Modified {} bytes of blob {}", blob_off, task->blob_id_);
  }

  /** Wait for the update to complete */
  void PutBlobWaitModifyPhase(PutBlobTask *task, RunContext &ctx) {
    std::vector<bdev::WriteTask*> &write_tasks = *task->bdev_writes_;
    for (int i = (int)write_tasks.size() - 1; i >= 0; --i) {
      bdev::WriteTask *write_task = write_tasks[i];
      if (!write_task->IsComplete()) {
        return;
      }
      LABSTOR_CLIENT->DelTask(write_task);
      write_tasks.pop_back();
    }
    HILOG(kDebug, "PutBlobTask complete");
    HSHM_DESTROY_AR(task->schema_);
    HSHM_DESTROY_AR(task->bdev_writes_);
    if (task->flags_.Any(HERMES_DID_STAGE_IN)) {
      LABSTOR_CLIENT->FreeBuffer(task->data_ptr_);
    }
    // Update the bucket statistics
    int update_mode = bucket_mdm::UpdateSizeMode::kAdd;
    if (task->flags_.Any(HERMES_IS_FILE)) {
      update_mode = bucket_mdm::UpdateSizeMode::kCap;
    }
    bkt_mdm_.AsyncUpdateSize(task->task_node_ + 1,
                             task->tag_id_,
                             task->data_off_,
                             update_mode);
    if (task->flags_.Any(HERMES_BLOB_DID_CREATE)) {
      bkt_mdm_.AsyncTagAddBlob(task->task_node_ + 1,
                               task->tag_id_,
                               task->blob_id_);
    }
    task->SetModuleComplete();
  }

  /** Get a blob's data */
  void GetBlob(GetBlobTask *task, RunContext &ctx) {
    switch (task->phase_) {
      case GetBlobPhase::kStart: {
        GetBlobGetPhase(task, ctx);
      }
      case GetBlobPhase::kWait: {
        GetBlobWaitPhase(task, ctx);
      }
    }
  }

  void GetBlobGetPhase(GetBlobTask *task, RunContext &ctx) {
    BlobInfo &blob_info = blob_map_[task->blob_id_];
    HSHM_MAKE_AR0(task->bdev_reads_, nullptr);
    std::vector<bdev::ReadTask*> &read_tasks = *task->bdev_reads_;
    read_tasks.reserve(blob_info.buffers_.size());
    if (task->data_size_ < 0) {
      task->data_size_ = (ssize_t)(blob_info.blob_size_ - task->blob_off_);
    }
    HILOG(kDebug, "Getting blob {} of size {} starting at offset {} (total_blob_size={}, buffers={})",
          task->blob_id_, task->data_size_, task->blob_off_, blob_info.blob_size_, blob_info.buffers_.size());
    size_t blob_off = 0, buf_off = 0;
    hipc::mptr<char> blob_data_mptr(task->data_);
    char *blob_buf = blob_data_mptr.get();
    for (BufferInfo &buf : blob_info.buffers_) {
      size_t blob_left = blob_off;
      size_t blob_right = blob_off + buf.t_size_;
      if (blob_left <= task->blob_off_ && task->blob_off_ < blob_right) {
        size_t rel_off = task->blob_off_ - blob_off;
        size_t tgt_off = buf.t_off_ + rel_off;
        size_t buf_size = buf.t_size_ - rel_off;
        if (blob_off + buf_size > task->blob_off_ + task->data_size_) {
          buf_size = task->blob_off_ + task->data_size_ - blob_off;
        }
        HILOG(kDebug, "Loading {} bytes at off {} from target {}", buf_size, tgt_off, buf.tid_)
        TargetInfo &target = *target_map_[buf.tid_];
        bdev::ReadTask *read_task = target.AsyncRead(task->task_node_ + 1,
                                                     blob_buf + buf_off,
                                                     tgt_off, buf_size).ptr_;
        read_tasks.emplace_back(read_task);
        buf_off += buf_size;
      }
      blob_off += buf.t_size_;
    }
    task->phase_ = GetBlobPhase::kWait;
  }

  void GetBlobWaitPhase(GetBlobTask *task, RunContext &ctx) {
    std::vector<bdev::ReadTask*> &read_tasks = *task->bdev_reads_;
    for (auto it = read_tasks.rbegin(); it != read_tasks.rend(); ++it) {
      bdev::ReadTask *read_task = *it;
      if (!read_task->IsComplete()) {
        return;
      }
      LABSTOR_CLIENT->DelTask(read_task);
      read_tasks.pop_back();
    }
    HSHM_DESTROY_AR(task->bdev_reads_);
    HILOG(kDebug, "GetBlobTask complete");
    task->SetModuleComplete();
  }

  /**
   * Tag a blob
   * */
  void TagBlob(TagBlobTask *task, RunContext &ctx) {
    auto it = blob_map_.find(task->blob_id_);
    if (it == blob_map_.end()) {
      task->SetModuleComplete();
      return;
    }
    BlobInfo &blob = it->second;
    blob.tags_.push_back(task->tag_);
    task->SetModuleComplete();
  }

  /**
   * Check if blob has a tag
   * */
  void BlobHasTag(BlobHasTagTask *task, RunContext &ctx) {
    auto it = blob_map_.find(task->blob_id_);
    if (it == blob_map_.end()) {
      task->SetModuleComplete();
      return;
    }
    BlobInfo &blob = it->second;
    task->has_tag_ = std::find(blob.tags_.begin(),
                               blob.tags_.end(),
                               task->tag_) != blob.tags_.end();
    task->SetModuleComplete();
  }

  /**
   * Create \a blob_id BLOB ID
   * */
  void GetOrCreateBlobId(GetOrCreateBlobIdTask *task, RunContext &ctx) {
    hshm::charbuf blob_name = hshm::to_charbuf(*task->blob_name_);
    hshm::charbuf blob_name_unique = GetBlobNameWithBucket(task->tag_id_, blob_name);
    auto it = blob_id_map_.find(blob_name_unique);
    if (it == blob_id_map_.end()) {
      task->blob_id_ = BlobId(node_id_, id_alloc_.fetch_add(1));
      blob_id_map_.emplace(blob_name_unique, task->blob_id_);
      task->SetModuleComplete();
      return;
    }
    task->blob_id_ = it->second;
    task->SetModuleComplete();
  }

  /**
   * Get \a blob_name BLOB from \a bkt_id bucket
   * */
  HSHM_ALWAYS_INLINE
  void GetBlobId(GetBlobIdTask *task, RunContext &ctx) {
    hshm::charbuf blob_name = hshm::to_charbuf(*task->blob_name_);
    hshm::charbuf blob_name_unique = GetBlobNameWithBucket(task->tag_id_, blob_name);
    auto it = blob_id_map_.find(blob_name_unique);
    if (it == blob_id_map_.end()) {
      task->blob_id_ = BlobId::GetNull();
      task->SetModuleComplete();
      HILOG(kDebug, "Failed to find blob {} in {}", blob_name.str(), task->tag_id_);
      return;
    }
    task->blob_id_ = it->second;
    HILOG(kDebug, "Found blob {} / {} in {}", task->blob_id_, blob_name.str(), task->tag_id_);
    task->SetModuleComplete();
  }

  /**
   * Get \a blob_name BLOB name from \a blob_id BLOB id
   * */
  void GetBlobName(GetBlobNameTask *task, RunContext &ctx) {
    auto it = blob_map_.find(task->blob_id_);
    if (it == blob_map_.end()) {
      task->SetModuleComplete();
      return;
    }
    BlobInfo &blob = it->second;
    (*task->blob_name_) = blob.name_;
    task->SetModuleComplete();
  }

  /**
   * Get \a score from \a blob_id BLOB id
   * */
  void GetBlobSize(GetBlobSizeTask *task, RunContext &ctx) {
    auto it = blob_map_.find(task->blob_id_);
    if (it == blob_map_.end()) {
      task->SetModuleComplete();
      return;
    }
    BlobInfo &blob = it->second;
    task->size_ = blob.blob_size_;
    task->SetModuleComplete();
  }

  /**
   * Get \a score from \a blob_id BLOB id
   * */
  void GetBlobScore(GetBlobScoreTask *task, RunContext &ctx) {
    auto it = blob_map_.find(task->blob_id_);
    if (it == blob_map_.end()) {
      task->SetModuleComplete();
      return;
    }
    BlobInfo &blob = it->second;
    task->score_ = blob.score_;
    task->SetModuleComplete();
  }

  /**
   * Get \a blob_id blob's buffers
   * */
  void GetBlobBuffers(GetBlobBuffersTask *task, RunContext &ctx) {
    auto it = blob_map_.find(task->blob_id_);
    if (it == blob_map_.end()) {
      task->SetModuleComplete();
      return;
    }
    BlobInfo &blob = it->second;
    (*task->buffers_) = blob.buffers_;
    task->SetModuleComplete();
  }

  /**
   * Rename \a blob_id blob to \a new_blob_name new blob name
   * in \a bkt_id bucket.
   * */
  void RenameBlob(RenameBlobTask *task, RunContext &ctx) {
    auto it = blob_map_.find(task->blob_id_);
    if (it == blob_map_.end()) {
      task->SetModuleComplete();
      return;
    }
    BlobInfo &blob = it->second;
    blob_id_map_.erase(blob.name_);
    blob_id_map_[blob.name_] = task->blob_id_;
    blob.name_ = hshm::to_charbuf(*task->new_blob_name_);
    task->SetModuleComplete();
  }

  /**
   * Truncate a blob to a new size
   * */
  void TruncateBlob(TruncateBlobTask *task, RunContext &ctx) {
    auto it = blob_map_.find(task->blob_id_);
    if (it == blob_map_.end()) {
      task->SetModuleComplete();
      return;
    }
    BlobInfo &blob_info = it->second;
    // TODO(llogan): truncate blob
    task->SetModuleComplete();
  }

  /**
   * Destroy \a blob_id blob in \a bkt_id bucket
   * */
  void DestroyBlob(DestroyBlobTask *task, RunContext &ctx) {
    switch (task->phase_) {
      case DestroyBlobPhase::kFreeBuffers: {
        auto it = blob_map_.find(task->blob_id_);
        if (it == blob_map_.end()) {
          task->SetModuleComplete();
          return;
        }
        BlobInfo &blob_info = it->second;
        hshm::charbuf unique_name = GetBlobNameWithBucket(blob_info.tag_id_, blob_info.name_);
        blob_id_map_.erase(unique_name);
        HSHM_MAKE_AR0(task->free_tasks_, nullptr);
        task->free_tasks_->reserve(blob_info.buffers_.size());
        for (BufferInfo &buf : blob_info.buffers_) {
          TargetInfo &tgt_info = *target_map_[buf.tid_];
          std::vector<BufferInfo> buf_vec = {buf};
          bdev::FreeTask *free_task = tgt_info.AsyncFree(
              task->task_node_ + 1, std::move(buf_vec), false).ptr_;
          task->free_tasks_->emplace_back(free_task);
        }
        task->phase_ = DestroyBlobPhase::kWaitFreeBuffers;
      }
      case DestroyBlobPhase::kWaitFreeBuffers: {
        std::vector<bdev::FreeTask *> &free_tasks = *task->free_tasks_;
        for (auto it = free_tasks.rbegin(); it != free_tasks.rend(); ++it) {
          bdev::FreeTask *free_task = *it;
          if (!free_task->IsComplete()) {
            return;
          }
          LABSTOR_CLIENT->DelTask(free_task);
          free_tasks.pop_back();
        }
        BlobInfo &blob_info = blob_map_[task->blob_id_];
        bkt_mdm_.AsyncUpdateSize(task->task_node_ + 1,
                                 task->tag_id_,
                                 -(ssize_t)blob_info.blob_size_,
                                 bucket_mdm::UpdateSizeMode::kAdd);
        HSHM_DESTROY_AR(task->free_tasks_);
        blob_map_.erase(task->blob_id_);
        task->SetModuleComplete();
      }
    }
  }

  /**
   * Reorganize \a blob_id blob in \a bkt_id bucket
   * */
  void ReorganizeBlob(ReorganizeBlobTask *task, RunContext &ctx) {
    switch (task->phase_) {
      case ReorganizeBlobPhase::kGet: {
        auto it = blob_map_.find(task->blob_id_);
        if (it == blob_map_.end()) {
          task->SetModuleComplete();
          return;
        }
        BlobInfo &blob_info = it->second;
        task->data_ = LABSTOR_CLIENT->AllocateBuffer(blob_info.blob_size_).shm_;
        task->data_size_ = blob_info.blob_size_;
        task->get_task_ = blob_mdm_.AsyncGetBlob(task->task_node_ + 1,
                                                 task->tag_id_,
                                                 task->blob_id_,
                                                 0,
                                                 task->data_size_,
                                                 task->data_).ptr_;
        task->tag_id_ = blob_info.tag_id_;
        task->phase_ = ReorganizeBlobPhase::kWaitGet;
      }
      case ReorganizeBlobPhase::kWaitGet: {
        if (!task->get_task_->IsComplete()) {
          return;
        }
        LABSTOR_CLIENT->DelTask(task->get_task_);
        task->phase_ = ReorganizeBlobPhase::kPut;
      }
      case ReorganizeBlobPhase::kPut: {
        task->put_task_ = blob_mdm_.AsyncPutBlob(task->task_node_ + 1,
                                                 task->tag_id_, hshm::charbuf(""),
                                                 task->blob_id_, 0,
                                                 task->data_size_,
                                                 task->data_,
                                                 task->score_,
                                                 bitfield32_t(HERMES_BLOB_REPLACE)).ptr_;
        task->SetModuleComplete();
      }
    }
  }

 public:
#include "hermes_blob_mdm/hermes_blob_mdm_lib_exec.h"
};

}  // namespace labstor

LABSTOR_TASK_CC(hermes::blob_mdm::Server, "hermes_blob_mdm");
