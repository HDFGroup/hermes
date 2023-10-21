//
// Created by lukemartinlogan on 6/29/23.
//
#include "hrun_admin/hrun_admin.h"
#include "hrun/api/hrun_runtime.h"
#include "hermes/config_server.h"
#include "hermes_blob_mdm/hermes_blob_mdm.h"
#include "hermes_adapters/mapper/mapper_factory.h"
#include "hermes/dpe/dpe_factory.h"
#include "hermes_adapters/posix/posix_api.h"
#include "bdev/bdev.h"
#include "data_stager/data_stager.h"
#include "hermes_data_op/hermes_data_op.h"
#include "hermes/score_histogram.h"

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
  std::vector<BLOB_ID_MAP_T> blob_id_map_;
  std::vector<BLOB_MAP_T> blob_map_;
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
  data_stager::Client stager_mdm_;
  data_op::Client op_mdm_;
  LPointer<FlushDataTask> flush_task_;

 public:
  Server() = default;

  void Construct(ConstructTask *task, RunContext &rctx) {
    id_alloc_ = 0;
    node_id_ = HRUN_CLIENT->node_id_;
    // Initialize blob maps
    blob_id_map_.resize(HRUN_QM_RUNTIME->max_lanes_);
    blob_map_.resize(HRUN_QM_RUNTIME->max_lanes_);
    // Initialize targets
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
    for (int i = 0; i < target_tasks_.size(); ++i) {
      bdev::ConstructTask *tgt_task = target_tasks_[i];
      tgt_task->Wait<TASK_YIELD_CO>(task);
      bdev::Client &client = targets_[i];
      client.AsyncCreateComplete(tgt_task);
      target_map_.emplace(client.id_, &client);
    }
    blob_mdm_.Init(id_);
    HILOG(kInfo, "(node {}) Created Blob MDM", HRUN_CLIENT->node_id_);
    task->SetModuleComplete();
  }

  void Destruct(DestructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }

 private:
  /** Get the globally unique blob name */
  const hshm::charbuf GetBlobNameWithBucket(TagId tag_id, const hshm::charbuf &blob_name) {
    hshm::charbuf new_name(sizeof(TagId) + blob_name.size());
    hrun::LocalSerialize srl(new_name);
    srl << tag_id.node_id_;
    srl << tag_id.unique_;
    srl << blob_name;
    return new_name;
  }

 public:
  /**
   * Set the Bucket MDM
   * */
  void SetBucketMdm(SetBucketMdmTask *task, RunContext &rctx) {
    if (bkt_mdm_.id_.IsNull()) {
      bkt_mdm_.Init(task->bkt_mdm_);
      stager_mdm_.Init(task->stager_mdm_);
      op_mdm_.Init(task->op_mdm_);
      // TODO(llogan): Add back
      flush_task_ = blob_mdm_.AsyncFlushData(task->task_node_ + 1);
    }
    task->SetModuleComplete();
  }

  /** New score */
  float MakeScore(BlobInfo &blob_info, hshm::Timepoint &now) {
    float freq_score = blob_info.access_freq_ / 5;
    float access_score = (float)(1 - (blob_info.last_access_.GetSecFromStart(now) / 5));
    if (freq_score > 1) {
      freq_score = 1;
    }
    if (access_score > 1) {
      access_score = 1;
    }
    float data_score = std::max(freq_score, access_score);
    float user_score = blob_info.user_score_;
    if (!blob_info.flags_.Any(HERMES_USER_SCORE_STATIONARY)) {
      user_score *= data_score;
    }
    return std::max(data_score, user_score);
  }

  /** Check if blob should be reorganized */
  template<bool UPDATE_SCORE=false>
  bool ShouldReorganize(BlobInfo &blob_info,
                        float score,
                        TaskNode &task_node) {
//    for (BufferInfo &buf : blob_info.buffers_) {
//      TargetInfo &target = *target_map_[buf.tid_];
//      Histogram &hist = target.monitor_task_->score_hist_;
//      if constexpr(UPDATE_SCORE) {
//        target.AsyncUpdateScore(task_node + 1,
//                                blob_info.score_, score);
//      }
//      u32 percentile = hist.GetPercentile(score);
//      size_t rem_cap = target.monitor_task_->rem_cap_;
//      size_t max_cap = target.max_cap_;
//      if (rem_cap < max_cap / 10) {
//        if (percentile < 10 || percentile > 90) {
//          return true;
//        }
//      }
//    }
    return false;
  }

  /**
   * Long-running task to stage out data periodically and
   * reorganize blobs
   * */
  void FlushData(FlushDataTask *task, RunContext &rctx) {
    hshm::Timepoint now;
    now.Now();
    // Get the blob info data structure
    BLOB_MAP_T &blob_map = blob_map_[rctx.lane_id_];
    for (auto &it : blob_map) {
      BlobInfo &blob_info = it.second;
      // Update blob scores
      // TODO(llogan): Add back
//      float new_score = MakeScore(blob_info, now);
//      if (ShouldReorganize<true>(blob_info, new_score, task->task_node_)) {
//        blob_mdm_.AsyncReorganizeBlob(task->task_node_ + 1,
//                                      blob_info.tag_id_,
//                                      blob_info.blob_id_,
//                                      new_score, 0, false);
//      }
//      blob_info.access_freq_ = 0;
//      blob_info.score_ = new_score;

      // Flush data
      if (blob_info.last_flush_ > 0 &&
          blob_info.mod_count_ > blob_info.last_flush_) {
        HILOG(kDebug, "Flushing blob {} (mod_count={}, last_flush={})",
              blob_info.blob_id_, blob_info.mod_count_, blob_info.last_flush_);
        blob_info.last_flush_ = 1;
        blob_info.mod_count_ = 0;
        blob_info.UpdateWriteStats();
        LPointer<char> data = HRUN_CLIENT->AllocateBuffer(blob_info.blob_size_);
        LPointer<GetBlobTask> get_blob =
            blob_mdm_.AsyncGetBlob(task->task_node_ + 1,
                                   blob_info.tag_id_,
                                   blob_info.name_,
                                   blob_info.blob_id_,
                                   0, blob_info.blob_size_,
                                   data.shm_);
        get_blob->Wait<TASK_YIELD_CO>(task);
        stager_mdm_.AsyncStageOut(task->task_node_ + 1,
                                  blob_info.tag_id_,
                                  blob_info.name_,
                                  data.shm_, blob_info.blob_size_,
                                  TASK_DATA_OWNER | TASK_FIRE_AND_FORGET);
      }
    }
  }

  /**
   * Create a blob's metadata
   * */
  void PutBlob(PutBlobTask *task, RunContext &rctx) {
    // Get the blob info data structure
    hshm::charbuf blob_name = hshm::to_charbuf(*task->blob_name_);
    if (task->blob_id_.IsNull()) {
      task->blob_id_ = GetOrCreateBlobId(task->tag_id_, task->lane_hash_,
                                         blob_name, rctx, task->flags_);
    }
    HILOG(kDebug, "Beginning PUT for {}", blob_name.str());
    BLOB_MAP_T &blob_map = blob_map_[rctx.lane_id_];
    BlobInfo &blob_info = blob_map[task->blob_id_];

    // Update the blob info
    if (task->flags_.Any(HERMES_BLOB_DID_CREATE)) {
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
      if (task->flags_.Any(HERMES_IS_FILE)) {
        blob_info.mod_count_ = 1;
        blob_info.last_flush_ = 1;
        LPointer<data_stager::StageInTask> stage_task =
            stager_mdm_.AsyncStageIn(task->task_node_ + 1,
                                     task->tag_id_,
                                     blob_info.name_,
                                     task->score_, 0);
        stage_task->Wait<TASK_YIELD_CO>(task);
        HRUN_CLIENT->DelTask(stage_task);
      }
    } else {
      // Modify existing blob
      blob_info.UpdateWriteStats();
    }
    if (task->flags_.Any(HERMES_BLOB_REPLACE)) {
      PutBlobFreeBuffersPhase(blob_info, task, rctx);
    }

    // Determine amount of additional buffering space needed
    size_t needed_space = task->blob_off_ + task->data_size_;
    size_t size_diff = 0;
    if (needed_space > blob_info.max_blob_size_) {
      size_diff = needed_space - blob_info.max_blob_size_;
    }
    blob_info.blob_size_ += size_diff;
    HILOG(kDebug, "The size diff is {} bytes", size_diff)

    // Use DPE
    std::vector<PlacementSchema> schema_vec;
    if (size_diff > 0) {
      Context ctx;
      auto *dpe = DpeFactory::Get(ctx.dpe_);
      dpe->Placement({size_diff}, targets_, ctx, schema_vec);
    }

    // Allocate blob buffers
    for (PlacementSchema &schema : schema_vec) {
      for (size_t sub_idx = 0; sub_idx < schema.plcmnts_.size(); ++sub_idx) {
        SubPlacement &placement = schema.plcmnts_[sub_idx];
        TargetInfo &bdev = *target_map_[placement.tid_];
        LPointer<bdev::AllocateTask> alloc_task =
            bdev.AsyncAllocate(task->task_node_ + 1,
                               blob_info.score_,
                               placement.size_,
                               blob_info.buffers_);
        alloc_task->Wait<TASK_YIELD_CO>(task);
        if (alloc_task->alloc_size_ < alloc_task->size_) {
          // SubPlacement &next_placement = schema.plcmnts_[sub_idx + 1];
          // size_t diff = alloc_task->size_ - alloc_task->alloc_size_;
          // next_placement.size_ += diff;
          HELOG(kFatal, "Ran outta space in this tier -- will fix soon")
        }
        HRUN_CLIENT->DelTask(alloc_task);
      }
    }

    // Place blob in buffers
    std::vector<LPointer<bdev::WriteTask>> write_tasks;
    write_tasks.reserve(blob_info.buffers_.size());
    size_t blob_off = 0, buf_off = 0;
    char *blob_buf = HRUN_CLIENT->GetPrivatePointer(task->data_);
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
        LPointer<bdev::WriteTask> write_task =
            target.AsyncWrite(task->task_node_ + 1,
                              blob_buf + buf_off,
                              tgt_off, buf_size);
        write_tasks.emplace_back(write_task);
        buf_off += buf_size;
      }
      blob_off += buf.t_size_;
    }
    blob_info.max_blob_size_ = blob_off;

    // Wait for the placements to complete
    for (LPointer<bdev::WriteTask> &write_task : write_tasks) {
      write_task->Wait<TASK_YIELD_CO>(task);
      HRUN_CLIENT->DelTask(write_task);
    }

    // Update information
    int update_mode = bucket_mdm::UpdateSizeMode::kAdd;
    if (task->flags_.Any(HERMES_IS_FILE)) {
      update_mode = bucket_mdm::UpdateSizeMode::kCap;
    }
    bkt_mdm_.AsyncUpdateSize(task->task_node_ + 1,
                             task->tag_id_,
                             task->blob_off_ + task->data_size_,
                             update_mode);
    if (task->flags_.Any(HERMES_BLOB_DID_CREATE)) {
      bkt_mdm_.AsyncTagAddBlob(task->task_node_ + 1,
                               task->tag_id_,
                               task->blob_id_);
    }
    if (task->flags_.Any(HERMES_HAS_DERIVED)) {
      op_mdm_.AsyncRegisterData(task->task_node_ + 1,
                                task->tag_id_,
                                task->blob_name_->str(),
                                task->blob_id_,
                                task->blob_off_,
                                task->data_size_);
    }

    // Free data
    HILOG(kDebug, "Completing PUT for {}", blob_name.str());
    task->SetModuleComplete();
  }

  /** Release buffers */
  void PutBlobFreeBuffersPhase(BlobInfo &blob_info, PutBlobTask *task, RunContext &rctx) {
    for (BufferInfo &buf : blob_info.buffers_) {
      TargetInfo &target = *target_map_[buf.tid_];
      std::vector<BufferInfo> buf_vec = {buf};
//      target.AsyncFree(task->task_node_ + 1,
//                       blob_info.score_,
//                       std::move(buf_vec), true);
    }
    blob_info.buffers_.clear();
    blob_info.max_blob_size_ = 0;
    blob_info.blob_size_ = 0;
  }

  /** Get a blob's data */
  void GetBlob(GetBlobTask *task, RunContext &rctx) {
    switch (task->phase_) {
      case GetBlobPhase::kStart: {
        GetBlobGetPhase(task, rctx);
      }
      case GetBlobPhase::kWait: {
        GetBlobWaitPhase(task, rctx);
      }
    }
  }

  void GetBlobGetPhase(GetBlobTask *task, RunContext &rctx) {
    if (task->blob_id_.IsNull()) {
      hshm::charbuf blob_name = hshm::to_charbuf(*task->blob_name_);
      task->blob_id_ = GetOrCreateBlobId(task->tag_id_, task->lane_hash_,
                                         blob_name, rctx, task->flags_);
    }
    BLOB_MAP_T &blob_map = blob_map_[rctx.lane_id_];
    BlobInfo &blob_info = blob_map[task->blob_id_];
    HSHM_MAKE_AR0(task->bdev_reads_, nullptr);
    std::vector<bdev::ReadTask*> &read_tasks = *task->bdev_reads_;
    read_tasks.reserve(blob_info.buffers_.size());
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

  void GetBlobWaitPhase(GetBlobTask *task, RunContext &rctx) {
    std::vector<bdev::ReadTask*> &read_tasks = *task->bdev_reads_;
    for (bdev::ReadTask *&read_task : read_tasks) {
      if (!read_task->IsComplete()) {
        return;
      }
    }
    for (bdev::ReadTask *&read_task : read_tasks) {
      HRUN_CLIENT->DelTask(read_task);
    }
    HSHM_DESTROY_AR(task->bdev_reads_);
    HILOG(kDebug, "GetBlobTask complete");
    task->SetModuleComplete();
  }

  /**
   * Tag a blob
   * */
  void TagBlob(TagBlobTask *task, RunContext &rctx) {
    BLOB_MAP_T &blob_map = blob_map_[rctx.lane_id_];
    auto it = blob_map.find(task->blob_id_);
    if (it == blob_map.end()) {
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
  void BlobHasTag(BlobHasTagTask *task, RunContext &rctx) {
    BLOB_MAP_T &blob_map = blob_map_[rctx.lane_id_];
    auto it = blob_map.find(task->blob_id_);
    if (it == blob_map.end()) {
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
  BlobId GetOrCreateBlobId(TagId &tag_id, u32 lane_hash,
                           const hshm::charbuf &blob_name, RunContext &rctx,
                           bitfield32_t &flags) {
    hshm::charbuf blob_name_unique = GetBlobNameWithBucket(tag_id, blob_name);
    BLOB_ID_MAP_T &blob_id_map = blob_id_map_[rctx.lane_id_];
    auto it = blob_id_map.find(blob_name_unique);
    if (it == blob_id_map.end()) {
      BlobId blob_id = BlobId(node_id_, lane_hash, id_alloc_.fetch_add(1));
      blob_id_map.emplace(blob_name_unique, blob_id);
      flags.SetBits(HERMES_BLOB_DID_CREATE);
      BLOB_MAP_T &blob_map = blob_map_[rctx.lane_id_];
      blob_map.emplace(blob_id, BlobInfo());
      return blob_id;
    }
    return it->second;
  }
  void GetOrCreateBlobId(GetOrCreateBlobIdTask *task, RunContext &rctx) {
    hshm::charbuf blob_name = hshm::to_charbuf(*task->blob_name_);
    bitfield32_t flags;
    task->blob_id_ = GetOrCreateBlobId(task->tag_id_, task->lane_hash_, blob_name, rctx, flags);
    task->SetModuleComplete();
  }

  /**
   * Get \a blob_name BLOB from \a bkt_id bucket
   * */
  HSHM_ALWAYS_INLINE
  void GetBlobId(GetBlobIdTask *task, RunContext &rctx) {
    hshm::charbuf blob_name = hshm::to_charbuf(*task->blob_name_);
    hshm::charbuf blob_name_unique = GetBlobNameWithBucket(task->tag_id_, blob_name);
    BLOB_ID_MAP_T &blob_id_map = blob_id_map_[rctx.lane_id_];
    auto it = blob_id_map.find(blob_name_unique);
    if (it == blob_id_map.end()) {
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
  void GetBlobName(GetBlobNameTask *task, RunContext &rctx) {
    BLOB_MAP_T &blob_map = blob_map_[rctx.lane_id_];
    auto it = blob_map.find(task->blob_id_);
    if (it == blob_map.end()) {
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
  void GetBlobSize(GetBlobSizeTask *task, RunContext &rctx) {
    if (task->blob_id_.IsNull()) {
      bitfield32_t flags;
      task->blob_id_ = GetOrCreateBlobId(task->tag_id_, task->lane_hash_,
                                         hshm::to_charbuf(*task->blob_name_),
                                         rctx, flags);
    }
    BLOB_MAP_T &blob_map = blob_map_[rctx.lane_id_];
    auto it = blob_map.find(task->blob_id_);
    if (it == blob_map.end()) {
      task->size_ = 0;
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
  void GetBlobScore(GetBlobScoreTask *task, RunContext &rctx) {
    BLOB_MAP_T &blob_map = blob_map_[rctx.lane_id_];
    auto it = blob_map.find(task->blob_id_);
    if (it == blob_map.end()) {
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
  void GetBlobBuffers(GetBlobBuffersTask *task, RunContext &rctx) {
    BLOB_MAP_T &blob_map = blob_map_[rctx.lane_id_];
    auto it = blob_map.find(task->blob_id_);
    if (it == blob_map.end()) {
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
  void RenameBlob(RenameBlobTask *task, RunContext &rctx) {
    BLOB_MAP_T &blob_map = blob_map_[rctx.lane_id_];
    auto it = blob_map.find(task->blob_id_);
    if (it == blob_map.end()) {
      task->SetModuleComplete();
      return;
    }
    BLOB_ID_MAP_T &blob_id_map = blob_id_map_[rctx.lane_id_];
    BlobInfo &blob = it->second;
    blob_id_map.erase(blob.name_);
    blob_id_map[blob.name_] = task->blob_id_;
    blob.name_ = hshm::to_charbuf(*task->new_blob_name_);
    task->SetModuleComplete();
  }

  /**
   * Truncate a blob to a new size
   * */
  void TruncateBlob(TruncateBlobTask *task, RunContext &rctx) {
    BLOB_MAP_T &blob_map = blob_map_[rctx.lane_id_];
    auto it = blob_map.find(task->blob_id_);
    if (it == blob_map.end()) {
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
  void DestroyBlob(DestroyBlobTask *task, RunContext &rctx) {
    switch (task->phase_) {
      case DestroyBlobPhase::kFreeBuffers: {
        BLOB_MAP_T &blob_map = blob_map_[rctx.lane_id_];
        auto it = blob_map.find(task->blob_id_);
        if (it == blob_map.end()) {
          task->SetModuleComplete();
          return;
        }
        BLOB_ID_MAP_T &blob_id_map = blob_id_map_[rctx.lane_id_];
        BlobInfo &blob_info = it->second;
        hshm::charbuf unique_name = GetBlobNameWithBucket(blob_info.tag_id_, blob_info.name_);
        blob_id_map.erase(unique_name);
        HSHM_MAKE_AR0(task->free_tasks_, nullptr);
        task->free_tasks_->reserve(blob_info.buffers_.size());
        for (BufferInfo &buf : blob_info.buffers_) {
          TargetInfo &tgt_info = *target_map_[buf.tid_];
          std::vector<BufferInfo> buf_vec = {buf};
          bdev::FreeTask *free_task = tgt_info.AsyncFree(
              task->task_node_ + 1, blob_info.score_,
              std::move(buf_vec), false).ptr_;
          task->free_tasks_->emplace_back(free_task);
        }
        task->phase_ = DestroyBlobPhase::kWaitFreeBuffers;
      }
      case DestroyBlobPhase::kWaitFreeBuffers: {
        std::vector<bdev::FreeTask *> &free_tasks = *task->free_tasks_;
        for (bdev::FreeTask *&free_task : free_tasks) {
          if (!free_task->IsComplete()) {
            return;
          }
        }
        for (bdev::FreeTask *&free_task : free_tasks) {
          HRUN_CLIENT->DelTask(free_task);
        }
        BLOB_MAP_T &blob_map = blob_map_[rctx.lane_id_];
        BlobInfo &blob_info = blob_map[task->blob_id_];
        bkt_mdm_.AsyncUpdateSize(task->task_node_ + 1,
                                 task->tag_id_,
                                 -(ssize_t)blob_info.blob_size_,
                                 bucket_mdm::UpdateSizeMode::kAdd);
        HSHM_DESTROY_AR(task->free_tasks_);
        blob_map.erase(task->blob_id_);
        task->SetModuleComplete();
      }
    }
  }

  /**
   * Reorganize \a blob_id blob in \a bkt_id bucket
   * */
  void ReorganizeBlob(ReorganizeBlobTask *task, RunContext &rctx) {
    switch (task->phase_) {
      case ReorganizeBlobPhase::kGet: {
        BLOB_MAP_T &blob_map = blob_map_[rctx.lane_id_];
        auto it = blob_map.find(task->blob_id_);
        if (it == blob_map.end()) {
          task->SetModuleComplete();
          return;
        }
        BlobInfo &blob_info = it->second;
        if (task->is_user_score_) {
          blob_info.user_score_ = task->score_;
          blob_info.score_ = std::max(blob_info.user_score_,
                                      blob_info.score_);
        } else {
          blob_info.score_ = task->score_;
        }
        if (!ShouldReorganize(blob_info, task->score_, task->task_node_)) {
          task->SetModuleComplete();
          return;
        }
        task->data_ = HRUN_CLIENT->AllocateBuffer(blob_info.blob_size_).shm_;
        task->data_size_ = blob_info.blob_size_;
        task->get_task_ = blob_mdm_.AsyncGetBlob(task->task_node_ + 1,
                                                 task->tag_id_,
                                                 hshm::charbuf(""),
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
        HRUN_CLIENT->DelTask(task->get_task_);
        task->phase_ = ReorganizeBlobPhase::kPut;
      }
      case ReorganizeBlobPhase::kPut: {
        task->put_task_ = blob_mdm_.AsyncPutBlob(task->task_node_ + 1,
                                                 task->tag_id_, hshm::charbuf(""),
                                                 task->blob_id_, 0,
                                                 task->data_size_,
                                                 task->data_,
                                                 task->score_,
                                                 HERMES_BLOB_REPLACE).ptr_;
        task->SetModuleComplete();
      }
    }
  }

  /**
   * Get all metadata about a blob
   * */
  HSHM_ALWAYS_INLINE
  void PollBlobMetadata(PollBlobMetadataTask *task, RunContext &rctx) {
    BLOB_MAP_T &blob_map = blob_map_[rctx.lane_id_];
    std::vector<BlobInfo> blob_mdms;
    blob_mdms.reserve(blob_map.size());
    for (const std::pair<BlobId, BlobInfo> &blob_part : blob_map) {
      const BlobInfo &blob_info = blob_part.second;
      blob_mdms.emplace_back(blob_info);
    }
    task->SerializeBlobMetadata(blob_mdms);
    task->SetModuleComplete();
  }

  /**
   * Get all metadata about a blob
   * */
  HSHM_ALWAYS_INLINE
  void PollTargetMetadata(PollTargetMetadataTask *task, RunContext &rctx) {
    std::vector<TargetStats> target_mdms;
    target_mdms.reserve(targets_.size());
    for (const bdev::Client &bdev_client : targets_) {
      bool is_remote = bdev_client.domain_id_.IsRemote(HRUN_RPC->GetNumHosts(), HRUN_CLIENT->node_id_);
      if (is_remote) {
        continue;
      }
      TargetStats stats;
      stats.tgt_id_ = bdev_client.id_;
      stats.rem_cap_ = bdev_client.monitor_task_->rem_cap_;
      stats.max_cap_ = bdev_client.max_cap_;
      stats.bandwidth_ = bdev_client.bandwidth_;
      stats.latency_ = bdev_client.latency_;
      stats.score_ = bdev_client.score_;
      target_mdms.emplace_back(stats);
    }
    task->SerializeTargetMetadata(target_mdms);
    task->SetModuleComplete();
  }

 public:
#include "hermes_blob_mdm/hermes_blob_mdm_lib_exec.h"
};

}  // namespace hrun

HRUN_TASK_CC(hermes::blob_mdm::Server, "hermes_blob_mdm");
