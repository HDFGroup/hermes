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
  bdev::Client *fallback_target_;
  std::unordered_map<TargetId, TargetInfo*> target_map_;
  Client blob_mdm_;
  bucket_mdm::Client bkt_mdm_;
  data_stager::Client stager_mdm_;
  data_op::Client op_mdm_;
  LPointer<FlushDataTask> flush_task_;

 public:
  Server() = default;

  /** Construct blob mdm and it targets */
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
          "hermes_" + dev.dev_name_ + "/" + std::to_string(HRUN_CLIENT->node_id_),
          dev_type,
          dev).ptr_;
      target_tasks_.emplace_back(create_task);
    }
    for (int i = 0; i < target_tasks_.size(); ++i) {
      bdev::ConstructTask *tgt_task = target_tasks_[i];
      tgt_task->Wait<TASK_YIELD_CO>(task);
      bdev::Client &client = targets_[i];
      client.AsyncCreateComplete(tgt_task);
    }
    std::sort(targets_.begin(), targets_.end(),
              [](const bdev::Client &a, const bdev::Client &b) {
                return a.bandwidth_ > b.bandwidth_;
              });
    float bw_max = targets_.front().bandwidth_;
    float bw_min = targets_.back().bandwidth_;
    for (bdev::Client &client : targets_) {
      client.bw_score_ = (client.bandwidth_ - bw_min) / (bw_max - bw_min);
      client.score_ = client.bw_score_;
    }
    for (bdev::Client &client : targets_) {
      target_map_.emplace(client.id_, &client);
      HILOG(kInfo, "(node {}) Target {} has bw {} and score {}", HRUN_CLIENT->node_id_,
            client.id_, client.bandwidth_, client.bw_score_);
    }
    fallback_target_ = &targets_.back();
    blob_mdm_.Init(id_);
    HILOG(kInfo, "(node {}) Created Blob MDM", HRUN_CLIENT->node_id_);
    task->SetModuleComplete();
  }
  void MonitorConstruct(u32 mode, ConstructTask *task, RunContext &rctx) {
  }

  /** Destroy blob mdm */
  void Destruct(DestructTask *task, RunContext &rctx) {
    task->SetModuleComplete();
  }
  void MonitorDestruct(u32 mode, DestructTask *task, RunContext &rctx) {
  }

 private:
  /** Get the globally unique blob name */
  const hshm::charbuf GetBlobNameWithBucket(TagId tag_id, const hshm::charbuf &blob_name) {
    hshm::charbuf new_name(sizeof(TagId) + blob_name.size());
    hrun::LocalSerialize srl(new_name);
    srl << tag_id;
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
      flush_task_ = blob_mdm_.AsyncFlushData(task->task_node_ + 1);
    }
    task->SetModuleComplete();
  }
  void MonitorSetBucketMdm(u32 mode, SetBucketMdmTask *task, RunContext &rctx) {
  }

  /** New score */
  float NormalizeScore(float score) {
    if (score > 1) {
      return 1;
    }
    if (score < 0) {
      return 0;
    }
    return score;
  }
  float MakeScore(BlobInfo &blob_info, hshm::Timepoint &now) {
    ServerConfig &server = HERMES_CONF->server_config_;
    // Frequency score: how many times blob accessed?
    float freq_min = server.borg_.freq_min_;
    float freq_diff = server.borg_.freq_max_ - freq_min;
    float freq_score = NormalizeScore((blob_info.access_freq_ - freq_min) / freq_diff);
    // Temporal score: how recently the blob was accessed?
    float time_diff = blob_info.last_access_.GetSecFromStart(now);
    float rec_min = server.borg_.recency_min_;
    float rec_max = server.borg_.recency_max_;
    float rec_diff = rec_max - rec_min;
    float temporal_score = NormalizeScore((time_diff - rec_min) / rec_diff);
    temporal_score = 1 - temporal_score;
    // Access score: was the blob accessed recently or frequently?
    float access_score = std::max(freq_score, temporal_score);
    float user_score = blob_info.user_score_;
    // Final scores
    if (!blob_info.flags_.Any(HERMES_USER_SCORE_STATIONARY)) {
      return user_score * access_score;
    } else {
      return std::max(access_score, user_score);
    }
  }
  const bdev::Client& FindNearestTarget(float score) {
    for (const bdev::Client &cmp_tgt: targets_) {
      if (cmp_tgt.score_ > score + .05) {
        continue;
      }
      return cmp_tgt;
    }
    return targets_.back();
  }

  /** Check if blob should be reorganized */
  template<bool UPDATE_SCORE=false>
  bool ShouldReorganize(BlobInfo &blob_info,
                        float score,
                        TaskNode &task_node) {
    ServerConfig &server = HERMES_CONF->server_config_;
    for (BufferInfo &buf : blob_info.buffers_) {
      TargetInfo &target = *target_map_[buf.tid_];
      Histogram &hist = target.monitor_task_->score_hist_;
      u32 percentile = hist.GetPercentile(score);
      u32 precentile_lt = hist.GetPercentileLT(score);
      size_t rem_cap = target.monitor_task_->rem_cap_;
      size_t max_cap = target.max_cap_;
      float borg_cap_min = target.borg_min_thresh_;
      float borg_cap_max = target.borg_max_thresh_;
      // float min_score = hist.GetQuantile(0);
      // Update the target score
      target.score_ = target.bw_score_;
      // Update blob score
      if constexpr(UPDATE_SCORE) {
        u32 bin_orig = hist.GetBin(blob_info.score_);
        u32 bin_new = hist.GetBin(score);
        if (bin_orig != bin_new) {
          target.AsyncUpdateScore(task_node + 1,
                                  blob_info.score_, score);
        }
      }
      // Determine if the blob should be reorganized
      // Get the target with minimum difference in score to this blob
      if (abs(target.score_ - score) < .1) {
        continue;
      }
      const bdev::Client &cmp_tgt = FindNearestTarget(score);
      if (cmp_tgt.id_ == target.id_) {
        continue;
      }
      if (cmp_tgt.score_ <= target.score_) {
        // Demote if we have sufficiently low capacity
        if (rem_cap < max_cap * borg_cap_min) {
          HILOG(kInfo, "Demoting blob {} of score {} from tgt={} tgt_score={} to tgt={} tgt_score={}",
                blob_info.blob_id_, blob_info.score_,
                target.id_, target.score_,
                cmp_tgt.id_, cmp_tgt.score_);
          return true;
        }
      } else {
        // Promote if the guy above us has sufficiently high capacity
        float cmp_rem_cap = cmp_tgt.monitor_task_->rem_cap_;
        if (cmp_rem_cap > blob_info.blob_size_) {
          HILOG(kInfo, "Promoting blob {} of score {} from tgt={} tgt_score={} to tgt={} tgt_score={}",
                blob_info.blob_id_, blob_info.score_,
                target.id_, target.score_,
                cmp_tgt.id_, cmp_tgt.score_);
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Long-running task to stage out data periodically and
   * reorganize blobs
   * */
  struct FlushInfo {
    BlobInfo *blob_info_;
    LPointer<data_stager::StageOutTask> stage_task_;
    size_t mod_count_;
  };
  void FlushData(FlushDataTask *task, RunContext &rctx) {
    hshm::Timepoint now;
    now.Now();
    // Get the blob info data structure
    BLOB_MAP_T &blob_map = blob_map_[rctx.lane_id_];
    std::vector<FlushInfo> stage_tasks;
    stage_tasks.reserve(256);
    for (auto &it : blob_map) {
      BlobInfo &blob_info = it.second;
      // Update blob scores
      float new_score = MakeScore(blob_info, now);
      blob_info.score_ = new_score;
      if (ShouldReorganize<true>(blob_info, new_score, task->task_node_)) {
        LPointer<ReorganizeBlobTask> reorg_task =
            blob_mdm_.AsyncReorganizeBlob(task->task_node_ + 1,
                                          blob_info.tag_id_,
                                          blob_info.blob_id_,
                                          new_score, 0, false,
                                          TASK_LOW_LATENCY);
        reorg_task->Wait<TASK_YIELD_CO>(task);
        HRUN_CLIENT->DelTask(reorg_task);
      }
      blob_info.access_freq_ = 0;

      // Flush data
      FlushInfo flush_info;
      flush_info.blob_info_ = &blob_info;
      flush_info.mod_count_ = blob_info.mod_count_;
      if (blob_info.last_flush_ > 0 &&
          flush_info.mod_count_ > blob_info.last_flush_) {
        HILOG(kDebug, "Flushing blob {} (mod_count={}, last_flush={})",
              blob_info.blob_id_, flush_info.mod_count_, blob_info.last_flush_);
        LPointer<char> data = HRUN_CLIENT->AllocateBufferServer<TASK_YIELD_CO>(
            blob_info.blob_size_, task);
        LPointer<GetBlobTask> get_blob =
            blob_mdm_.AsyncGetBlob(task->task_node_ + 1,
                                   blob_info.tag_id_,
                                   blob_info.name_,
                                   blob_info.blob_id_,
                                   0, blob_info.blob_size_,
                                   data.shm_);
        get_blob->Wait<TASK_YIELD_CO>(task);
        HRUN_CLIENT->DelTask(get_blob);
        flush_info.stage_task_ =
          stager_mdm_.AsyncStageOut(task->task_node_ + 1,
                                    blob_info.tag_id_,
                                    blob_info.name_,
                                    data.shm_, blob_info.blob_size_,
                                    TASK_DATA_OWNER);
        stage_tasks.emplace_back(flush_info);
      }
      if (stage_tasks.size() == 256) {
        break;
      }
    }

    for (FlushInfo &flush_info : stage_tasks) {
      BlobInfo &blob_info = *flush_info.blob_info_;
      flush_info.stage_task_->Wait<TASK_YIELD_CO>(task);
      blob_info.last_flush_ = flush_info.mod_count_;
      HRUN_CLIENT->DelTask(flush_info.stage_task_);
    }
  }
  void MonitorFlushData(u32 mode, FlushDataTask *task, RunContext &rctx) {
    BLOB_MAP_T &blob_map = blob_map_[rctx.lane_id_];
    for (auto &it : blob_map) {
      BlobInfo &blob_info = it.second;
      if (blob_info.last_flush_ > 0 &&
          blob_info.mod_count_ > blob_info.last_flush_) {
        rctx.flush_->count_ += 1;
        return;
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
    HILOG(kDebug, "Beginning PUT for (hash: {})",
          std::hash<hshm::charbuf>{}(blob_name));
    BLOB_MAP_T &blob_map = blob_map_[rctx.lane_id_];
    BlobInfo &blob_info = blob_map[task->blob_id_];
    blob_info.score_ = task->score_;
    blob_info.user_score_ = task->score_;

    // Stage Blob
    if (task->flags_.Any(HERMES_SHOULD_STAGE) && blob_info.last_flush_ == 0) {
      HILOG(kDebug, "This file has not yet been flushed");
      blob_info.last_flush_ = 1;
      LPointer<data_stager::StageInTask> stage_task =
          stager_mdm_.AsyncStageIn(task->task_node_ + 1,
                                   task->tag_id_,
                                   blob_info.name_,
                                   task->score_, 0);
      stage_task->Wait<TASK_YIELD_CO>(task);
      blob_info.mod_count_ = 1;
      HRUN_CLIENT->DelTask(stage_task);
    }
    if (task->flags_.Any(HERMES_SHOULD_STAGE)) {
      HILOG(kDebug, "This is marked as a file: {} {}",
            blob_info.mod_count_, blob_info.last_flush_);
    }
    ssize_t bkt_size_diff = 0;
    if (task->flags_.Any(HERMES_BLOB_REPLACE)) {
      bkt_size_diff -= blob_info.blob_size_;
      PutBlobFreeBuffersPhase(blob_info, task, rctx);
    }

    // Determine amount of additional buffering space needed
    size_t needed_space = task->blob_off_ + task->data_size_;
    size_t size_diff = 0;
    if (needed_space > blob_info.max_blob_size_) {
      size_diff = needed_space - blob_info.max_blob_size_;
    }
    size_t min_blob_size = task->blob_off_ + task->data_size_;
    if (min_blob_size > blob_info.blob_size_) {
      blob_info.blob_size_ = task->blob_off_ + task->data_size_;
    }
    bkt_size_diff += (ssize_t)size_diff;
    HILOG(kDebug, "The size diff is {} bytes (bkt diff {})", size_diff, bkt_size_diff)

    // Use DPE
    std::vector<PlacementSchema> schema_vec;
    if (size_diff > 0) {
      Context ctx;
      auto *dpe = DpeFactory::Get(ctx.dpe_);
      ctx.blob_score_ = task->score_;
      dpe->Placement({size_diff}, targets_, ctx, schema_vec);
    }

    // Allocate blob buffers
    for (PlacementSchema &schema : schema_vec) {
      schema.plcmnts_.emplace_back(0, fallback_target_->id_);
      for (size_t sub_idx = 0; sub_idx < schema.plcmnts_.size(); ++sub_idx) {
        SubPlacement &placement = schema.plcmnts_[sub_idx];
        TargetInfo &bdev = *target_map_[placement.tid_];
        LPointer<bdev::AllocateTask> alloc_task =
            bdev.AsyncAllocate(task->task_node_ + 1,
                               blob_info.score_,
                               placement.size_,
                               blob_info.buffers_);
        alloc_task->Wait<TASK_YIELD_CO>(task);
//        HILOG(kInfo, "(node {}) Placing {}/{} bytes in target {} of bw {}",
//              HRUN_CLIENT->node_id_,
//              alloc_task->alloc_size_, task->data_size_,
//              placement.tid_, bdev.bandwidth_)
        if (alloc_task->alloc_size_ < alloc_task->size_) {
          SubPlacement &next_placement = schema.plcmnts_[sub_idx + 1];
          size_t diff = alloc_task->size_ - alloc_task->alloc_size_;
          next_placement.size_ += diff;
        }
        // bdev.monitor_task_->rem_cap_ -= alloc_task->alloc_size_;
        HRUN_CLIENT->DelTask(alloc_task);
      }
    }

    // Place blob in buffers
    std::vector<LPointer<bdev::WriteTask>> write_tasks;
    write_tasks.reserve(blob_info.buffers_.size());
    size_t blob_off = task->blob_off_, buf_off = 0;
    size_t buf_left = 0, buf_right = 0;
    size_t blob_right = task->blob_off_ + task->data_size_;
    char *blob_buf = HRUN_CLIENT->GetDataPointer(task->data_);
    HILOG(kDebug, "Number of buffers {}", blob_info.buffers_.size());
    bool found_left = false;
    for (BufferInfo &buf : blob_info.buffers_) {
      buf_right = buf_left + buf.t_size_;
      if (blob_off >= blob_right) {
        break;
      }
      if (buf_left <= blob_off && blob_off < buf_right) {
        found_left = true;
      }
      if (found_left) {
        size_t rel_off = blob_off - buf_left;
        size_t tgt_off = buf.t_off_ + rel_off;
        size_t buf_size = buf.t_size_ - rel_off;
        if (buf_right > blob_right) {
          buf_size = blob_right - (buf_left + rel_off);
        }
        HILOG(kDebug, "Writing {} bytes at off {} from target {}", buf_size, tgt_off, buf.tid_)
        TargetInfo &target = *target_map_[buf.tid_];
        LPointer<bdev::WriteTask> write_task =
            target.AsyncWrite(task->task_node_ + 1,
                              blob_buf + buf_off,
                              tgt_off, buf_size);
        write_tasks.emplace_back(write_task);
        buf_off += buf_size;
        blob_off = buf_right;
      }
      buf_left += buf.t_size_;
    }
    blob_info.max_blob_size_ = blob_off;

    // Wait for the placements to complete
    for (LPointer<bdev::WriteTask> &write_task : write_tasks) {
      write_task->Wait<TASK_YIELD_CO>(task);
      HRUN_CLIENT->DelTask(write_task);
    }

    // Update information
    if (task->flags_.Any(HERMES_SHOULD_STAGE)) {
      // TODO(llogan): Move to data stager
      adapter::BlobPlacement p;
      std::string blob_name_str = task->blob_name_->str();
      p.DecodeBlobName(blob_name_str, 1 << 20);
      bkt_mdm_.AsyncUpdateSize(task->task_node_ + 1,
                               task->tag_id_,
                               p.bucket_off_ + task->blob_off_ + task->data_size_,
                               bucket_mdm::UpdateSizeMode::kCap);
    } else {
      bkt_mdm_.AsyncUpdateSize(task->task_node_ + 1,
                               task->tag_id_,
                               bkt_size_diff,
                               bucket_mdm::UpdateSizeMode::kAdd);
    }
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
    blob_info.UpdateWriteStats();
    task->SetModuleComplete();
  }
  void MonitorPutBlob(u32 mode, PutBlobTask *task, RunContext &rctx) {
  }

  /** Release buffers */
  void PutBlobFreeBuffersPhase(BlobInfo &blob_info, PutBlobTask *task, RunContext &rctx) {
    for (BufferInfo &buf : blob_info.buffers_) {
      TargetInfo &target = *target_map_[buf.tid_];
      std::vector<BufferInfo> buf_vec = {buf};
      target.AsyncFree(task->task_node_ + 1,
                       blob_info.score_,
                       std::move(buf_vec), true);
    }
    blob_info.buffers_.clear();
    blob_info.max_blob_size_ = 0;
    blob_info.blob_size_ = 0;
  }

  /** Get a blob's data */
  void GetBlob(GetBlobTask *task, RunContext &rctx) {
    if (task->blob_id_.IsNull()) {
      hshm::charbuf blob_name = hshm::to_charbuf(*task->blob_name_);
      task->blob_id_ = GetOrCreateBlobId(task->tag_id_, task->lane_hash_,
                                         blob_name, rctx, task->flags_);
    }
    BLOB_MAP_T &blob_map = blob_map_[rctx.lane_id_];
    BlobInfo &blob_info = blob_map[task->blob_id_];

    // Stage Blob
    if (task->flags_.Any(HERMES_SHOULD_STAGE) && blob_info.last_flush_ == 0) {
      // TODO(llogan): Don't hardcore score = 1
      blob_info.last_flush_ = 1;
      LPointer<data_stager::StageInTask> stage_task =
          stager_mdm_.AsyncStageIn(task->task_node_ + 1,
                                   task->tag_id_,
                                   blob_info.name_,
                                   1, 0);
      stage_task->Wait<TASK_YIELD_CO>(task);
      HRUN_CLIENT->DelTask(stage_task);
    }

    // Read blob from buffers
    std::vector<bdev::ReadTask*> read_tasks;
    read_tasks.reserve(blob_info.buffers_.size());
    HILOG(kDebug, "Getting blob {} of size {} starting at offset {} (total_blob_size={}, buffers={})",
          task->blob_id_, task->data_size_, task->blob_off_, blob_info.blob_size_, blob_info.buffers_.size());
    size_t blob_off = task->blob_off_;
    size_t buf_left = 0, buf_right = 0;
    size_t buf_off = 0;
    size_t blob_right = task->blob_off_ + task->data_size_;
    bool found_left = false;
    char *blob_buf = HRUN_CLIENT->GetDataPointer(task->data_);
    for (BufferInfo &buf : blob_info.buffers_) {
      buf_right = buf_left + buf.t_size_;
      if (blob_off >= blob_right) {
        break;
      }
      if (buf_left <= blob_off && blob_off < buf_right) {
        found_left = true;
      }
      if (found_left) {
        size_t rel_off = blob_off - buf_left;
        size_t tgt_off = buf.t_off_ + rel_off;
        size_t buf_size = buf.t_size_ - rel_off;
        if (buf_right > blob_right) {
          buf_size = blob_right - (buf_left + rel_off);
        }
        HILOG(kDebug, "Loading {} bytes at off {} from target {}", buf_size, tgt_off, buf.tid_)
        TargetInfo &target = *target_map_[buf.tid_];
        bdev::ReadTask *read_task = target.AsyncRead(task->task_node_ + 1,
                                                     blob_buf + buf_off,
                                                     tgt_off, buf_size).ptr_;
        read_tasks.emplace_back(read_task);
        buf_off += buf_size;
        blob_off = buf_right;
      }
      buf_left += buf.t_size_;
    }
    for (bdev::ReadTask *&read_task : read_tasks) {
      read_task->Wait<TASK_YIELD_CO>(task);
      HRUN_CLIENT->DelTask(read_task);
    }
    task->data_size_ = buf_off;
    task->SetModuleComplete();
  }
  void MonitorGetBlob(u32 mode, GetBlobTask *task, RunContext &rctx) {
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
  void MonitorTagBlob(u32 mode, TagBlobTask *task, RunContext &rctx) {
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
  void MonitorBlobHasTag(u32 mode, BlobHasTagTask *task, RunContext &rctx) {
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
      BlobInfo &blob_info = blob_map[blob_id];
      blob_info.name_ = blob_name;
      blob_info.blob_id_ = blob_id;
      blob_info.tag_id_ = tag_id;
      blob_info.blob_size_ = 0;
      blob_info.max_blob_size_ = 0;
      blob_info.score_ = 1;
      blob_info.mod_count_ = 0;
      blob_info.access_freq_ = 0;
      blob_info.last_flush_ = 0;
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
  void MonitorGetOrCreateBlobId(u32 mode, GetOrCreateBlobIdTask *task, RunContext &rctx) {
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
  void MonitorGetBlobId(u32 mode, GetBlobIdTask *task, RunContext &rctx) {
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
  void MonitorGetBlobName(u32 mode, GetBlobNameTask *task, RunContext &rctx) {
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
  void MonitorGetBlobSize(u32 mode, GetBlobSizeTask *task, RunContext &rctx) {
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
  void MonitorGetBlobScore(u32 mode, GetBlobScoreTask *task, RunContext &rctx) {
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
  void MonitorGetBlobBuffers(u32 mode, GetBlobBuffersTask *task, RunContext &rctx) {
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
  void MonitorRenameBlob(u32 mode, RenameBlobTask *task, RunContext &rctx) {
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
  void MonitorTruncateBlob(u32 mode, TruncateBlobTask *task, RunContext &rctx) {
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
        if (task->update_size_) {
          bkt_mdm_.AsyncUpdateSize(task->task_node_ + 1,
                                   task->tag_id_,
                                   -(ssize_t) blob_info.blob_size_,
                                   bucket_mdm::UpdateSizeMode::kAdd);
        }
        HSHM_DESTROY_AR(task->free_tasks_);
        blob_map.erase(task->blob_id_);
        task->SetModuleComplete();
      }
    }
  }
  void MonitorDestroyBlob(u32 mode, DestroyBlobTask *task, RunContext &rctx) {
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
          blob_info.score_ = blob_info.user_score_;
        } else {
          blob_info.score_ = task->score_;
        }
//        if (!ShouldReorganize(blob_info, task->score_, task->task_node_)) {
//          task->SetModuleComplete();
//          return;
//        }
        task->data_ = HRUN_CLIENT->AllocateBufferServer<TASK_YIELD_STD>(
            blob_info.blob_size_, task).shm_;
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
  void MonitorReorganizeBlob(u32 mode, ReorganizeBlobTask *task, RunContext &rctx) {
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
  void MonitorPollBlobMetadata(u32 mode, PollBlobMetadataTask *task, RunContext &rctx) {
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
      stats.node_id_ = HRUN_CLIENT->node_id_;
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
  void MonitorPollTargetMetadata(u32 mode, PollTargetMetadataTask *task, RunContext &rctx) {
  }

 public:
#include "hermes_blob_mdm/hermes_blob_mdm_lib_exec.h"
};

}  // namespace hrun

HRUN_TASK_CC(hermes::blob_mdm::Server, "hermes_blob_mdm");
