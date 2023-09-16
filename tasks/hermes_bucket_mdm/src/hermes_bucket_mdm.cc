//
// Created by lukemartinlogan on 6/29/23.
//

#include "labstor_admin/labstor_admin.h"
#include "labstor/api/labstor_runtime.h"
#include "hermes/config_server.h"
#include "hermes_bucket_mdm/hermes_bucket_mdm.h"
#include "hermes_adapters/mapper/abstract_mapper.h"
#include "hermes/dpe/dpe_factory.h"
#include "bdev/bdev.h"

namespace hermes::bucket_mdm {

typedef std::unordered_map<hshm::charbuf, TagId> TAG_ID_MAP_T;
typedef std::unordered_map<TagId, TagInfo> TAG_MAP_T;

class Server : public TaskLib {
 public:
  TAG_ID_MAP_T tag_id_map_;
  TAG_MAP_T tag_map_;
  u32 node_id_;
  std::atomic<u64> id_alloc_;
  Client bkt_mdm_;
  blob_mdm::Client blob_mdm_;

 public:
  Server() = default;

  void Construct(ConstructTask *task) {
    id_alloc_ = 0;
    node_id_ = LABSTOR_CLIENT->node_id_;
    bkt_mdm_.Init(id_);
    task->SetModuleComplete();
  }

  void Destruct(DestructTask *task) {
    task->SetModuleComplete();
  }

  /**
   * Set the Blob MDM
   * */
  void SetBlobMdm(SetBlobMdmTask *task) {
    blob_mdm_.Init(task->blob_mdm_);
    task->SetModuleComplete();
  }

  /** Update the size of the bucket */
  void UpdateSize(UpdateSizeTask *task) {
    TagInfo &tag_info = tag_map_[task->tag_id_];
    ssize_t internal_size = (ssize_t) tag_info.internal_size_;
    if (task->mode_ == UpdateSizeMode::kAdd) {
      internal_size += task->update_;
    } else {
      internal_size = std::max(task->update_, internal_size);
    }
    tag_info.internal_size_ = (size_t) internal_size;
    task->SetModuleComplete();
  }

  /**
   * Create the PartialPuts for append operations.
   * */
  void AppendBlobSchema(AppendBlobSchemaTask *task) {
    switch (task->phase_) {
      case AppendBlobPhase::kGetBlobIds: {
        HILOG(kDebug, "(node {}) Getting blob IDs for tag {} (task_node={})",
              LABSTOR_CLIENT->node_id_, task->tag_id_, task->task_node_)
        TagInfo &tag_info = tag_map_[task->tag_id_];
        size_t bucket_size = tag_info.internal_size_;
        size_t cur_page = bucket_size / task->page_size_;
        size_t cur_page_off = bucket_size % task->page_size_;
        size_t update_size = task->page_size_ - cur_page_off;
        size_t max_pages = task->data_size_ / task->page_size_ + 1;
        size_t cur_size = 0;
        HILOG(kDebug, "(node {}) Bucket size {}, page_size {}, cur_page {} (task_node={})",
              LABSTOR_CLIENT->node_id_, bucket_size, task->page_size_, cur_page, task->task_node_)
        HSHM_MAKE_AR0(task->append_info_, nullptr);
        std::vector<AppendInfo> &append_info = *task->append_info_;
        append_info.reserve(max_pages);
        while (cur_size < task->data_size_) {
          if (update_size > task->data_size_) {
            update_size = task->data_size_;
          }
          append_info.emplace_back();
          AppendInfo &append = append_info.back();
          append.blob_name_ = hshm::charbuf(std::to_string(cur_page));
          append.data_size_ = update_size;
          append.blob_off_ = cur_page_off;
          append.blob_id_task_ = blob_mdm_.AsyncGetOrCreateBlobId(task->task_node_ + 1,
                                                                  task->tag_id_,
                                                                  append.blob_name_).ptr_;
          cur_size += update_size;
          cur_page_off = 0;
          ++cur_page;
          update_size = task->page_size_;
        }
        task->phase_ = AppendBlobPhase::kWaitBlobIds;
      }
      case AppendBlobPhase::kWaitBlobIds: {
        std::vector<AppendInfo> &append_info = *task->append_info_;
        for (AppendInfo &append : append_info) {
          if (!append.blob_id_task_->IsComplete()) {
            return;
          }
        }
        HILOG(kDebug, "(node {}) Finished blob IDs for tag {} (task_node={})",
              LABSTOR_CLIENT->node_id_, task->tag_id_, task->task_node_)
        for (AppendInfo &append : append_info) {
          append.blob_id_ = append.blob_id_task_->blob_id_;
          LABSTOR_CLIENT->DelTask(append.blob_id_task_);
        }
        task->SetModuleComplete();
      }
    }
  }

  /**
   * Append data to a bucket. Assumes that the blobs in the bucket
   * are named 0 ... N. Each blob is assumed to have a certain
   * fixed page size.
   * */
  void AppendBlob(AppendBlobTask *task) {
    switch (task->phase_) {
      case AppendBlobPhase::kGetBlobIds: {
        HILOG(kDebug, "(node {}) Appending {} bytes to bucket {} (task_node={})",
              LABSTOR_CLIENT->node_id_, task->data_size_, task->tag_id_, task->task_node_);
        task->schema_ = bkt_mdm_.AsyncAppendBlobSchema(task->task_node_ + 1,
                                                       task->tag_id_,
                                                       task->data_size_,
                                                       task->page_size_).ptr_;
        task->phase_ = AppendBlobPhase::kWaitBlobIds;
      }
      case AppendBlobPhase::kWaitBlobIds: {
        if (!task->schema_->IsComplete()) {
          return;
        }
        std::vector<AppendInfo> &append_info = *task->schema_->append_info_;
        size_t buf_off = 0;
        HILOG(kDebug, "(node {}) Got blob schema of size {} for tag {} (task_node={})",
              LABSTOR_CLIENT->node_id_, append_info.size(), task->tag_id_, task->task_node_)
        for (AppendInfo &append : append_info) {
          HILOG(kDebug, "(node {}) Spawning blob {} of size {} for tag {} (task_node={} blob_mdm={})",
                LABSTOR_CLIENT->node_id_, append.blob_name_.str(), append.data_size_,
                task->tag_id_, task->task_node_, blob_mdm_.id_);
          append.put_task_ = blob_mdm_.AsyncPutBlob(task->task_node_ + 1,
                                                    task->tag_id_,
                                                    append.blob_name_,
                                                    append.blob_id_,
                                                    append.blob_off_,
                                                    append.data_size_,
                                                    task->data_ + buf_off,
                                                    task->score_,
                                                    bitfield32_t(0),
                                                    Context(),
                                                    bitfield32_t(0)).ptr_;
          HILOG(kDebug, "(node {}) Finished spawning blob {} of size {} for tag {} (task_node={} blob_mdm={})",
                LABSTOR_CLIENT->node_id_, append.blob_name_.str(), append.data_size_,
                task->tag_id_, task->task_node_, blob_mdm_.id_);
          buf_off += append.data_size_;
        }
        task->phase_ = AppendBlobPhase::kWaitPutBlobs;
      }
      case AppendBlobPhase::kWaitPutBlobs: {
        std::vector<AppendInfo> &append_info = *task->schema_->append_info_;
        for (AppendInfo &append : append_info) {
          if (!append.put_task_->IsComplete()) {
            return;
          }
        }
        HILOG(kDebug, "(node {}) PUT blobs for tag {} (task_node={})",
              LABSTOR_CLIENT->node_id_, task->tag_id_, task->task_node_)
        for (AppendInfo &append : append_info) {
          LABSTOR_CLIENT->DelTask(append.put_task_);
        }
        HSHM_DESTROY_AR(task->schema_->append_info_);
        LABSTOR_CLIENT->DelTask(task->schema_);
        task->SetModuleComplete();
      }
    }
  }

  /** Get or create a tag */
  void GetOrCreateTag(GetOrCreateTagTask *task) {
    TagId tag_id;
    HILOG(kDebug, "Creating a tag")

    // Check if the tag exists
    hshm::charbuf tag_name = hshm::to_charbuf(*task->tag_name_);
    bool did_create = false;
    if (tag_name.size() > 0) {
      did_create = tag_id_map_.find(tag_name) == tag_id_map_.end();
    }

    // Emplace bucket if it does not already exist
    if (did_create) {
      tag_id.unique_ = id_alloc_.fetch_add(1);
      tag_id.node_id_ = LABSTOR_RUNTIME->rpc_.node_id_;
      HILOG(kDebug, "Creating tag for the first time: {} {}", tag_name.str(), tag_id)
      tag_id_map_.emplace(tag_name, tag_id);
      tag_map_.emplace(tag_id, TagInfo());
      TagInfo &info = tag_map_[tag_id];
      info.name_ = tag_name;
      info.tag_id_ = tag_id;
      info.owner_ = task->blob_owner_;
      info.internal_size_ = task->backend_size_;
    } else {
      if (tag_name.size()) {
        HILOG(kDebug, "Found existing tag: {}", tag_name.str())
        tag_id = tag_id_map_[tag_name];
      } else {
        HILOG(kDebug, "Found existing tag: {}", task->tag_id_)
        tag_id = task->tag_id_;
      }
    }

    task->tag_id_ = tag_id;
    // task->did_create_ = did_create;
    task->SetModuleComplete();
  }

  /** Get tag ID */
  void GetTagId(GetTagIdTask *task) {
    hshm::charbuf tag_name = hshm::to_charbuf(*task->tag_name_);
    auto it = tag_id_map_.find(tag_name);
    if (it == tag_id_map_.end()) {
      task->tag_id_ = TagId::GetNull();
      task->SetModuleComplete();
      return;
    }
    task->tag_id_ = it->second;
    task->SetModuleComplete();
  }

  /** Get tag name */
  void GetTagName(GetTagNameTask *task) {
    auto it = tag_map_.find(task->tag_id_);
    if (it == tag_map_.end()) {
      task->SetModuleComplete();
      return;
    }
    (*task->tag_name_) = it->second.name_;
    task->SetModuleComplete();
  }

  /** Rename tag */
  void RenameTag(RenameTagTask *task) {
    auto it = tag_map_.find(task->tag_id_);
    if (it == tag_map_.end()) {
      task->SetModuleComplete();
      return;
    }
    (*task->tag_name_) = (*task->tag_name_);
    task->SetModuleComplete();
  }

  /** Destroy tag */
  void DestroyTag(DestroyTagTask *task) {
    switch (task->phase_) {
      case DestroyTagPhase::kDestroyBlobs: {
        TagInfo &tag = tag_map_[task->tag_id_];
        tag_id_map_.erase(tag.name_);
        HSHM_MAKE_AR0(task->destroy_blob_tasks_, nullptr);
        std::vector<blob_mdm::DestroyBlobTask*> blob_tasks = *task->destroy_blob_tasks_;
        blob_tasks.reserve(tag.blobs_.size());
        for (BlobId &blob_id : tag.blobs_) {
          blob_mdm::DestroyBlobTask *blob_task =
              blob_mdm_.AsyncDestroyBlob(task->task_node_ + 1, task->tag_id_, blob_id).ptr_;
          blob_tasks.emplace_back(blob_task);
        }
        task->phase_ = DestroyTagPhase::kWaitDestroyBlobs;
        return;
      }
      case DestroyTagPhase::kWaitDestroyBlobs: {
        std::vector<blob_mdm::DestroyBlobTask*> blob_tasks = *task->destroy_blob_tasks_;
        for (auto it = blob_tasks.rbegin(); it != blob_tasks.rend(); ++it) {
          blob_mdm::DestroyBlobTask *blob_task = *it;
          if (!blob_task->IsComplete()) {
            return;
          }
          LABSTOR_CLIENT->DelTask(blob_task);
          blob_tasks.pop_back();
        }
        HSHM_DESTROY_AR(task->destroy_blob_tasks_);
        tag_map_.erase(task->tag_id_);
        task->SetModuleComplete();
      }
    }
  }

  /** Add a blob to a tag */
  void TagAddBlob(TagAddBlobTask *task) {
    auto it = tag_map_.find(task->tag_id_);
    if (it == tag_map_.end()) {
      task->SetModuleComplete();
      return;
    }
    TagInfo &tag = it->second;
    tag.blobs_.emplace_back(task->blob_id_);
    task->SetModuleComplete();
  }

  /** Remove a blob from a tag */
  void TagRemoveBlob(TagRemoveBlobTask *task) {
    auto it = tag_map_.find(task->tag_id_);
    if (it == tag_map_.end()) {
      task->SetModuleComplete();
      return;
    }
    TagInfo &tag = it->second;
    auto blob_it = std::find(tag.blobs_.begin(), tag.blobs_.end(), task->blob_id_);
    tag.blobs_.erase(blob_it);
    task->SetModuleComplete();
  }

  /** Clear blobs from a tag */
  void TagClearBlobs(TagClearBlobsTask *task) {
    auto it = tag_map_.find(task->tag_id_);
    if (it == tag_map_.end()) {
      task->SetModuleComplete();
      return;
    }
    TagInfo &tag = it->second;
    tag.blobs_.clear();
    tag.internal_size_ = 0;
    task->SetModuleComplete();
  }

  /** Get size of the bucket */
  void GetSize(GetSizeTask *task) {
    auto it = tag_map_.find(task->tag_id_);
    if (it == tag_map_.end()) {
      task->size_ = 0;
      task->SetModuleComplete();
      return;
    }
    TagInfo &tag = it->second;
    task->size_ = tag.internal_size_;
    task->SetModuleComplete();
  }



 public:
#include "hermes_bucket_mdm/hermes_bucket_mdm_lib_exec.h"
};

}  // namespace labstor

LABSTOR_TASK_CC(hermes::bucket_mdm::Server, "hermes_bucket_mdm");
