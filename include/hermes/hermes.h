//
// Created by lukemartinlogan on 7/9/23.
//

#ifndef HRUN_TASKS_HERMES_INCLUDE_HERMES_HERMES_H_
#define HRUN_TASKS_HERMES_INCLUDE_HERMES_HERMES_H_

#include "hermes/bucket.h"

namespace hermes {

struct MetadataTable {
  std::vector<BlobInfo> blob_info_;
  std::vector<TargetStats> target_info_;
  std::vector<TagInfo> bkt_info_;
};

class Hermes {
 public:
  /** Init hermes client */
  void ClientInit() {
    HERMES_CONF->ClientInit();
  }

  /** Check if initialized */
  bool IsInitialized() {
    return HERMES_CONF->is_initialized_;
  }

  /** Get tag ID */
  TagId GetTagId(const std::string &tag_name) {
    return HERMES_CONF->bkt_mdm_.GetTagIdRoot(hshm::to_charbuf(tag_name));
  }

  /** Collect a snapshot of all metadata in Hermes */
  MetadataTable CollectMetadataSnapshot() {
    MetadataTable table;
    table.blob_info_ = HERMES_CONF->blob_mdm_.PollBlobMetadataRoot();
    table.target_info_ = HERMES_CONF->blob_mdm_.PollTargetMetadataRoot();
    table.bkt_info_ = HERMES_CONF->bkt_mdm_.PollTagMetadataRoot();
    return table;
  }

  /** Get or create a bucket */
  hermes::Bucket GetBucket(const std::string &path,
                           Context ctx = Context(),
                           size_t backend_size = 0,
                           u32 flags = 0) {
    return hermes::Bucket(path, ctx, backend_size, flags);
  }

  /** Register an operation graph */
  void RegisterOp(hermes::data_op::OpGraph &op_graph) {
    HERMES_CONF->op_mdm_.RegisterOpRoot(op_graph);
  }

  /** Clear all data from hermes */
  void Clear() {
    // TODO(llogan)
  }
};

#define HERMES hshm::EasySingleton<::hermes::Hermes>::GetInstance()

}  // namespace hermes

#endif  // HRUN_TASKS_HERMES_INCLUDE_HERMES_HERMES_H_
