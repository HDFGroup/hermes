//
// Created by lukemartinlogan on 7/9/23.
//

#ifndef LABSTOR_TASKS_HERMES_INCLUDE_HERMES_HERMES_H_
#define LABSTOR_TASKS_HERMES_INCLUDE_HERMES_HERMES_H_

#include "hermes/bucket.h"

namespace hermes {

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

  /** Get or create a bucket */
  hermes::Bucket GetBucket(const std::string &path,
                           Context ctx = Context(),
                           size_t backend_size = 0,
                           u32 flags = 0) {
    return hermes::Bucket(path, ctx, backend_size, flags);
  }

  /** Clear all data from hermes */
  void Clear() {
    // TODO(llogan)
  }
};

#define HERMES hshm::EasySingleton<hermes::Hermes>::GetInstance()

}  // namespace hermes

#endif  // LABSTOR_TASKS_HERMES_INCLUDE_HERMES_HERMES_H_
