/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Distributed under BSD 3-Clause license.                                   *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Illinois Institute of Technology.                        *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Hermes. The full Hermes copyright notice, including  *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the top directory. If you do not  *
 * have access to the file, you may request a copy from help@hdfgroup.org.   *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#ifndef HRUN_INCLUDE_HRUN_QUEUE_MANAGER_QUEUE_MANAGER_H_
#define HRUN_INCLUDE_HRUN_QUEUE_MANAGER_QUEUE_MANAGER_H_

#include "hrun/config/config_server.h"
#include "hrun/hrun_types.h"
#include "queue_factory.h"

namespace hrun {

/** Shared-memory representation of the QueueManager */
struct QueueManagerShm {
  hipc::ShmArchive<hipc::vector<MultiQueue>> queue_map_;
  hipc::ShmArchive<hipc::split_ticket_queue<size_t>> tickets_;
};

/** A base class inherited by Client & Server QueueManagers */
class QueueManager {
 public:
  hipc::vector<MultiQueue> *queue_map_;   /**< Queues which directly interact with tasks states */
  u32 node_id_;             /**< The ID of the node this QueueManager is on */
  QueueId admin_queue_;     /**< The queue used to submit administrative requests */
  QueueId process_queue_;   /**< The queue used to submit tasks from clients */
  TaskStateId admin_task_state_;  /**< The ID of the admin queue */

 public:
  void Init(u32 node_id) {
    node_id_ = node_id;
    admin_queue_ = QueueId(1, 0);
    admin_task_state_ = TaskStateId(1, 0);
    process_queue_ = QueueId(1, 1);
  }

  /**
   * Get a queue by ID
   *
   * TODO(llogan): Maybe make a local hashtable to map id -> ticket?
   * */
  HSHM_ALWAYS_INLINE MultiQueue* GetQueue(const QueueId &id) {
    return &(*queue_map_)[id.unique_];
  }
};

}  // namespace hrun

#endif  // HRUN_INCLUDE_HRUN_QUEUE_MANAGER_QUEUE_MANAGER_H_
