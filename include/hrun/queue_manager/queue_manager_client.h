//
// Created by lukemartinlogan on 6/28/23.
//

#ifndef HRUN_INCLUDE_HRUN_QUEUE_MANAGER_QUEUE_MANAGER_CLIENT_H_
#define HRUN_INCLUDE_HRUN_QUEUE_MANAGER_QUEUE_MANAGER_CLIENT_H_

#include "queue_manager.h"

namespace hrun {

#define HRUN_QM_CLIENT \
  (&HRUN_CLIENT->queue_manager_)

/** Enable client programs to access queues */
class QueueManagerClient : public QueueManager {
 public:
  hipc::Allocator *alloc_;

 public:
  /** Default constructor */
  QueueManagerClient() = default;

  /** Destructor*/
  ~QueueManagerClient() = default;

  /** Initialize client */
  void ClientInit(hipc::Allocator *alloc, QueueManagerShm &shm, u32 node_id) {
    alloc_ = alloc;
    queue_map_ = shm.queue_map_.get();
    Init(node_id);
  }
};

}  // namespace hrun

#endif  // HRUN_INCLUDE_HRUN_QUEUE_MANAGER_QUEUE_MANAGER_CLIENT_H_
