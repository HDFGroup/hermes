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
