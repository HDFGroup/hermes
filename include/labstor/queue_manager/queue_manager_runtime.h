//
// Created by lukemartinlogan on 6/28/23.
//

#ifndef LABSTOR_INCLUDE_LABSTOR_QUEUE_MANAGER_QUEUE_MANAGER_SERVER_H_
#define LABSTOR_INCLUDE_LABSTOR_QUEUE_MANAGER_QUEUE_MANAGER_SERVER_H_

#include "queue_manager.h"
#include "queue_manager_client.h"

namespace labstor {

#define LABSTOR_QM_RUNTIME \
  (&LABSTOR_RUNTIME->queue_manager_)

/** Administrative queuing actions */
class QueueManagerRuntime : public QueueManager {
 public:
  ServerConfig *config_;
  size_t max_queues_;
  size_t max_lanes_;
  hipc::split_ticket_queue<u64> *tickets_;
  u32 node_id_;

 public:
  /** Default constructor */
  QueueManagerRuntime() = default;

  /** Destructor*/
  ~QueueManagerRuntime() = default;

  /** Create queues in shared memory */
  void ServerInit(hipc::Allocator *alloc, u32 node_id, ServerConfig *config, QueueManagerShm &shm) {
    config_ = config;
    Init(node_id);
    QueueManagerInfo &qm = config_->queue_manager_;
    // Initialize ticket queue (ticket 0 is for admin queue)
    max_queues_ = qm.max_queues_;
    max_lanes_ = qm.max_lanes_;
    HSHM_MAKE_AR(shm.tickets_, alloc, max_queues_)
    for (u64 i = 1; i <= max_queues_; ++i) {
      shm.tickets_->emplace(i);
    }
    // Initialize queue map
    HSHM_MAKE_AR0(shm.queue_map_, alloc)
    queue_map_ = shm.queue_map_.get();
    queue_map_->resize(max_queues_);
    // Create the admin queue
    CreateQueue(admin_queue_, {
      {1, 1, qm.queue_depth_, QUEUE_UNORDERED}
    });
    CreateQueue(process_queue_, {
        {1, 1, qm.queue_depth_, QUEUE_UNORDERED},
        {1, 1, qm.queue_depth_, QUEUE_LONG_RUNNING},
        {qm.max_lanes_, qm.max_lanes_, qm.queue_depth_, QUEUE_LOW_LATENCY}
    });
  }

  /** Create a new queue (with pre-allocated ID) in the map */
  void CreateQueue(const QueueId &id, const std::vector<PriorityInfo> &queue_info) {
    MultiQueue *queue = GetQueue(id);
    if (id.IsNull()) {
      HELOG(kError, "Cannot create null queue {}", id);
      return;
    }
    if (!queue->id_.IsNull()) {
      HELOG(kError, "Queue {} already exists", id);
      return;
    }
    // HILOG(kDebug, "Creating queue {}", id);
    queue_map_->replace(queue_map_->begin() + id.unique_, id, queue_info);
  }

  /**
   * Remove a queue
   *
   * For now, this function assumes that the queue is not in use.
   * TODO(llogan): don't assume this
   * */
  void DestroyQueue(QueueId &id) {
    queue_map_->erase(queue_map_->begin() + id.unique_);
    tickets_->emplace(id.unique_);
  }
};

}  // namespace labstor

#endif  // LABSTOR_INCLUDE_LABSTOR_QUEUE_MANAGER_QUEUE_MANAGER_SERVER_H_
