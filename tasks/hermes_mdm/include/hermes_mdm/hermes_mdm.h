//
// Created by lukemartinlogan on 6/29/23.
//

#ifndef LABSTOR_hermes_mdm_H_
#define LABSTOR_hermes_mdm_H_

#include "hermes_mdm_tasks.h"

namespace hermes::mdm {

/** Create requests */
class Client : public TaskLibClient {
 public:
  /** Default constructor */
  Client() = default;

  /** Destructor */
  ~Client() = default;

  /** Create a hermes_mdm */
  HSHM_ALWAYS_INLINE
  void CreateRoot(const DomainId &domain_id,
                  const std::string &state_name) {
    id_ = TaskStateId::GetNull();
    std::vector<PriorityInfo> queue_info = {
        {1, 1, 1, 0},
    };
    id_ = LABSTOR_ADMIN->CreateTaskStateRoot<ConstructTask>(
        domain_id, state_name, id_, queue_info);
    queue_id_ = QueueId(id_);
  }

  /** Destroy task state + queue */
  HSHM_ALWAYS_INLINE
  void DestroyRoot(const DomainId &domain_id) {
    LABSTOR_ADMIN->DestroyTaskStateRoot(domain_id, id_);
  }
};

}  // namespace labstor

#endif  // LABSTOR_hermes_mdm_H_
