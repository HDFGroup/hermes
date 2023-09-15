//
// Created by lukemartinlogan on 6/29/23.
//

#ifndef LABSTOR_worch_queue_round_robin_H_
#define LABSTOR_worch_queue_round_robin_H_

#include "worch_queue_round_robin_tasks.h"

namespace labstor::worch_queue_round_robin {

/** Create admin requests */
class Client : public TaskLibClient {

 public:
  /** Default constructor */
  Client() = default;

  /** Destructor */
  ~Client() = default;

  /** Create a worch_queue_round_robin */
  HSHM_ALWAYS_INLINE
  void CreateRoot(const DomainId &domain_id,
                  const std::string &state_name) {
    id_ = TaskStateId::GetNull();
    std::vector<PriorityInfo> queue_info = {
        {1, 1, 4, 0},
    };
    id_ = LABSTOR_ADMIN->CreateTaskStateRoot<ConstructTask>(
        domain_id, state_name, id_, queue_info);
  }

  /** Destroy task state */
  HSHM_ALWAYS_INLINE
  void DestroyRoot(const DomainId &domain_id) {
    LABSTOR_ADMIN->DestroyTaskStateRoot(domain_id, id_);
  }
};

}  // namespace labstor

#endif  // LABSTOR_worch_queue_round_robin_H_
