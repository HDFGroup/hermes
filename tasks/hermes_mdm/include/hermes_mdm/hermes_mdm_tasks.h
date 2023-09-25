//
// Created by lukemartinlogan on 8/14/23.
//

#ifndef LABSTOR_TASKS_HERMES_MDM_INCLUDE_HERMES_MDM_HERMES_MDM_TASKS_H_
#define LABSTOR_TASKS_HERMES_MDM_INCLUDE_HERMES_MDM_HERMES_MDM_TASKS_H_

#include "labstor/api/labstor_client.h"
#include "labstor/task_registry/task_lib.h"
#include "labstor_admin/labstor_admin.h"
#include "labstor/queue_manager/queue_manager_client.h"
#include "hermes/hermes_types.h"
#include "bdev/bdev.h"
#include "proc_queue/proc_queue.h"

namespace hermes::mdm {

#include "hermes_mdm_methods.h"
#include "labstor/labstor_namespace.h"

/**
 * A task to create hermes_mdm
 * */
using labstor::Admin::CreateTaskStateTask;
struct ConstructTask : public CreateTaskStateTask {
  IN hipc::ShmArchive<hipc::string> server_config_path_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  ConstructTask(hipc::Allocator *alloc) : CreateTaskStateTask(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  ConstructTask(hipc::Allocator *alloc,
                const TaskNode &task_node,
                const DomainId &domain_id,
                const std::string &state_name,
                const TaskStateId &id,
                const std::vector<PriorityInfo> &queue_info,
                const std::string &server_config_path = "")
      : CreateTaskStateTask(alloc, task_node, domain_id, state_name,
                            "hermes_mdm", id, queue_info) {
    // Custom params
    HSHM_MAKE_AR(server_config_path_, alloc, server_config_path);
  }

  /** Destructor */
  HSHM_ALWAYS_INLINE
  ~ConstructTask() {
    HSHM_DESTROY_AR(server_config_path_);
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    CreateTaskStateTask::SerializeStart(ar);
    ar(server_config_path_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {}
};

/** A task to destroy hermes_mdm */
using labstor::Admin::DestroyTaskStateTask;
struct DestructTask : public DestroyTaskStateTask {
  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  DestructTask(hipc::Allocator *alloc) : DestroyTaskStateTask(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  DestructTask(hipc::Allocator *alloc,
               const TaskNode &task_node,
               const DomainId &domain_id,
               const TaskStateId &state_id)
      : DestroyTaskStateTask(alloc, task_node, domain_id, state_id) {}

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

}  // namespace hermes::mdm

#endif  // LABSTOR_TASKS_HERMES_MDM_INCLUDE_HERMES_MDM_HERMES_MDM_TASKS_H_
