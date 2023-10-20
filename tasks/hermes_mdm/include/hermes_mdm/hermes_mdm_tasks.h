//
// Created by lukemartinlogan on 8/14/23.
//

#ifndef HRUN_TASKS_HERMES_MDM_INCLUDE_HERMES_MDM_HERMES_MDM_TASKS_H_
#define HRUN_TASKS_HERMES_MDM_INCLUDE_HERMES_MDM_HERMES_MDM_TASKS_H_

#include "hrun/api/hrun_client.h"
#include "hrun/task_registry/task_lib.h"
#include "hrun_admin/hrun_admin.h"
#include "hrun/queue_manager/queue_manager_client.h"
#include "hermes/hermes_types.h"
#include "bdev/bdev.h"
#include "proc_queue/proc_queue.h"

namespace hermes::mdm {

#include "hermes_mdm_methods.h"
#include "hrun/hrun_namespace.h"

/**
 * A task to create hermes_mdm
 * */
using hrun::Admin::CreateTaskStateTask;
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
    std::stringstream ss;
    cereal::BinaryOutputArchive ar(ss);
    ar(server_config_path_);
    std::string data = ss.str();
    *custom_ = data;
  }

  void Deserialize() {
    std::string data = custom_->str();
    std::stringstream ss(data);
    cereal::BinaryInputArchive ar(ss);
    ar(server_config_path_);
  }

  /** Destructor */
  HSHM_ALWAYS_INLINE
  ~ConstructTask() {
    HSHM_DESTROY_AR(server_config_path_);
  }
};

/** A task to destroy hermes_mdm */
using hrun::Admin::DestroyTaskStateTask;
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

#endif  // HRUN_TASKS_HERMES_MDM_INCLUDE_HERMES_MDM_HERMES_MDM_TASKS_H_
