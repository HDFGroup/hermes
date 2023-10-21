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

#ifndef HRUN_INCLUDE_HRUN_TASK_TASK_H_
#define HRUN_INCLUDE_HRUN_TASK_TASK_H_

#include <dlfcn.h>
#include "hrun/hrun_types.h"
#include "hrun/queue_manager/queue_factory.h"
#include "hrun/network/serialize.h"
#include "task.h"

namespace hrun {

typedef LPointer<Task> TaskPointer;

/**
 * Represents a custom operation to perform.
 * Tasks are independent of Hermes.
 * */
class TaskLib {
 public:
  TaskStateId id_;    /**< The unique name of a task state */
  QueueId queue_id_;  /**< The queue id of a task state */
  std::string name_; /**< The unique semantic name of a task state */

  /** Default constructor */
  TaskLib() : id_(TaskStateId::GetNull()) {}

  /** Emplace Constructor */
  void Init(const TaskStateId &id, const std::string &name) {
    id_ = id;
    queue_id_ = QueueId(id);
    name_ = name;
  }

  /** Virtual destructor */
  virtual ~TaskLib() = default;

  /** Run a method of the task */
  virtual void Run(u32 method, Task *task, RunContext &rctx) = 0;

  /** Delete a task */
  virtual void Del(u32 method, Task *task) = 0;

  /** Duplicate a task */
  virtual void Dup(u32 method, Task *orig_task, std::vector<LPointer<Task>> &dups) = 0;

  /** Register end of duplicate */
  virtual void DupEnd(u32 method, u32 replica, Task *orig_task, Task *dup_task) = 0;

  /** Allow task to store replicas of completion */
  virtual void ReplicateStart(u32 method, u32 count, Task *task) = 0;

  /** Can be used to summarize the completions */
  virtual void ReplicateEnd(u32 method, Task *task) = 0;

  /** Serialize a task when initially pushing into remote */
  virtual std::vector<DataTransfer> SaveStart(u32 method, BinaryOutputArchive<true> &ar, Task *task) = 0;

  /** Deserialize a task when popping from remote queue */
  virtual TaskPointer LoadStart(u32 method, BinaryInputArchive<true> &ar) = 0;

  /** Serialize a task when returning from remote queue */
  virtual std::vector<DataTransfer> SaveEnd(u32 method, BinaryOutputArchive<false> &ar, Task *task) = 0;

  /** Deserialize a task when returning from remote queue */
  virtual void LoadEnd(u32 replica, u32 method, BinaryInputArchive<false> &ar, Task *task) = 0;

  /** Deserialize a task when returning from remote queue */
  virtual u32 GetGroup(u32 method, Task *task, hshm::charbuf &buf) = 0;
};

/** Represents a TaskLib in action */
typedef TaskLib TaskState;

/** Represents the TaskLib client-side */
class TaskLibClient {
 public:
  TaskStateId id_;
  QueueId queue_id_;

 public:
  /** Init from existing ID */
  void Init(const TaskStateId &id) {
    id_ = id;
    queue_id_ = QueueId(id_);
  }
};

extern "C" {
/** Allocate a state (no construction) */
typedef TaskState* (*alloc_state_t)(Task *task, const char *state_name);
/** Allocate + construct a state */
typedef TaskState* (*create_state_t)(Task *task, const char *state_name);
/** Get the name of a task */
typedef const char* (*get_task_lib_name_t)(void);
}  // extern c

/** Used internally by task source file */
#define HRUN_TASK_CC(TRAIT_CLASS, TASK_NAME)\
  extern "C" {\
  void* alloc_state(hrun::Admin::CreateTaskStateTask *task, const char *state_name) {\
    hrun::TaskState *exec = reinterpret_cast<hrun::TaskState*>(\
        new TYPE_UNWRAP(TRAIT_CLASS)());\
    exec->Init(task->id_, state_name);\
    return exec;\
  }\
  void* create_state(hrun::Admin::CreateTaskStateTask *task, const char *state_name) {\
    hrun::TaskState *exec = reinterpret_cast<hrun::TaskState*>(\
        new TYPE_UNWRAP(TRAIT_CLASS)());\
    exec->Init(task->id_, state_name);\
    RunContext rctx(0);\
    exec->Run(hrun::TaskMethod::kConstruct, task, rctx);\
    return exec;\
  }\
  const char* get_task_lib_name(void) { return TASK_NAME; }\
  bool is_hrun_task_ = true;\
  }

}   // namespace hrun

#endif  // HRUN_INCLUDE_HRUN_TASK_TASK_H_
