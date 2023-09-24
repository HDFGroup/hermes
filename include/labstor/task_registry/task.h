//
// Created by lukemartinlogan on 6/23/23.
//

#ifndef LABSTOR_INCLUDE_LABSTOR_QUEUE_MANAGER_REQUEST_H_
#define LABSTOR_INCLUDE_LABSTOR_QUEUE_MANAGER_REQUEST_H_

#include "labstor/labstor_types.h"
#include "labstor/network/local_serialize.h"
#include <thallium.hpp>

namespace labstor {

class TaskLib;

/** This task reads a state */
#define TASK_READ BIT_OPT(u32, 0)
/** This task writes to a state */
#define TASK_WRITE BIT_OPT(u32, 1)
/** This task fundamentally updates a state */
#define TASK_UPDATE BIT_OPT(u32, 2)
/** This task is paused until a set of tasks complete */
#define TASK_BLOCKED BIT_OPT(u32, 3)
/** This task is latency-sensitive */
#define TASK_LOW_LATENCY BIT_OPT(u32, 4)
/** This task makes system calls and may hurt caching */
#define TASK_SYSCALL BIT_OPT(u32, 5)
/** This task does not depend on state */
#define TASK_STATELESS BIT_OPT(u32, 6)
/** This task does not depend on its position in the queue */
#define TASK_UNORDERED BIT_OPT(u32, 7)
/** This task has began execution */
#define TASK_HAS_STARTED BIT_OPT(u32, 8)
/** This task is completed */
#define TASK_COMPLETE BIT_OPT(u32, 9)
/** This task was marked completed outside of the worker thread */
#define TASK_MODULE_COMPLETE BIT_OPT(u32, 10)
/** This task is long-running */
#define TASK_LONG_RUNNING (BIT_OPT(u32, 11) | TASK_UNORDERED)
/** This task is fire and forget. Free when completed */
#define TASK_FIRE_AND_FORGET BIT_OPT(u32, 12)
/** This task should not be run at this time */
#define TASK_DISABLE_RUN BIT_OPT(u32, 13)
/** This task owns the data in the task */
#define TASK_DATA_OWNER BIT_OPT(u32, 14)
/** This task is marked */
#define TASK_MARKED BIT_OPT(u32, 15)
/** This task uses co-routine wait */
#define TASK_COROUTINE BIT_OPT(u32, 16)
/** This task uses argobot wait */
#define TASK_PREEMPTIVE BIT_OPT(u32, 17)

/** Used to define task methods */
#define TASK_METHOD_T static inline const u32

/** Used to indicate Yield to use */
#define TASK_YIELD_STD 0
#define TASK_YIELD_CO 1
#define TASK_YIELD_ABT 2

/** The baseline set of tasks */
struct TaskMethod {
  TASK_METHOD_T kConstruct = 0; /**< The constructor of the task */
  TASK_METHOD_T kDestruct = 1;  /**< The destructor of the task */
  TASK_METHOD_T kLast = 2;    /**< Where the next method should take place */
};

/**
 * Let's say we have an I/O request to a device
 * I/O requests + MD operations need to be controlled for correctness
 * Is there a case where root tasks from different TaskStates need to be ordered? No.
 * Tasks spawned from the same root task need to be keyed to the same worker stack
 * Tasks apart of the same task group need to be ordered
 * */

/** An identifier used for representing the location of a task in a task graph */
struct TaskNode {
  TaskId root_;         /**< The id of the root task */
  u32 node_depth_;      /**< The depth of the task in the task graph */

  /** Default constructor */
  HSHM_ALWAYS_INLINE
  TaskNode() = default;

  /** Emplace constructor for root task */
  HSHM_ALWAYS_INLINE
  TaskNode(TaskId root) {
    root_ = root;
    node_depth_ = 0;
  }

  /** Copy constructor */
  HSHM_ALWAYS_INLINE
  TaskNode(const TaskNode &other) {
    root_ = other.root_;
    node_depth_ = other.node_depth_;
  }

  /** Copy assignment operator */
  HSHM_ALWAYS_INLINE
  TaskNode& operator=(const TaskNode &other) {
    root_ = other.root_;
    node_depth_ = other.node_depth_;
    return *this;
  }

  /** Move constructor */
  HSHM_ALWAYS_INLINE
  TaskNode(TaskNode &&other) noexcept {
    root_ = other.root_;
    node_depth_ = other.node_depth_;
  }

  /** Move assignment operator */
  HSHM_ALWAYS_INLINE
  TaskNode& operator=(TaskNode &&other) noexcept {
    root_ = other.root_;
    node_depth_ = other.node_depth_;
    return *this;
  }

  /** Addition operator*/
  HSHM_ALWAYS_INLINE
  TaskNode operator+(int i) const {
    TaskNode ret;
    ret.root_ = root_;
    ret.node_depth_ = node_depth_ + i;
    return ret;
  }

  /** Null task node */
  HSHM_ALWAYS_INLINE
  static TaskNode GetNull() {
    TaskNode ret;
    ret.root_ = TaskId::GetNull();
    ret.node_depth_ = 0;
    return ret;
  }

  /** Check if null */
  HSHM_ALWAYS_INLINE
  bool IsNull() const {
    return root_.IsNull();
  }

  /** Check if the root task */
  HSHM_ALWAYS_INLINE
  bool IsRoot() const {
    return node_depth_ == 0;
  }

  /** Serialization*/
  template<typename Ar>
  void serialize(Ar &ar) {
    ar(root_, node_depth_);
  }
};

/** Allow TaskNode to be printed as strings */
static inline std::ostream &operator<<(std::ostream &os, const TaskNode &obj) {
  return os << obj.root_ << "/" << std::to_string(obj.node_depth_);
}

/** This task uses SerializeStart */
#define TF_SRL_SYM_START BIT_OPT(u32, 0)
/** This task uses SaveStart + LoadStart */
#define TF_SRL_ASYM_START BIT_OPT(u32, 1)
/** This task uses SerializeEnd */
#define TF_SRL_SYM_END BIT_OPT(u32, 2)
/** This task uses SaveEnd + LoadEnd */
#define TF_SRL_ASYM_END BIT_OPT(u32, 3)
/** This task uses symmetric serialization */
#define TF_SRL_SYM (TF_SRL_SYM_START | TF_SRL_SYM_END)
/** This task uses asymmetric serialization */
#define TF_SRL_ASYM (TF_SRL_ASYM_START | TF_SRL_ASYM_END)
/** This task uses replication */
#define TF_REPLICA BIT_OPT(u32, 31)
/** This task is intended to be used only locally */
#define TF_LOCAL BIT_OPT(u32, 5)

/** All tasks inherit this to easily check if a class is a task using SFINAE */
class IsTask {};
/** The type of a compile-time task flag */
#define TASK_FLAG_T constexpr inline static bool
/** Determine this is a task */
#define IS_TASK(T) \
  std::is_base_of_v<labstor::IsTask, T>
/** Determine this task supports serialization */
#define IS_SRL(T) \
  T::SUPPORTS_SRL
/** Determine this task uses SerializeStart */
#define USES_SRL_START(T) \
  T::SRL_SYM_START
/** Determine this task uses SerializeEnd */
#define USES_SRL_END(T) \
  T::SRL_SYM_END
/** Call replica start if applicable */
template<typename T>
constexpr inline void CALL_REPLICA_START(u32 count, T *task) {
  if constexpr(T::REPLICA) {
    task->ReplicateStart(count);
  }
}
/** Call replica end if applicable */
template<typename T>
constexpr inline void CALL_REPLICA_END(T *task) {
  if constexpr(T::REPLICA) {
    task->ReplicateEnd();
  }
}

/** Compile-time flags indicating task methods and operation support */
template<u32 FLAGS>
struct TaskFlags : public IsTask {
 public:
  TASK_FLAG_T SUPPORTS_SRL = FLAGS & (TF_SRL_SYM | TF_SRL_ASYM);
  TASK_FLAG_T SRL_SYM_START = FLAGS & TF_SRL_SYM_START;
  TASK_FLAG_T SRL_SYM_END = FLAGS & TF_SRL_SYM_END;
  TASK_FLAG_T REPLICA = FLAGS & TF_REPLICA;
};

/** The type of a compile-time task flag */
#define TASK_PRIO_T constexpr inline static int

/** Prioritization of tasks */
class TaskPrio {
 public:
  TASK_PRIO_T kAdmin = 0;
  TASK_PRIO_T kLongRunning = 1;
  TASK_PRIO_T kLowLatency = 2;
};

/** Context passed to the Run method of a task */
struct RunContext {
  u32 lane_id_;  /**< The lane id of the task */
  bctx::transfer_t jmp_;  /**< Current execution state of the task (runtime) */
  size_t stack_size_ = KILOBYTES(64);  /**< The size of the stack for the task (runtime) */
  void *stack_ptr_;                    /**< The pointer to the stack (runtime) */
  TaskLib *exec_;

  /** Default constructor */
  RunContext() {}

  /** Emplace constructor */
  RunContext(u32 lane_id) : lane_id_(lane_id) {}
};

/** A generic task base class */
struct Task : public hipc::ShmContainer {
 SHM_CONTAINER_TEMPLATE((Task), (Task))
 public:
  TaskStateId task_state_;     /**< The unique name of a task state */
  TaskNode task_node_;         /**< The unique ID of this task in the graph */
  DomainId domain_id_;         /**< The nodes that the task should run on */
  u32 prio_;                   /**< An indication of the priority of the request */
  u32 lane_hash_;              /**< Determine the lane a task is keyed to */
  u32 method_;                 /**< The method to call in the state */
  bitfield32_t task_flags_;    /**< Properties of the task */
  RunContext ctx_;

  /**====================================
   * Task Helpers
   * ===================================*/

  /** Set task as externally complete */
  HSHM_ALWAYS_INLINE void SetModuleComplete() {
    task_flags_.SetBits(TASK_MODULE_COMPLETE);
  }

  /** Check if a task marked complete externally */
  HSHM_ALWAYS_INLINE bool IsModuleComplete() {
    return task_flags_.Any(TASK_MODULE_COMPLETE);
  }

  /** Check if a task is fire & forget */
  HSHM_ALWAYS_INLINE bool IsFireAndForget() {
    return task_flags_.Any(TASK_FIRE_AND_FORGET);
  }

  /** Unset fire & forget */
  HSHM_ALWAYS_INLINE void UnsetFireAndForget() {
    task_flags_.UnsetBits(TASK_FIRE_AND_FORGET);
  }

  /** Check if task is long running */
  HSHM_ALWAYS_INLINE bool IsLongRunning() {
    return task_flags_.Any(TASK_LONG_RUNNING);
  }

  /** Check if task is unordered */
  HSHM_ALWAYS_INLINE bool IsUnordered() {
    return task_flags_.Any(TASK_UNORDERED);
  }

  /** Set task as unordered */
  HSHM_ALWAYS_INLINE bool SetUnordered() {
    return task_flags_.Any(TASK_UNORDERED);
  }

  /** Set task as complete */
  HSHM_ALWAYS_INLINE void SetComplete() {
    task_flags_.SetBits(TASK_MODULE_COMPLETE | TASK_COMPLETE);
  }

  /** Check if task is complete */
  HSHM_ALWAYS_INLINE bool IsComplete() {
    return task_flags_.Any(TASK_COMPLETE);
  }

  /** Disable the running of a task */
  HSHM_ALWAYS_INLINE void DisableRun() {
    task_flags_.SetBits(TASK_DISABLE_RUN);
  }

  /** Check if running task is disable */
  HSHM_ALWAYS_INLINE bool IsRunDisabled() {
    return task_flags_.Any(TASK_DISABLE_RUN);
  }

  /** Set task as data owner */
  HSHM_ALWAYS_INLINE void SetDataOwner() {
    task_flags_.SetBits(TASK_DATA_OWNER);
  }

  /** Check if task is data owner */
  HSHM_ALWAYS_INLINE bool IsDataOwner() {
    return task_flags_.Any(TASK_DATA_OWNER);
  }

  /** Unset task as data owner */
  HSHM_ALWAYS_INLINE void UnsetDataOwner() {
    task_flags_.UnsetBits(TASK_DATA_OWNER);
  }

  /** Set this task as started */
  HSHM_ALWAYS_INLINE void SetStarted() {
    task_flags_.SetBits(TASK_HAS_STARTED);
  }

  /** Set this task as started */
  HSHM_ALWAYS_INLINE void UnsetStarted() {
    task_flags_.UnsetBits(TASK_HAS_STARTED);
  }

  /** Check if task has started */
  HSHM_ALWAYS_INLINE bool IsStarted() {
    return task_flags_.Any(TASK_HAS_STARTED);
  }

  /** Set this task as started */
  HSHM_ALWAYS_INLINE void SetMarked() {
    task_flags_.SetBits(TASK_MARKED);
  }

  /** Set this task as started */
  HSHM_ALWAYS_INLINE void UnsetMarked() {
    task_flags_.UnsetBits(TASK_MARKED);
  }

  /** Check if task is marked */
  HSHM_ALWAYS_INLINE bool IsMarked() {
    return task_flags_.Any(TASK_MARKED);
  }

  /** Set this task as started */
  HSHM_ALWAYS_INLINE void UnsetLongRunning() {
    task_flags_.UnsetBits(TASK_LONG_RUNNING);
  }

  /** Set this task as blocking */
  HSHM_ALWAYS_INLINE bool IsCoroutine() {
    return task_flags_.Any(TASK_COROUTINE);
  }

  /** Set this task as blocking */
  HSHM_ALWAYS_INLINE bool IsPreemptive() {
    return task_flags_.Any(TASK_PREEMPTIVE);
  }

  /** Yield the task */
  template<int THREAD_MODEL = 0>
  HSHM_ALWAYS_INLINE
  void Yield() {
    if constexpr (THREAD_MODEL == TASK_YIELD_STD) {
      HERMES_THREAD_MODEL->Yield();
    } else if constexpr (THREAD_MODEL == TASK_YIELD_CO) {
      ctx_.jmp_ = bctx::jump_fcontext(ctx_.jmp_.fctx, nullptr);
    } else if constexpr (THREAD_MODEL == TASK_YIELD_ABT) {
      ABT_thread_yield();
    }
  }

  /** Wait for task to complete */
  template<int THREAD_MODEL = 0>
  void Wait() {
    while (!IsComplete()) {
//      for (int i = 0; i < 100000; ++i) {
//        if (IsComplete()) {
//          return;
//        }
//      }
      Yield<THREAD_MODEL>();
    }
  }

  /** Wait for task to complete */
  template<int THREAD_MODEL = 0>
  void Wait(Task *yield_task) {
    while (!IsComplete()) {
//      for (int i = 0; i < 100000; ++i) {
//        if (IsComplete()) {
//          return;
//        }
//      }
      yield_task->Yield<THREAD_MODEL>();
    }
  }

  /**====================================
   * Default Constructor
   * ===================================*/

  /** Default SHM constructor */
  HSHM_ALWAYS_INLINE explicit
  Task(hipc::Allocator *alloc) {
    shm_init_container(alloc);
  }

  /** SHM constructor */
  HSHM_ALWAYS_INLINE explicit
  Task(hipc::Allocator *alloc,
       const TaskNode &task_node) {
    shm_init_container(alloc);
    task_node_ = task_node;
  }

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  Task(hipc::Allocator *alloc,
       const TaskNode &task_node,
       const DomainId &domain_id,
       const TaskStateId &task_state,
       u32 lane_hash,
       u32 method,
       bitfield32_t task_flags) {
    shm_init_container(alloc);
    task_node_ = task_node;
    lane_hash_ = lane_hash;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = task_state;
    method_ = method;
    domain_id_ = domain_id;
    task_flags_ = task_flags;
  }

  /**====================================
   * Copy Constructors
   * ===================================*/

  /** SHM copy constructor */
  HSHM_ALWAYS_INLINE explicit Task(hipc::Allocator *alloc, const Task &other) {}

  /** SHM copy assignment operator */
  HSHM_ALWAYS_INLINE Task& operator=(const Task &other) {
    return *this;
  }

  /**====================================
   * Move Constructors
   * ===================================*/

  /** SHM move constructor. */
  HSHM_ALWAYS_INLINE Task(hipc::Allocator *alloc, Task &&other) noexcept {}

  /** SHM move assignment operator. */
  HSHM_ALWAYS_INLINE Task& operator=(Task &&other) noexcept {
    return *this;
  }

  /**====================================
   * Destructor
   * ===================================*/

  /** SHM destructor.  */
  HSHM_ALWAYS_INLINE void shm_destroy_main() {}

  /** Check if the Task is empty */
  HSHM_ALWAYS_INLINE bool IsNull() const { return false; }

  /** Sets this Task as empty */
  HSHM_ALWAYS_INLINE void SetNull() {}

 /**====================================
  * Serialization
  * ===================================*/
 template<typename Ar>
 void task_serialize(Ar &ar) {
   ar(task_state_, task_node_, domain_id_, lane_hash_, prio_, method_, task_flags_);
 }

  /**====================================
   * Grouping
   * ===================================*/

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

/** Decorator macros */
#define IN
#define OUT
#define INOUT
#define TEMP

}  // namespace labstor

#endif  // LABSTOR_INCLUDE_LABSTOR_QUEUE_MANAGER_REQUEST_H_
