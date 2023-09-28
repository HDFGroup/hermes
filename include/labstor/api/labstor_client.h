//
// Created by lukemartinlogan on 6/23/23.
//

#ifndef LABSTOR_INCLUDE_LABSTOR_CLIENT_LABSTOR_CLIENT_H_
#define LABSTOR_INCLUDE_LABSTOR_CLIENT_LABSTOR_CLIENT_H_

#include <string>
#include "manager.h"
#include "labstor/queue_manager/queue_manager_client.h"

// Singleton macros
#define LABSTOR_CLIENT hshm::Singleton<labstor::Client>::GetInstance()
#define LABSTOR_CLIENT_T labstor::Client*

namespace labstor {

class Client : public ConfigurationManager {
 public:
  int data_;
  QueueManagerClient queue_manager_;
  std::atomic<u64> *unique_;
  u32 node_id_;

 public:
  /** Default constructor */
  Client() {}

  /** Initialize the client */
  Client* Create(std::string server_config_path = "",
                 std::string client_config_path = "",
                 bool server = false) {
    hshm::ScopedMutex lock(lock_, 1);
    if (is_initialized_) {
      return this;
    }
    mode_ = LabstorMode::kClient;
    is_being_initialized_ = true;
    ClientInit(std::move(server_config_path),
               std::move(client_config_path),
               server);
    is_initialized_ = true;
    is_being_initialized_ = false;
    return this;
  }

 private:
  /** Initialize client */
  void ClientInit(std::string server_config_path,
                  std::string client_config_path,
                  bool server) {
    LoadServerConfig(server_config_path);
    LoadClientConfig(client_config_path);
    LoadSharedMemory(server);
    queue_manager_.ClientInit(main_alloc_,
                              header_->queue_manager_,
                              header_->node_id_);
    if (!server) {
      HERMES_THREAD_MODEL->SetThreadModel(hshm::ThreadType::kPthread);
    }
  }

 public:
  /** Connect to a Daemon's shared memory */
  void LoadSharedMemory(bool server) {
    // Load shared-memory allocator
    auto mem_mngr = HERMES_MEMORY_MANAGER;
    if (!server) {
      mem_mngr->AttachBackend(hipc::MemoryBackendType::kPosixShmMmap,
                              server_config_.queue_manager_.shm_name_);
    }
    main_alloc_ = mem_mngr->GetAllocator(main_alloc_id_);
    header_ = main_alloc_->GetCustomHeader<LabstorShm>();
    unique_ = &header_->unique_;
    node_id_ = header_->node_id_;
  }

  /** Finalize Hermes explicitly */
  void Finalize() {}

  /** Create task node id */
  TaskNode MakeTaskNodeId() {
    return TaskId(header_->node_id_, unique_->fetch_add(1));;
  }

  /** Create a unique ID */
  TaskStateId MakeTaskStateId() {
    return TaskStateId(header_->node_id_, unique_->fetch_add(1));
  }

  /** Create a default-constructed task */
  template<typename TaskT, typename ...Args>
  HSHM_ALWAYS_INLINE
  TaskT* NewEmptyTask(hipc::Pointer &p) {
    return main_alloc_->NewObj<TaskT>(p, main_alloc_);
  }

  /** Allocate task */
  template<typename TaskT, typename ...Args>
  HSHM_ALWAYS_INLINE
  hipc::LPointer<TaskT> AllocateTask() {
    return main_alloc_->AllocateLocalPtr<TaskT>(sizeof(TaskT));
  }

  /** Construct task */
  template<typename TaskT, typename ...Args>
  HSHM_ALWAYS_INLINE
  void ConstructTask(TaskT *task, Args&& ...args) {
    return hipc::Allocator::ConstructObj<TaskT>(
        *task, main_alloc_, std::forward<Args>(args)...);
  }

  /** Create a task */
  template<typename TaskT, typename ...Args>
  HSHM_ALWAYS_INLINE
  LPointer<TaskT> NewTask(const TaskNode &task_node, Args&& ...args) {
    LPointer<TaskT> ptr = main_alloc_->NewObjLocal<TaskT>(
        main_alloc_, task_node, std::forward<Args>(args)...);
    if (ptr.ptr_ == nullptr) {
      throw std::runtime_error("Could not allocate buffer");
    }
    return ptr;
  }

  /** Create a root task */
  template<typename TaskT, typename ...Args>
  HSHM_ALWAYS_INLINE
  LPointer<TaskT> NewTaskRoot(Args&& ...args) {
    TaskNode task_node = MakeTaskNodeId();
    LPointer<TaskT> ptr = main_alloc_->NewObjLocal<TaskT>(
        main_alloc_, task_node, std::forward<Args>(args)...);
    if (ptr.ptr_ == nullptr) {
      throw std::runtime_error("Could not allocate buffer");
    }
    return ptr;
  }

  /** Destroy a task */
  template<typename TaskT>
  HSHM_ALWAYS_INLINE
  void DelTask(TaskT *task) {
    // TODO(llogan): verify leak
    task->delcnt_++;
    if (task->delcnt_ != 1) {
      HELOG(kFatal, "Freed task {} times: node={}, state={}. method={}",
            task->delcnt_.load(), task->task_node_, task->task_state_, task->method_)
    }
    main_alloc_->DelObj<TaskT>(task);
  }

  /** Destroy a task */
  template<typename TaskT>
  HSHM_ALWAYS_INLINE
  void DelTask(LPointer<TaskT> &task) {
    task->delcnt_++;
    if (task->delcnt_ != 1) {
      HELOG(kFatal, "Freed task {} times: node={}, state={}. method={}",
            task->delcnt_.load(), task->task_node_, task->task_state_, task->method_)
    }
    main_alloc_->DelObjLocal<TaskT>(task);
  }

  /** Destroy a task */
  template<typename TaskStateT, typename TaskT>
  HSHM_ALWAYS_INLINE
  void DelTask(TaskStateT *exec, TaskT *task) {
    exec->Del(task->method_, task);
  }

  /** Destroy a task */
  template<typename TaskStateT, typename TaskT>
  HSHM_ALWAYS_INLINE
  void DelTask(TaskStateT *exec, LPointer<TaskT> &task) {
    exec->Del(task->method_, task);
  }

  /** Get a queue by its ID */
  HSHM_ALWAYS_INLINE
  MultiQueue* GetQueue(const QueueId &queue_id) {
    return queue_manager_.GetQueue(queue_id);
  }

  /** Detect if a task is local or remote */
  HSHM_ALWAYS_INLINE
  bool IsRemote(Task *task) {
    if (task->domain_id_.IsNode()) {
      return task->domain_id_.GetId() != header_->node_id_;
    } else if (task->domain_id_.IsGlobal()) {
      return true;
    } else {
      return false;
    }
  }

  /** Allocate a buffer */
  HSHM_ALWAYS_INLINE
  LPointer<char> AllocateBuffer(size_t size) {
    LPointer<char> p = main_alloc_->AllocateLocalPtr<char>(size);
    if (p.ptr_ == nullptr) {
      throw std::runtime_error("Could not allocate buffer");
    }
    return p;
  }

  /** Convert pointer to char* */
  template<typename T = char>
  HSHM_ALWAYS_INLINE
  T* GetPrivatePointer(const hipc::Pointer &p) {
    return main_alloc_->Convert<T, hipc::Pointer>(p);
  }

  /** Free a buffer */
  HSHM_ALWAYS_INLINE
  void FreeBuffer(hipc::Pointer &p) {
    // TODO(llogan): verify leak
     main_alloc_->Free(p);
  }

  /** Free a buffer */
  HSHM_ALWAYS_INLINE
  void FreeBuffer(LPointer<char> &p) {
    // TODO(llogan): verify leak
     main_alloc_->FreeLocalPtr(p);
  }
};

/** A function which creates a new TaskNode value */
#define LABSTOR_TASK_NODE_ROOT(CUSTOM)\
  template<typename ...Args>\
  auto CUSTOM##Root(Args&& ...args) {\
    TaskNode task_node = LABSTOR_CLIENT->MakeTaskNodeId();\
    return CUSTOM(task_node, std::forward<Args>(args)...);\
  }

/** Fill in common default parameters for task client wrapper function */
#define LABSTOR_TASK_NODE_ADMIN_ROOT(CUSTOM)\
  template<typename ...Args>\
  hipc::LPointer<CUSTOM##Task> Async##CUSTOM##Alloc(const TaskNode &task_node,\
                                                    Args&& ...args) {\
    hipc::LPointer<CUSTOM##Task> task = LABSTOR_CLIENT->AllocateTask<CUSTOM##Task>();\
    Async##CUSTOM##Construct(task.ptr_, task_node, std::forward<Args>(args)...);\
    return task;\
  }\
  template<typename ...Args>\
  hipc::LPointer<CUSTOM##Task> Async##CUSTOM(const TaskNode &task_node, \
                                             Args&& ...args) {\
    hipc::LPointer<CUSTOM##Task> task = Async##CUSTOM##Alloc(task_node, std::forward<Args>(args)...);\
    MultiQueue *queue = LABSTOR_CLIENT->GetQueue(queue_id_);\
    queue->Emplace(task.ptr_->prio_, task.ptr_->lane_hash_, task.shm_);\
    return task;\
  }\
  template<typename ...Args>\
  hipc::LPointer<CUSTOM##Task> Async##CUSTOM##Emplace(MultiQueue *queue,\
                                                      const TaskNode &task_node,\
                                                      Args&& ...args) {\
    hipc::LPointer<CUSTOM##Task> task = Async##CUSTOM##Alloc(task_node, std::forward<Args>(args)...);\
    queue->Emplace(task.ptr_->prio_, task.ptr_->lane_hash_, task.shm_);\
    return task;\
  }\
  template<typename ...Args>\
  hipc::LPointer<CUSTOM##Task> Async##CUSTOM##Root(Args&& ...args) {\
    TaskNode task_node = LABSTOR_CLIENT->MakeTaskNodeId();\
    hipc::LPointer<CUSTOM##Task> task = Async##CUSTOM(task_node + 1, std::forward<Args>(args)...);\
    return task;\
  }

/** The default asynchronous method behavior */
#define LABSTOR_TASK_NODE_PUSH_ROOT(CUSTOM)\
  template<typename ...Args>\
  hipc::LPointer<CUSTOM##Task> Async##CUSTOM##Alloc(const TaskNode &task_node,\
                                                    Args&& ...args) {\
    hipc::LPointer<CUSTOM##Task> task = LABSTOR_CLIENT->AllocateTask<CUSTOM##Task>();\
    Async##CUSTOM##Construct(task.ptr_, task_node, std::forward<Args>(args)...);\
    return task;\
  }\
  template<typename ...Args>\
  hipc::LPointer<CUSTOM##Task> Async##CUSTOM(const TaskNode &task_node, \
                                             Args&& ...args) {\
    hipc::LPointer<CUSTOM##Task> task = Async##CUSTOM##Alloc(task_node, std::forward<Args>(args)...);\
    MultiQueue *queue = LABSTOR_CLIENT->GetQueue(queue_id_);\
    queue->Emplace(task.ptr_->prio_, task.ptr_->lane_hash_, task.shm_);\
    return task;\
  }\
  template<typename ...Args>\
  hipc::LPointer<CUSTOM##Task> Async##CUSTOM##Emplace(MultiQueue *queue,\
                                                      const TaskNode &task_node,\
                                                      Args&& ...args) {\
    hipc::LPointer<CUSTOM##Task> task = Async##CUSTOM##Alloc(task_node, std::forward<Args>(args)...);\
    queue->Emplace(task.ptr_->prio_, task.ptr_->lane_hash_, task.shm_);\
    return task;\
  }\
  template<typename ...Args>\
  hipc::LPointer<labpq::TypedPushTask<CUSTOM##Task>> Async##CUSTOM##Root(Args&& ...args) {\
    TaskNode task_node = LABSTOR_CLIENT->MakeTaskNodeId();\
    hipc::LPointer<CUSTOM##Task> task = Async##CUSTOM##Alloc(task_node + 1, std::forward<Args>(args)...);\
    hipc::LPointer<labpq::TypedPushTask<CUSTOM##Task>> push_task =\
      LABSTOR_PROCESS_QUEUE->AsyncPush<CUSTOM##Task>(task_node,\
                                                     DomainId::GetLocal(),\
                                                     task);\
      return push_task;\
  }

}  // namespace labstor

static inline bool TRANSPARENT_LABSTOR() {
  if (!LABSTOR_CLIENT->IsInitialized() &&
      !LABSTOR_CLIENT->IsBeingInitialized() &&
      !LABSTOR_CLIENT->IsTerminated()) {
    LABSTOR_CLIENT->Create();
    LABSTOR_CLIENT->is_transparent_ = true;
    return true;
  }
  return false;
}

#define HASH_TO_NODE_ID(hash) (1 + ((hash) % LABSTOR_CLIENT->GetNumNodes()))

#endif  // LABSTOR_INCLUDE_LABSTOR_CLIENT_LABSTOR_CLIENT_H_
