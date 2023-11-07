//
// Created by lukemartinlogan on 8/14/23.
//

#ifndef HRUN_TASKS_HERMES_BLOB_MDM_INCLUDE_HERMES_BLOB_MDM_HERMES_BLOB_MDM_TASKS_H_
#define HRUN_TASKS_HERMES_BLOB_MDM_INCLUDE_HERMES_BLOB_MDM_HERMES_BLOB_MDM_TASKS_H_

#include "hrun/api/hrun_client.h"
#include "hrun/task_registry/task_lib.h"
#include "hrun_admin/hrun_admin.h"
#include "hrun/queue_manager/queue_manager_client.h"
#include "hermes/hermes_types.h"
#include "bdev/bdev.h"
#include "hrun/api/hrun_client.h"
#include "proc_queue/proc_queue.h"

namespace hermes::blob_mdm {

#include "hermes_blob_mdm_methods.h"
#include "hrun/hrun_namespace.h"

using hrun::Task;
using hrun::TaskFlags;
using hrun::DataTransfer;

static inline u32 HashBlobName(const TagId &tag_id, const hshm::charbuf &blob_name) {
  u32 h2 = std::hash<TagId>{}(tag_id);
  u32 h1 = std::hash<hshm::charbuf>{}(blob_name);
  return std::hash<u32>{}(h1 ^ h2);
}

/** Phases of the construct task */
using hrun::Admin::CreateTaskStatePhase;
class ConstructTaskPhase : public CreateTaskStatePhase {
 public:
  TASK_METHOD_T kCreateTaskStates = kLast + 0;
  TASK_METHOD_T kWaitForTaskStates = kLast + 1;
};

/**
 * A task to create hermes_mdm
 * */
using hrun::Admin::CreateTaskStateTask;
struct ConstructTask : public CreateTaskStateTask {
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
                const std::vector<PriorityInfo> &queue_info)
      : CreateTaskStateTask(alloc, task_node, domain_id, state_name,
                            "hermes_blob_mdm", id, queue_info) {
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

/** Set the BUCKET MDM ID */
struct SetBucketMdmTask : public Task, TaskFlags<TF_SRL_SYM | TF_REPLICA> {
  IN TaskStateId bkt_mdm_;
  IN TaskStateId stager_mdm_;
  IN TaskStateId op_mdm_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  SetBucketMdmTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  SetBucketMdmTask(hipc::Allocator *alloc,
                   const TaskNode &task_node,
                   const DomainId &domain_id,
                   const TaskStateId &state_id,
                   const TaskStateId &bkt_mdm,
                   const TaskStateId &stager_mdm,
                   const TaskStateId &op_mdm) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kAdmin;
    task_state_ = state_id;
    method_ = Method::kSetBucketMdm;
    task_flags_.SetBits(TASK_LOW_LATENCY);
    domain_id_ = domain_id;

    // Custom params
    bkt_mdm_ = bkt_mdm;
    stager_mdm_ = stager_mdm;
    op_mdm_ = op_mdm;
  }

  /** Destructor */
  ~SetBucketMdmTask() {}

  /** Duplicate message */
  void Dup(hipc::Allocator *alloc, SetBucketMdmTask &other) {
    task_dup(other);
  }

  /** Process duplicate message output */
  void DupEnd(u32 replica, SetBucketMdmTask &dup_task) {
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(bkt_mdm_, stager_mdm_, op_mdm_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }

  /** Begin replication */
  void ReplicateStart(u32 count) {}

  /** Finalize replication */
  void ReplicateEnd() {}
};

/**====================================
 * Blob Operations
 * ===================================*/

/**
 * Get \a blob_name BLOB from \a bkt_id bucket
 * */
struct GetOrCreateBlobIdTask : public Task, TaskFlags<TF_SRL_SYM> {
  IN TagId tag_id_;
  IN hipc::ShmArchive<hipc::charbuf> blob_name_;
  OUT BlobId blob_id_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  GetOrCreateBlobIdTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  GetOrCreateBlobIdTask(hipc::Allocator *alloc,
                        const TaskNode &task_node,
                        const DomainId &domain_id,
                        const TaskStateId &state_id,
                        const TagId &tag_id,
                        const hshm::charbuf &blob_name) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = HashBlobName(tag_id, blob_name);
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kGetOrCreateBlobId;
    task_flags_.SetBits(TASK_LOW_LATENCY);
    domain_id_ = domain_id;

    // Custom
    tag_id_ = tag_id;
    HSHM_MAKE_AR(blob_name_, alloc, blob_name)
  }

  /** Destructor */
  ~GetOrCreateBlobIdTask() {
    HSHM_DESTROY_AR(blob_name_)
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(tag_id_, blob_name_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
    ar(blob_id_);
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

/** Phases for the put task */
class PutBlobPhase {
 public:
  TASK_METHOD_T kCreate = 0;
  TASK_METHOD_T kAllocate = 1;
  TASK_METHOD_T kWaitAllocate = 2;
  TASK_METHOD_T kModify = 3;
  TASK_METHOD_T kWaitModify = 4;
};

#define HERMES_BLOB_REPLACE BIT_OPT(u32, 0)
#define HERMES_BLOB_APPEND BIT_OPT(u32, 1)
#define HERMES_DID_STAGE_IN BIT_OPT(u32, 2)
#define HERMES_IS_FILE BIT_OPT(u32, 3)
#define HERMES_BLOB_DID_CREATE BIT_OPT(u32, 4)
#define HERMES_GET_BLOB_ID BIT_OPT(u32, 5)
#define HERMES_HAS_DERIVED BIT_OPT(u32, 6)
#define HERMES_USER_SCORE_STATIONARY BIT_OPT(u32, 7)

/** A task to put data in a blob */
struct PutBlobTask : public Task, TaskFlags<TF_SRL_ASYM_START | TF_SRL_SYM_END> {
  IN TagId tag_id_;
  IN hipc::ShmArchive<hipc::charbuf> blob_name_;
  IN size_t blob_off_;
  IN size_t data_size_;
  IN hipc::Pointer data_;
  IN float score_;
  IN bitfield32_t flags_;
  IN BlobId blob_id_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  PutBlobTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  PutBlobTask(hipc::Allocator *alloc,
              const TaskNode &task_node,
              const DomainId &domain_id,
              const TaskStateId &state_id,
              const TagId &tag_id,
              const hshm::charbuf &blob_name,
              const BlobId &blob_id,
              size_t blob_off,
              size_t data_size,
              const hipc::Pointer &data,
              float score,
              u32 flags,
              const Context &ctx,
              u32 task_flags) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kPutBlob;
    task_flags_ = bitfield32_t(task_flags);
    task_flags_.SetBits(TASK_COROUTINE);
    if (!blob_id.IsNull()) {
      lane_hash_ = blob_id.hash_;
      domain_id_ = domain_id;
    } else {
      lane_hash_ = HashBlobName(tag_id, blob_name);
      domain_id_ = DomainId::GetNode(HASH_TO_NODE_ID(lane_hash_));
    }

    // Custom params
    tag_id_ = tag_id;
    HSHM_MAKE_AR(blob_name_, alloc, blob_name);
    blob_id_ = blob_id;
    blob_off_ = blob_off;
    data_size_ = data_size;
    data_ = data;
    score_ = score;
    flags_ = bitfield32_t(flags | ctx.flags_.bits_);
  }

  /** Destructor */
  ~PutBlobTask() {
    HSHM_DESTROY_AR(blob_name_);
    if (IsDataOwner()) {
      HRUN_CLIENT->FreeBuffer(data_);
    }
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SaveStart(Ar &ar) {
    DataTransfer xfer(DT_RECEIVER_READ,
                      HERMES_MEMORY_MANAGER->Convert<char>(data_),
                      data_size_, domain_id_);
    task_serialize<Ar>(ar);
    ar & xfer;
    ar(tag_id_, blob_name_, blob_id_, blob_off_, data_size_, score_, flags_);
  }

  /** Deserialize message call */
  template<typename Ar>
  void LoadStart(Ar &ar) {
    DataTransfer xfer;
    task_serialize<Ar>(ar);
    ar & xfer;
    data_ = HERMES_MEMORY_MANAGER->Convert<void, hipc::Pointer>(xfer.data_);
    ar(tag_id_, blob_name_, blob_id_, blob_off_, data_size_, score_, flags_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
    if (flags_.Any(HERMES_GET_BLOB_ID)) {
      ar(blob_id_);
    }
  }

   /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    hrun::LocalSerialize srl(group);
    srl << tag_id_.unique_;
    srl << tag_id_.node_id_;
    return 0;
  }
};

/** Phases for the get task */
class GetBlobPhase {
 public:
  TASK_METHOD_T kStart = 0;
  TASK_METHOD_T kWait = 1;
};

/** A task to get data from a blob */
struct GetBlobTask : public Task, TaskFlags<TF_SRL_ASYM_START | TF_SRL_SYM_END> {
  IN TagId tag_id_;
  IN hipc::ShmArchive<hipc::charbuf> blob_name_;
  INOUT BlobId blob_id_;
  IN size_t blob_off_;
  IN hipc::Pointer data_;
  INOUT ssize_t data_size_;
  IN bitfield32_t flags_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  GetBlobTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  GetBlobTask(hipc::Allocator *alloc,
              const TaskNode &task_node,
              const DomainId &domain_id,
              const TaskStateId &state_id,
              const TagId &tag_id,
              const hshm::charbuf &blob_name,
              const BlobId &blob_id,
              size_t off,
              ssize_t data_size,
              hipc::Pointer &data,
              const Context &ctx,
              u32 flags) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kGetBlob;
    task_flags_.SetBits(TASK_LOW_LATENCY | TASK_COROUTINE);
    if (!blob_id.IsNull()) {
      lane_hash_ = blob_id.hash_;
      domain_id_ = domain_id;
    } else {
      lane_hash_ = HashBlobName(tag_id, blob_name);
      domain_id_ = DomainId::GetNode(HASH_TO_NODE_ID(lane_hash_));
    }

    // Custom params
    tag_id_ = tag_id;
    blob_id_ = blob_id;
    blob_off_ = off;
    data_size_ = data_size;
    data_ = data;
    flags_ = bitfield32_t(flags | ctx.flags_.bits_);
    HSHM_MAKE_AR(blob_name_, alloc, blob_name);
  }

  /** Convert data to a data structure */
  template<typename T>
  HSHM_ALWAYS_INLINE
  void Get(T &obj) {
    char *data = HRUN_CLIENT->GetDataPointer(data_);
    std::stringstream ss(std::string(data, data_size_));
    cereal::BinaryInputArchive ar(ss);
    ar >> obj;
  }

  /** Convert data to a data structure */
  template<typename T>
  HSHM_ALWAYS_INLINE
  T Get() {
    T obj;
    return Get(obj);
  }

  /** Destructor */
  ~GetBlobTask() {
    HSHM_DESTROY_AR(blob_name_);
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SaveStart(Ar &ar) {
    DataTransfer xfer(DT_RECEIVER_WRITE,
                      HERMES_MEMORY_MANAGER->Convert<char>(data_),
                      data_size_, domain_id_);
    task_serialize<Ar>(ar);
    ar & xfer;
    ar(tag_id_, blob_name_, blob_id_, blob_off_, data_size_, flags_);
  }

  /** Deserialize message call */
  template<typename Ar>
  void LoadStart(Ar &ar) {
    DataTransfer xfer;
    task_serialize<Ar>(ar);
    ar & xfer;
    data_ = HERMES_MEMORY_MANAGER->Convert<void, hipc::Pointer>(xfer.data_);
    ar(tag_id_, blob_name_, blob_id_, blob_off_, data_size_, flags_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
    if (flags_.Any(HERMES_GET_BLOB_ID)) {
      ar(blob_id_);
    }
  }

   /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    hrun::LocalSerialize srl(group);
    srl << tag_id_.unique_;
    srl << tag_id_.node_id_;
    return 0;
  }
};

/** A task to tag a blob */
struct TagBlobTask : public Task, TaskFlags<TF_SRL_SYM> {
  IN TagId tag_id_;
  IN BlobId blob_id_;
  IN TagId tag_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  TagBlobTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  TagBlobTask(hipc::Allocator *alloc,
              const TaskNode &task_node,
              const DomainId &domain_id,
              const TaskStateId &state_id,
              const TagId &tag_id,
              const BlobId &blob_id,
              const TagId &tag) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = blob_id.hash_;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kTagBlob;
    task_flags_.SetBits(TASK_LOW_LATENCY);
    domain_id_ = domain_id;

    // Custom
    tag_id_ = tag_id;
    blob_id_ = blob_id;
    tag_ = tag;
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(blob_id_, tag_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
  }

   /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    hrun::LocalSerialize srl(group);
    srl << tag_id_.unique_;
    srl << tag_id_.node_id_;
    return 0;
  }
};

/**
 * Check if blob has a tag
 * */
struct BlobHasTagTask : public Task, TaskFlags<TF_SRL_SYM> {
  IN TagId tag_id_;
  IN BlobId blob_id_;
  IN TagId tag_;
  OUT bool has_tag_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  BlobHasTagTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  BlobHasTagTask(hipc::Allocator *alloc,
                 const TaskNode &task_node,
                 const DomainId &domain_id,
                 const TaskStateId &state_id,
                 const TagId &tag_id,
                 const BlobId &blob_id,
                 const TagId &tag) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    task_node_ = task_node;
    lane_hash_ = blob_id.hash_;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kBlobHasTag;
    task_flags_.SetBits(TASK_LOW_LATENCY);
    domain_id_ = domain_id;

    // Custom
    tag_id_ = tag_id;
    blob_id_ = blob_id;
    tag_ = tag;
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(tag_id_, blob_id_, tag_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
    ar(has_tag_);
  }

   /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    hrun::LocalSerialize srl(group);
    srl << tag_id_.unique_;
    srl << tag_id_.node_id_;
    return 0;
  }
};

/**
 * Get \a blob_name BLOB from \a bkt_id bucket
 * */
struct GetBlobIdTask : public Task, TaskFlags<TF_SRL_SYM> {
  IN TagId tag_id_;
  IN hipc::ShmArchive<hipc::charbuf> blob_name_;
  OUT BlobId blob_id_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  GetBlobIdTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  GetBlobIdTask(hipc::Allocator *alloc,
                const TaskNode &task_node,
                const DomainId &domain_id,
                const TaskStateId &state_id,
                const TagId &tag_id,
                const hshm::charbuf &blob_name) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = HashBlobName(tag_id, blob_name);
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kGetBlobId;
    task_flags_.SetBits(TASK_LOW_LATENCY);
    domain_id_ = domain_id;

    // Custom
    tag_id_ = tag_id;
    HSHM_MAKE_AR(blob_name_, alloc, blob_name)
  }

  /** Destructor */
  ~GetBlobIdTask() {
    HSHM_DESTROY_AR(blob_name_)
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(tag_id_, blob_name_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
    ar(blob_id_);
  }

   /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    hrun::LocalSerialize srl(group);
    srl << tag_id_.unique_;
    srl << tag_id_.node_id_;
    return 0;
  }
};

/**
 * Get \a blob_name BLOB name from \a blob_id BLOB id
 * */
struct GetBlobNameTask : public Task, TaskFlags<TF_SRL_SYM> {
  IN TagId tag_id_;
  IN BlobId blob_id_;
  OUT hipc::ShmArchive<hipc::string> blob_name_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  GetBlobNameTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  GetBlobNameTask(hipc::Allocator *alloc,
                  const TaskNode &task_node,
                  const DomainId &domain_id,
                  const TaskStateId &state_id,
                  const TagId &tag_id,
                  const BlobId &blob_id) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = blob_id.hash_;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kGetBlobName;
    task_flags_.SetBits(TASK_LOW_LATENCY);
    domain_id_ = domain_id;

    // Custom
    tag_id_ = tag_id;
    blob_id_ = blob_id;
    HSHM_MAKE_AR0(blob_name_, alloc)
  }

  /** Destructor */
  ~GetBlobNameTask() {
    HSHM_DESTROY_AR(blob_name_)
  };

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(tag_id_, blob_id_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
    ar(blob_name_);
  }

   /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    hrun::LocalSerialize srl(group);
    srl << tag_id_.unique_;
    srl << tag_id_.node_id_;
    return 0;
  }
};

/** Get \a score from \a blob_id BLOB id */
struct GetBlobSizeTask : public Task, TaskFlags<TF_SRL_SYM> {
  IN TagId tag_id_;
  IN hipc::ShmArchive<hipc::charbuf> blob_name_;
  IN BlobId blob_id_;
  OUT size_t size_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  GetBlobSizeTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  GetBlobSizeTask(hipc::Allocator *alloc,
                  const TaskNode &task_node,
                  const DomainId &domain_id,
                  const TaskStateId &state_id,
                  const TagId &tag_id,
                  const hshm::charbuf &blob_name,
                  const BlobId &blob_id) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kGetBlobSize;
    task_flags_.SetBits(TASK_LOW_LATENCY);
    if (!blob_id.IsNull()) {
      lane_hash_ = blob_id.hash_;
      domain_id_ = domain_id;
    } else {
      lane_hash_ = HashBlobName(tag_id, blob_name);
      domain_id_ = DomainId::GetNode(HASH_TO_NODE_ID(lane_hash_));
    }

    // Custom
    tag_id_ = tag_id;
    HSHM_MAKE_AR(blob_name_, alloc, blob_name)
    blob_id_ = blob_id;
  }

  /** Destructor */
  ~GetBlobSizeTask() {
    HSHM_DESTROY_AR(blob_name_)
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(tag_id_, blob_name_, blob_id_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
    ar(size_);
  }

   /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    hrun::LocalSerialize srl(group);
    srl << tag_id_.unique_;
    srl << tag_id_.node_id_;
    return 0;
  }
};

/** Get \a score from \a blob_id BLOB id */
struct GetBlobScoreTask : public Task, TaskFlags<TF_SRL_SYM> {
  IN TagId tag_id_;
  IN BlobId blob_id_;
  OUT float score_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  GetBlobScoreTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  GetBlobScoreTask(hipc::Allocator *alloc,
                   const TaskNode &task_node,
                   const DomainId &domain_id,
                   const TaskStateId &state_id,
                   const TagId &tag_id,
                   const BlobId &blob_id) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = blob_id.hash_;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kGetBlobScore;
    task_flags_.SetBits(TASK_LOW_LATENCY);
    domain_id_ = domain_id;

    // Custom
    tag_id_ = tag_id;
    blob_id_ = blob_id;
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(tag_id_, blob_id_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
    ar(score_);
  }

   /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    hrun::LocalSerialize srl(group);
    srl << tag_id_.unique_;
    srl << tag_id_.node_id_;
    return 0;
  }
};

/** Get \a blob_id blob's buffers */
struct GetBlobBuffersTask : public Task, TaskFlags<TF_SRL_SYM> {
  IN TagId tag_id_;
  IN BlobId blob_id_;
  OUT hipc::ShmArchive<hipc::vector<BufferInfo>> buffers_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  GetBlobBuffersTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  GetBlobBuffersTask(hipc::Allocator *alloc,
                     const TaskNode &task_node,
                     const DomainId &domain_id,
                     const TaskStateId &state_id,
                     const TagId &tag_id,
                     const BlobId &blob_id) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = blob_id.hash_;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kGetBlobBuffers;
    task_flags_.SetBits(TASK_LOW_LATENCY);
    domain_id_ = domain_id;

    // Custom
    tag_id_ = tag_id;
    blob_id_ = blob_id;
    HSHM_MAKE_AR0(buffers_, alloc)
  }

  /** Destructor */
  ~GetBlobBuffersTask() {
    HSHM_DESTROY_AR(buffers_)
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(tag_id_, blob_id_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
    ar(buffers_);
  }

   /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    hrun::LocalSerialize srl(group);
    srl << tag_id_.unique_;
    srl << tag_id_.node_id_;
    return 0;
  }
};

/**
 * Rename \a blob_id blob to \a new_blob_name new blob name
 * in \a bkt_id bucket.
 * */
struct RenameBlobTask : public Task, TaskFlags<TF_SRL_SYM> {
  IN TagId tag_id_;
  IN BlobId blob_id_;
  IN hipc::ShmArchive<hipc::charbuf> new_blob_name_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  RenameBlobTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  RenameBlobTask(hipc::Allocator *alloc,
                 const TaskNode &task_node,
                 const DomainId &domain_id,
                 const TaskStateId &state_id,
                 const TagId &tag_id,
                 const BlobId &blob_id,
                 const hshm::charbuf &new_blob_name) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = blob_id.hash_;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kRenameBlob;
    task_flags_.SetBits(TASK_LOW_LATENCY);
    domain_id_ = domain_id;

    // Custom
    tag_id_ = tag_id;
    blob_id_ = blob_id;
    HSHM_MAKE_AR(new_blob_name_, alloc, new_blob_name)
  }

  /** Destructor */
  ~RenameBlobTask() {
    HSHM_DESTROY_AR(new_blob_name_)
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(tag_id_, blob_id_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
    ar(new_blob_name_);
  }

   /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    hrun::LocalSerialize srl(group);
    srl << tag_id_.unique_;
    srl << tag_id_.node_id_;
    return 0;
  }
};

/** A task to truncate a blob */
struct TruncateBlobTask : public Task, TaskFlags<TF_SRL_SYM> {
  IN TagId tag_id_;
  IN BlobId blob_id_;
  IN u64 size_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  TruncateBlobTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  TruncateBlobTask(hipc::Allocator *alloc,
                   const TaskNode &task_node,
                   const DomainId &domain_id,
                   const TaskStateId &state_id,
                   const TagId &tag_id,
                   const BlobId &blob_id,
                   u64 size) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = blob_id.hash_;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kTruncateBlob;
    task_flags_.SetBits(TASK_LOW_LATENCY);
    domain_id_ = domain_id;

    // Custom params
    tag_id_ = tag_id;
    blob_id_ = blob_id;
    size_ = size;
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(tag_id_, blob_id_, size_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
  }

   /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    hrun::LocalSerialize srl(group);
    srl << tag_id_.unique_;
    srl << tag_id_.node_id_;
    return 0;
  }
};

/** Phases of the destroy blob task */
struct DestroyBlobPhase {
  TASK_METHOD_T kFreeBuffers = 0;
  TASK_METHOD_T kWaitFreeBuffers = 1;
};

/** A task to destroy a blob */
struct DestroyBlobTask : public Task, TaskFlags<TF_SRL_SYM> {
  IN TagId tag_id_;
  IN BlobId blob_id_;
  IN bool update_size_;
  TEMP int phase_ = DestroyBlobPhase::kFreeBuffers;
  TEMP hipc::ShmArchive<std::vector<bdev::FreeTask *>> free_tasks_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  DestroyBlobTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  DestroyBlobTask(hipc::Allocator *alloc,
                  const TaskNode &task_node,
                  const DomainId &domain_id,
                  const TaskStateId &state_id,
                  const TagId &tag_id,
                  const BlobId &blob_id,
                  bool update_size = true) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = blob_id.hash_;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kDestroyBlob;
    task_flags_.SetBits(TASK_LOW_LATENCY);
    domain_id_ = domain_id;

    // Custom params
    tag_id_ = tag_id;
    blob_id_ = blob_id;
    update_size_ = update_size;
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(tag_id_, blob_id_, update_size_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
  }

   /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    hrun::LocalSerialize srl(group);
    srl << tag_id_.unique_;
    srl << tag_id_.node_id_;
    return 0;
  }
};

/** Phases of the destroy blob task */
struct ReorganizeBlobPhase {
  TASK_METHOD_T kGet = 0;
  TASK_METHOD_T kWaitGet = 1;
  TASK_METHOD_T kPut = 2;
};

/** A task to reorganize a blob's composition in the hierarchy */
struct ReorganizeBlobTask : public Task, TaskFlags<TF_SRL_SYM> {
  IN BlobId blob_id_;
  IN float score_;
  IN u32 node_id_;
  IN bool is_user_score_;
  TEMP int phase_ = ReorganizeBlobPhase::kGet;
  TEMP hipc::Pointer data_;
  TEMP size_t data_size_;
  TEMP GetBlobTask *get_task_;
  TEMP PutBlobTask *put_task_;
  TEMP TagId tag_id_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  ReorganizeBlobTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  ReorganizeBlobTask(hipc::Allocator *alloc,
                     const TaskNode &task_node,
                     const DomainId &domain_id,
                     const TaskStateId &state_id,
                     const TagId &tag_id,
                     const BlobId &blob_id,
                     float score,
                     u32 node_id,
                     bool is_user_score) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = blob_id.hash_;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kReorganizeBlob;
    task_flags_.SetBits(TASK_LOW_LATENCY | TASK_FIRE_AND_FORGET);
    domain_id_ = domain_id;

    // Custom params
    tag_id_ = tag_id;
    blob_id_ = blob_id;
    score_ = score;
    node_id_ = node_id;
    is_user_score_ = is_user_score;
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(tag_id_, blob_id_, score_, node_id_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
  }

   /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    hrun::LocalSerialize srl(group);
    srl << tag_id_.unique_;
    srl << tag_id_.node_id_;
    return 0;
  }
};

/** A task to reorganize a blob's composition in the hierarchy */
struct FlushDataTask : public Task, TaskFlags<TF_SRL_SYM | TF_REPLICA> {
  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  FlushDataTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  FlushDataTask(hipc::Allocator *alloc,
                const TaskNode &task_node,
                const TaskStateId &state_id) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kLongRunning;
    task_state_ = state_id;
    method_ = Method::kFlushData;
    task_flags_.SetBits(
        TASK_LANE_ALL |
        TASK_FIRE_AND_FORGET |
        TASK_LONG_RUNNING |
        TASK_COROUTINE |
        TASK_REMOTE_DEBUG_MARK);
    SetPeriodMs(5);  // TODO(llogan): don't hardcode this
    domain_id_ = DomainId::GetLocal();
  }

  /** Duplicate message */
  void Dup(hipc::Allocator *alloc, FlushDataTask &other) {
    task_dup(other);
  }

  /** Process duplicate message output */
  void DupEnd(u32 replica, FlushDataTask &dup_task) {
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SerializeEnd(u32 replica, Ar &ar) {
  }

  /** Begin replication */
  void ReplicateStart(u32 count) {}

  /** Finalize replication */
  void ReplicateEnd() {}

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

/** A task to collect blob metadata */
struct PollBlobMetadataTask : public Task, TaskFlags<TF_SRL_SYM_START | TF_SRL_ASYM_END | TF_REPLICA> {
  TEMP hipc::ShmArchive<hipc::string> my_blob_mdm_;
  TEMP hipc::ShmArchive<hipc::vector<hipc::string>> blob_mdms_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  PollBlobMetadataTask(hipc::Allocator *alloc) : Task(alloc) {
    HSHM_MAKE_AR0(blob_mdms_, alloc)
  }

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  PollBlobMetadataTask(hipc::Allocator *alloc,
                       const TaskNode &task_node,
                       const TaskStateId &state_id) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kPollBlobMetadata;
    task_flags_.SetBits(TASK_LANE_ALL);
    domain_id_ = DomainId::GetGlobal();

    // Custom params
    HSHM_MAKE_AR0(blob_mdms_, alloc)
    HSHM_MAKE_AR0(my_blob_mdm_, alloc)
  }

  /** Serialize blob info */
  void SerializeBlobMetadata(const std::vector<BlobInfo> &blob_info) {
    std::stringstream ss;
    cereal::BinaryOutputArchive ar(ss);
    ar << blob_info;
    (*my_blob_mdm_) = ss.str();
  }

  /** Deserialize blob info */
  void DeserializeBlobMetadata(const std::string &srl, std::vector<BlobInfo> &blob_mdms) {
    std::vector<BlobInfo> tmp_blob_mdms;
    std::stringstream ss(srl);
    cereal::BinaryInputArchive ar(ss);
    ar >> tmp_blob_mdms;
    for (BlobInfo &blob_info : tmp_blob_mdms) {
      blob_mdms.emplace_back(blob_info);
    }
  }

  /** Get combined output of all replicas */
  std::vector<BlobInfo> MergeBlobMetadata() {
    std::vector<BlobInfo> blob_mdms;
    for (const hipc::string &srl : *blob_mdms_) {
      DeserializeBlobMetadata(srl.str(), blob_mdms);
    }
    return blob_mdms;
  }

  /** Deserialize final query output */
  std::vector<BlobInfo> DeserializeBlobMetadata() {
    std::vector<BlobInfo> blob_mdms;
    DeserializeBlobMetadata((*my_blob_mdm_).str(), blob_mdms);
    return blob_mdms;
  }

  /** Destructor */
  ~PollBlobMetadataTask() {
    HSHM_DESTROY_AR(blob_mdms_)
    HSHM_DESTROY_AR(my_blob_mdm_)
  }

  /** Duplicate message */
  void Dup(hipc::Allocator *alloc, PollBlobMetadataTask &other) {
    task_dup(other);
    HSHM_MAKE_AR(blob_mdms_, alloc, *other.blob_mdms_)
    HSHM_MAKE_AR(my_blob_mdm_, alloc, *other.my_blob_mdm_)
  }

  /** Process duplicate message output */
  void DupEnd(u32 replica, PollBlobMetadataTask &dup_task) {
    (*blob_mdms_)[replica] = (*dup_task.my_blob_mdm_);
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(my_blob_mdm_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SaveEnd(Ar &ar) {
    ar(my_blob_mdm_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void LoadEnd(u32 replica, Ar &ar) {
    ar(my_blob_mdm_);
    DupEnd(replica, *this);
  }

  /** Begin replication */
  void ReplicateStart(u32 count) {
    blob_mdms_->resize(count);
  }

  /** Finalize replication */
  void ReplicateEnd() {
    std::vector<BlobInfo> blob_mdms = MergeBlobMetadata();
    SerializeBlobMetadata(blob_mdms);
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

/** A task to collect blob metadata */
struct PollTargetMetadataTask : public Task, TaskFlags<TF_SRL_SYM_START | TF_SRL_ASYM_START | TF_REPLICA> {
  OUT hipc::ShmArchive<hipc::string> my_target_mdms_;
  TEMP hipc::ShmArchive<hipc::vector<hipc::string>> target_mdms_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  PollTargetMetadataTask(hipc::Allocator *alloc) : Task(alloc) {
    HSHM_MAKE_AR0(target_mdms_, alloc)
  }

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  PollTargetMetadataTask(hipc::Allocator *alloc,
                         const TaskNode &task_node,
                         const TaskStateId &state_id) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    lane_hash_ = 0;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kPollTargetMetadata;
    task_flags_.SetBits(TASK_COROUTINE);
    domain_id_ = DomainId::GetGlobal();

    // Custom params
    HSHM_MAKE_AR0(my_target_mdms_, alloc)
    HSHM_MAKE_AR0(target_mdms_, alloc)
  }

  /** Serialize target info */
  void SerializeTargetMetadata(const std::vector<TargetStats> &target_info) {
    std::stringstream ss;
    cereal::BinaryOutputArchive ar(ss);
    ar << target_info;
    (*my_target_mdms_) = ss.str();
  }

  /** Deserialize target info */
  void DeserializeTargetMetadata(const std::string &srl, std::vector<TargetStats> &target_mdms) {
    std::vector<TargetStats> tmp_target_mdms;
    std::stringstream ss(srl);
    cereal::BinaryInputArchive ar(ss);
    ar >> tmp_target_mdms;
    for (TargetStats &target_info : tmp_target_mdms) {
      target_mdms.emplace_back(target_info);
    }
  }

  /** Get combined output of all replicas */
  std::vector<TargetStats> MergeTargetMetadata() {
    std::vector<TargetStats> target_mdms;
    for (const hipc::string &srl : *target_mdms_) {
      DeserializeTargetMetadata(srl.str(), target_mdms);
    }
    return target_mdms;
  }

  /** Deserialize final query output */
  std::vector<TargetStats> DeserializeTargetMetadata() {
    std::vector<TargetStats> target_mdms;
    DeserializeTargetMetadata(my_target_mdms_->str(), target_mdms);
    return target_mdms;
  }

  /** Destructor */
  ~PollTargetMetadataTask() {
    HSHM_DESTROY_AR(my_target_mdms_)
    HSHM_DESTROY_AR(target_mdms_)
  }

  /** Duplicate message */
  void Dup(hipc::Allocator *alloc, PollTargetMetadataTask &other) {}

  /** Process duplicate message output */
  void DupEnd(u32 replica, PollTargetMetadataTask &dup_task) {
    (*target_mdms_)[replica] = (*dup_task.my_target_mdms_);
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(my_target_mdms_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void SaveEnd(Ar &ar) {
    ar(my_target_mdms_);
  }

  /** (De)serialize message return */
  template<typename Ar>
  void LoadEnd(u32 replica, Ar &ar) {
    ar(my_target_mdms_);
    DupEnd(replica, *this);
  }

  /** Begin replication */
  void ReplicateStart(u32 count) {
    target_mdms_->resize(count);
  }

  /** Finalize replication */
  void ReplicateEnd() {
    std::vector<TargetStats> target_mdms = MergeTargetMetadata();
    SerializeTargetMetadata(target_mdms);
  }

  /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    return TASK_UNORDERED;
  }
};

}  // namespace hermes::blob_mdm

#endif //HRUN_TASKS_HERMES_BLOB_MDM_INCLUDE_HERMES_BLOB_MDM_HERMES_BLOB_MDM_TASKS_H_
