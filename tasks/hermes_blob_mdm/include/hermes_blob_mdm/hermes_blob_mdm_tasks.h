//
// Created by lukemartinlogan on 8/14/23.
//

#ifndef LABSTOR_TASKS_HERMES_BLOB_MDM_INCLUDE_HERMES_BLOB_MDM_HERMES_BLOB_MDM_TASKS_H_
#define LABSTOR_TASKS_HERMES_BLOB_MDM_INCLUDE_HERMES_BLOB_MDM_HERMES_BLOB_MDM_TASKS_H_

#include "labstor/api/labstor_client.h"
#include "labstor/task_registry/task_lib.h"
#include "labstor_admin/labstor_admin.h"
#include "labstor/queue_manager/queue_manager_client.h"
#include "hermes/hermes_types.h"
#include "bdev/bdev.h"
#include "labstor/api/labstor_client.h"
#include "proc_queue/proc_queue.h"

namespace hermes::blob_mdm {

#include "hermes_blob_mdm_methods.h"
#include "labstor/labstor_namespace.h"

using labstor::Task;
using labstor::TaskFlags;
using labstor::DataTransfer;

/** Phases of the construct task */
using labstor::Admin::CreateTaskStatePhase;
class ConstructTaskPhase : public CreateTaskStatePhase {
 public:
  TASK_METHOD_T kCreateTaskStates = kLast + 0;
  TASK_METHOD_T kWaitForTaskStates = kLast + 1;
};

/**
 * A task to create hermes_mdm
 * */
using labstor::Admin::CreateTaskStateTask;
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

/** Set the BUCKET MDM ID */
struct SetBucketMdmTask : public Task, TaskFlags<TF_SRL_SYM | TF_REPLICA> {
  IN TaskStateId bkt_mdm_;

  /** SHM default constructor */
  HSHM_ALWAYS_INLINE explicit
  SetBucketMdmTask(hipc::Allocator *alloc) : Task(alloc) {}

  /** Emplace constructor */
  HSHM_ALWAYS_INLINE explicit
  SetBucketMdmTask(hipc::Allocator *alloc,
                   const TaskNode &task_node,
                   const DomainId &domain_id,
                   const TaskStateId &state_id,
                   const TaskStateId &bkt_mdm) : Task(alloc) {
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
  }

  /** Destructor */
  ~SetBucketMdmTask() {}

  /** (De)serialize message call */
  template<typename Ar>
  void SerializeStart(Ar &ar) {
    task_serialize<Ar>(ar);
    ar(bkt_mdm_);
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
    lane_hash_ = std::hash<hshm::charbuf>{}(blob_name);
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
    // HSHM_DESTROY_AR(blob_name_)
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
  IN hipc::ShmArchive<hipc::charbuf> filename_;
  IN size_t page_size_;

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
              bitfield32_t flags,
              const Context &ctx,
              bitfield32_t task_flags) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kPutBlob;
    task_flags_ = task_flags;
    task_flags_.SetBits(TASK_COROUTINE);
    if (!blob_id.IsNull()) {
      lane_hash_ = blob_id.hash_;
      domain_id_ = domain_id;
    } else {
      lane_hash_ = std::hash<hshm::charbuf>{}(blob_name);
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
    flags_ = flags;
    HSHM_MAKE_AR(filename_, alloc, ctx.filename_);
    page_size_ = ctx.page_size_;
    HILOG(kDebug, "Construct PUT task for {}, while getting BlobId is {}",
          blob_name.str(), flags_.Any(HERMES_GET_BLOB_ID));
  }

  /** Destructor */
  ~PutBlobTask() {
    // HSHM_DESTROY_AR(blob_name_);
    // HSHM_DESTROY_AR(filename_);
    if (IsDataOwner()) {
      LABSTOR_CLIENT->FreeBuffer(data_);
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
    ar(tag_id_, blob_name_, blob_id_, blob_off_, data_size_, score_, flags_, filename_, page_size_);
  }

  /** Deserialize message call */
  template<typename Ar>
  void LoadStart(Ar &ar) {
    DataTransfer xfer;
    task_serialize<Ar>(ar);
    ar & xfer;
    data_ = HERMES_MEMORY_MANAGER->Convert<void, hipc::Pointer>(xfer.data_);
    ar(tag_id_, blob_name_, blob_id_, blob_off_, data_size_, score_, flags_, filename_, page_size_);
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
    labstor::LocalSerialize srl(group);
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
  IN hipc::ShmArchive<hipc::charbuf> filename_;
  IN size_t page_size_;
  INOUT ssize_t data_size_;
  IN bitfield32_t flags_;
  TEMP int phase_ = GetBlobPhase::kStart;
  TEMP hipc::ShmArchive<std::vector<bdev::ReadTask*>> bdev_reads_;
  TEMP PutBlobTask *stage_task_ = nullptr;

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
              bitfield32_t flags) : Task(alloc) {
    // Initialize task
    task_node_ = task_node;
    prio_ = TaskPrio::kLowLatency;
    task_state_ = state_id;
    method_ = Method::kGetBlob;
    task_flags_.SetBits(TASK_LOW_LATENCY);
    if (!blob_id.IsNull()) {
      lane_hash_ = blob_id.hash_;
      domain_id_ = domain_id;
    } else {
      lane_hash_ = std::hash<hshm::charbuf>{}(blob_name);
      domain_id_ = DomainId::GetNode(HASH_TO_NODE_ID(lane_hash_));
    }

    // Custom params
    tag_id_ = tag_id;
    blob_id_ = blob_id;
    blob_off_ = off;
    data_size_ = data_size;
    data_ = data;
    flags_ = flags;
    HSHM_MAKE_AR(blob_name_, alloc, blob_name);
    HSHM_MAKE_AR(filename_, alloc, ctx.filename_);
    page_size_ = ctx.page_size_;
  }

  /** Destructor */
  ~GetBlobTask() {
    // HSHM_DESTROY_AR(blob_name_);
    // HSHM_DESTROY_AR(filename_);
  }

  /** (De)serialize message call */
  template<typename Ar>
  void SaveStart(Ar &ar) {
    DataTransfer xfer(DT_RECEIVER_WRITE,
                      HERMES_MEMORY_MANAGER->Convert<char>(data_),
                      data_size_, domain_id_);
    task_serialize<Ar>(ar);
    ar & xfer;
    ar(tag_id_, blob_name_, blob_id_, blob_off_, data_size_, filename_, page_size_, flags_);
  }

  /** Deserialize message call */
  template<typename Ar>
  void LoadStart(Ar &ar) {
    DataTransfer xfer;
    task_serialize<Ar>(ar);
    ar & xfer;
    data_ = HERMES_MEMORY_MANAGER->Convert<void, hipc::Pointer>(xfer.data_);
    ar(tag_id_, blob_name_, blob_id_, blob_off_, data_size_, filename_, page_size_, flags_);
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
    labstor::LocalSerialize srl(group);
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
    labstor::LocalSerialize srl(group);
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
    labstor::LocalSerialize srl(group);
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
    lane_hash_ = std::hash<hshm::charbuf>{}(blob_name);
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
    // HSHM_DESTROY_AR(blob_name_)
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
    labstor::LocalSerialize srl(group);
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
    // HSHM_DESTROY_AR(blob_name_)
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
    labstor::LocalSerialize srl(group);
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
      lane_hash_ = std::hash<hshm::charbuf>{}(blob_name);
      domain_id_ = DomainId::GetNode(HASH_TO_NODE_ID(lane_hash_));
    }

    // Custom
    tag_id_ = tag_id;
    HSHM_MAKE_AR(blob_name_, alloc, blob_name)
    blob_id_ = blob_id;
  }

  /** Destructor */
  ~GetBlobSizeTask() {
    // HSHM_DESTROY_AR(blob_name_)
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
    labstor::LocalSerialize srl(group);
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
    labstor::LocalSerialize srl(group);
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
    // HSHM_DESTROY_AR(buffers_)
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
    labstor::LocalSerialize srl(group);
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
    // HSHM_DESTROY_AR(new_blob_name_)
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
    labstor::LocalSerialize srl(group);
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
    labstor::LocalSerialize srl(group);
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
                  const BlobId &blob_id) : Task(alloc) {
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
  }

   /** Create group */
  HSHM_ALWAYS_INLINE
  u32 GetGroup(hshm::charbuf &group) {
    labstor::LocalSerialize srl(group);
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
                     u32 node_id) : Task(alloc) {
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
    labstor::LocalSerialize srl(group);
    srl << tag_id_.unique_;
    srl << tag_id_.node_id_;
    return 0;
  }
};

}  // namespace hermes::blob_mdm

#endif //LABSTOR_TASKS_HERMES_BLOB_MDM_INCLUDE_HERMES_BLOB_MDM_HERMES_BLOB_MDM_TASKS_H_
