//
// Created by lukemartinlogan on 7/8/23.
//

#ifndef LABSTOR_TASKS_HERMES_INCLUDE_HERMES_HERMES_TYPES_H_
#define LABSTOR_TASKS_HERMES_INCLUDE_HERMES_HERMES_TYPES_H_

#include "labstor/labstor_types.h"
#include "labstor/task_registry/task_registry.h"
#include "labstor/api/labstor_client.h"
#include "status.h"
#include "statuses.h"

namespace hapi = hermes;

namespace hermes {

using labstor::TaskLib;
using labstor::TaskMethod;
using labstor::UniqueId;
using labstor::TaskStateId;
using labstor::DomainId;
using labstor::Task;
using labstor::TaskId;
using labstor::TaskNode;
using hshm::bitfield32_t;

/** Queue id */
using labstor::QueueId;

/** Queue for interprocess-communication */
using labstor::MultiQueue;

/** Unique blob id */
typedef UniqueId<100> BlobId;

/** Unique bucket id */
typedef UniqueId<101> BucketId;

/** Represents a tag */
typedef UniqueId<102> TagId;

/** Represetnts a storage target */
typedef TaskStateId TargetId;

/** Represents a trait */
typedef TaskStateId TraitId;

/** Different categories of traits */
enum class TraitType {
  kStagingTrait,
  kProducerOpTrait
};

/** Represents a blob  */
typedef hshm::charbuf Blob;

/** Represents a blob in shared memory */
typedef hipc::uptr<hipc::charbuf> IpcBlob;

/** Supported data placement policies */
enum class PlacementPolicy {
  kRandom,         /**< Random blob placement */
  kRoundRobin,     /**< Round-Robin (around devices) blob placement */
  kMinimizeIoTime, /**< LP-based blob placement, minimize I/O time */
  kNone,           /**< No Dpe for cases we want it disabled */
};

/** A class to convert placement policy enum value to string */
class PlacementPolicyConv {
 public:
  /** A function to return string representation of \a policy */
  static std::string to_str(PlacementPolicy policy) {
    switch (policy) {
      case PlacementPolicy::kRandom: {
        return "PlacementPolicy::kRandom";
      }
      case PlacementPolicy::kRoundRobin: {
        return "PlacementPolicy::kRoundRobin";
      }
      case PlacementPolicy::kMinimizeIoTime: {
        return "PlacementPolicy::kMinimizeIoTime";
      }
      case PlacementPolicy::kNone: {
        return "PlacementPolicy::kNone";
      }
    }
    return "PlacementPolicy::Invalid";
  }

  /** return enum value of \a policy  */
  static PlacementPolicy to_enum(const std::string &policy) {
    if (policy.find("Random") != std::string::npos) {
      return PlacementPolicy::kRandom;
    } else if (policy.find("RoundRobin") != std::string::npos) {
      return PlacementPolicy::kRoundRobin;
    } else if (policy.find("MinimizeIoTime") != std::string::npos) {
      return PlacementPolicy::kMinimizeIoTime;
    } else if (policy.find("None") != std::string::npos) {
      return PlacementPolicy::kNone;
    }
    return PlacementPolicy::kNone;
  }
};

/** Hermes API call context */
struct Context {
  /** Data placement engine */
  PlacementPolicy dpe_;

  /** The blob's score */
  float blob_score_;

  /** Page size to use for FS reads / writes*/
  size_t page_size_;

  /** Flags */
  bitfield32_t flags_;

  /** The node id the blob will be accessed from */
  u32 node_id_;

  Context()
  : dpe_(PlacementPolicy::kNone),
    blob_score_(1),
    node_id_(0) {}
};

/**
 * Represents the fraction of a blob to place
 * on a particular target during data placement
 * */
struct SubPlacement {
  size_t size_;   /**< Size (bytes) */
  TargetId tid_;  /**< Index in the target vector */

  SubPlacement() = default;

  explicit SubPlacement(size_t size, const TargetId &tid)
      : size_(size), tid_(tid) {}
};

/**
 * The organization of a particular blob in the storage
 * hierarchy during data placement.
 */
struct PlacementSchema {
  std::vector<SubPlacement> plcmnts_;

  void AddSubPlacement(size_t size, const TargetId &tid) {
    plcmnts_.emplace_back(size, tid);
  }

  void Clear() {
    plcmnts_.clear();
  }
};

/** The types of topologies */
enum class TopologyType {
  Local,
  Neighborhood,
  Global,

  kCount
};

/** The flushing mode */
enum class FlushingMode {
  kSync,
  kAsync
};

/** Convert flushing modes to strings */
class FlushingModeConv {
 public:
  static FlushingMode GetEnum(const std::string &str) {
    if (str == "kSync") {
      return FlushingMode::kSync;
    }
    if (str == "kAsync") {
      return FlushingMode::kAsync;
    }
    return FlushingMode::kAsync;
  }
};

/** A class with static constants */
#define CONST_T inline const static
class Constant {
 public:
  /** Hermes server environment variable */
  CONST_T char* kHermesServerConf = "HERMES_CONF";

  /** Hermes client environment variable */
  CONST_T char* kHermesClientConf = "HERMES_CLIENT_CONF";

  /** Hermes adapter mode environment variable */
  CONST_T char* kHermesAdapterMode = "HERMES_ADAPTER_MODE";

  /** Filesystem page size environment variable */
  CONST_T char* kHermesPageSize = "HERMES_PAGE_SIZE";

  /** Stop daemon environment variable */
  CONST_T char* kHermesStopDaemon = "HERMES_STOP_DAEMON";

  /** Maximum path length environment variable */
  CONST_T size_t kMaxPathLength = 4096;
};

/** Represents an allocated fraction of a target */
struct BufferInfo {
  TargetId tid_;        /**< The destination target */
  size_t t_slab_;       /**< The index of the slab in the target */
  size_t t_off_;        /**< Offset in the target */
  size_t t_size_;       /**< Size in the target */

  /** Serialization */
  template<typename Ar>
  void serialize(Ar &ar) {
    ar(tid_, t_slab_, t_off_, t_size_);
  }

  /** Default constructor */
  BufferInfo() = default;

  /** Primary constructor */
  BufferInfo(TaskStateId tid, size_t t_off, size_t t_size,
             size_t blob_off, size_t blob_size)
      : tid_(tid), t_off_(t_off), t_size_(t_size) {}

  /** Copy constructor */
  BufferInfo(const BufferInfo &other) {
    Copy(other);
  }

  /** Move constructor */
  BufferInfo(BufferInfo &&other) {
    Copy(other);
  }

  /** Copy assignment */
  BufferInfo& operator=(const BufferInfo &other) {
    Copy(other);
    return *this;
  }

  /** Move assignment */
  BufferInfo& operator=(BufferInfo &&other) {
    Copy(other);
    return *this;
  }

  /** Performs move/copy */
  void Copy(const BufferInfo &other) {
    tid_ = other.tid_;
    t_slab_ = other.t_slab_;
    t_off_ = other.t_off_;
    t_size_ = other.t_size_;
  }
};

/** Data structure used to store Blob information */
struct BlobInfo {
  TagId tag_id_;    /**< Tag the blob is on */
  BlobId blob_id_;  /**< Unique ID of the blob */
  hshm::charbuf name_;  /**< Name of the blob */
  std::vector<BufferInfo> buffers_;  /**< Set of buffers */
  std::vector<TagId> tags_;  /**< Set of tags */
  size_t blob_size_;      /**< The overall size of the blob */
  size_t max_blob_size_;  /**< The amount of space current buffers support */
  float score_;  /**< The priority of this blob */
  std::atomic<u32> access_freq_;  /**< Number of times blob accessed in epoch */
  u64 last_access_;  /**< Last time blob accessed */
  std::atomic<size_t> mod_count_;   /**< The number of times blob modified */
  std::atomic<size_t> last_flush_;  /**< The last mod that was flushed */

  /** Serialization */
  template<typename Ar>
  void serialize(Ar &ar) {
    ar(tag_id_, blob_id_, name_, buffers_, tags_, blob_size_, max_blob_size_,
       score_, access_freq_, last_access_, mod_count_, last_flush_);
  }

  /** Default constructor */
  BlobInfo() = default;

  /** Copy constructor */
  BlobInfo(const BlobInfo &other) {
    tag_id_ = other.tag_id_;
    blob_id_ = other.blob_id_;
    name_ = other.name_;
    buffers_ = other.buffers_;
    tags_ = other.tags_;
    blob_size_ = other.blob_size_;
    max_blob_size_ = other.max_blob_size_;
    score_ = other.score_;
    access_freq_ = other.access_freq_.load();
    last_access_ = other.last_access_;
    mod_count_ = other.mod_count_.load();
    last_flush_ = other.last_flush_.load();
  }

  /** Update modify stats */
  void UpdateWriteStats() {
    mod_count_.fetch_add(1);
    UpdateReadStats();
  }

  /** Update read stats */
  void UpdateReadStats() {
    last_access_ = GetTimeFromStartNs();
    access_freq_.fetch_add(1);
  }

  /** Get the time from start in nanoseconds */
  static u64 GetTimeFromStartNs() {
    struct timespec currentTime;
    clock_gettime(CLOCK_MONOTONIC, &currentTime);
    unsigned long long nanoseconds =
        currentTime.tv_sec * 1000000000ULL + currentTime.tv_nsec;
    return nanoseconds;
  }
};

/** Data structure used to store Bucket information */
struct TagInfo {
  TagId tag_id_;
  hshm::charbuf name_;
  std::list<BlobId> blobs_;
  std::list<Task*> traits_;
  size_t internal_size_;
  size_t page_size_;
  bool owner_;
};

/** The types of I/O that can be performed (for IoCall RPC) */
enum class IoType {
  kRead,
  kWrite,
  kNone
};

/** Indicates a PUT or GET for a particular blob */
struct IoStat {
  IoType type_;
  BlobId blob_id_;
  TagId tag_id_;
  size_t blob_size_;
  int rank_;

  /** Default constructor */
  IoStat() = default;

  /** Copy constructor */
  IoStat(const IoStat &other) {
    Copy(other);
  }

  /** Copy assignment */
  IoStat& operator=(const IoStat &other) {
    if (this != &other) {
      Copy(other);
    }
    return *this;
  }

  /** Move constructor */
  IoStat(IoStat &&other) {
    Copy(other);
  }

  /** Move assignment */
  IoStat& operator=(IoStat &&other) {
    if (this != &other) {
      Copy(other);
    }
    return *this;
  }

  /** Generic copy / move */
  HSHM_ALWAYS_INLINE void Copy(const IoStat &other) {
    type_ = other.type_;
    blob_id_ = other.blob_id_;
    tag_id_ = other.tag_id_;
    blob_size_ = other.blob_size_;
    rank_ = other.rank_;
  }

  /** Serialize */
  template<class Archive>
  void save(Archive &ar) const {
    int type = static_cast<int>(type_);
    u64 ids[2] = {blob_id_.unique_, tag_id_.unique_};
    u32 nodes[2] = {blob_id_.node_id_, tag_id_.node_id_};
    ar(type, ids[0], nodes[0], ids[1], nodes[1], blob_size_, rank_);
  }

  /** Deserialize */
  template<class Archive>
  void load(Archive &ar) {
    int type;
    ar(type,
       blob_id_.unique_,
       blob_id_.node_id_,
       tag_id_.unique_,
       tag_id_.node_id_,
       blob_size_,
       rank_);
    type_ = static_cast<IoType>(type);
  }
};

/** Used for debugging concurrency issues with locks */
enum LockOwners {
  kNone = 0,
  kMDM_Create = 1,
  kMDM_Update = 2,
  kMDM_Find = 3,
  kMDM_Find2 = 4,
  kMDM_Delete = 5,
  kFS_GetBaseAdapterMode = 6,
  kFS_GetAdapterMode = 7,
  kFS_GetAdapterPageSize = 8,
  kBORG_LocalEnqueueFlushes = 9,
  kBORG_LocalProcessFlushes = 10,
  kMDM_LocalGetBucketSize = 12,
  kMDM_LocalSetBucketSize = 13,
  kMDM_LocalLockBucket = 14,
  kMDM_LocalUnlockBucket = 15,
  kMDM_LocalClearBucket = 16,
  kMDM_LocalTagBlob = 17,
  kMDM_LocalBlobHasTag = 18,
  kMDM_LocalTryCreateBlob = 19,
  kMDM_LocalPutBlobMetadata = 20,
  kMDM_LocalGetBlobId = 21,
  kMDM_LocalGetBlobName = 22,
  kMDM_LocalGetBlobScore = 24,
  kMDM_LocalLockBlob = 26,
  kMDM_LocalUnlockBlob = 27,
  kMDM_LocalGetBlobBuffers = 28,
  kMDM_LocalRenameBlob = 29,
  kMDM_LocalDestroyBlob = 30,
  kMDM_LocalClear = 31,
  kMDM_LocalGetOrCreateTag = 32,
  kMDM_LocalGetTagId = 33,
  kMDM_LocalGetTagName = 34,
  kMDM_LocalRenameTag = 35,
  kMDM_LocalDestroyTag = 36,
  kMDM_LocalTagAddBlob = 37,
  kMDM_LocalTagRemoveBlob = 38,
  kMDM_LocalGroupByTag = 39,
  kMDM_LocalTagAddTrait = 40,
  kMDM_LocalTagGetTraits = 41,
  kMDM_LocalRegisterTrait = 42,
  kMDM_LocalGetTraitId = 43,
  kMDM_LocalGetTraitParams = 44,
  kMDM_GlobalGetTrait = 45,
  kMDM_AddIoStat = 46,
  kMDM_ClearIoStats = 47
};


}  // namespace hermes

#endif  // LABSTOR_TASKS_HERMES_INCLUDE_HERMES_HERMES_TYPES_H_
