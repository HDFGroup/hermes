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

#ifndef HERMES_TYPES_H_
#define HERMES_TYPES_H_

#include "hermes_shm/util/logging.h"
#include <stdint.h>

#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "data_structures.h"

/**
 * \file hermes_types.h
 * Types used in Hermes.
 */

/** Convert kilobytes to bytes */
#ifndef KILOBYTES
#define KILOBYTES(n) (((size_t)n) * 1024)                     /**< KB */
#endif

/** Convert megabytes to bytes */
#ifndef MEGABYTES
#define MEGABYTES(n) (((size_t)n) * 1024 * 1024)              /**< MB */
#endif

/** Convert gigabytes to bytes */
#ifndef GIGABYTES
#define GIGABYTES(n) (((size_t)n) * 1024UL * 1024UL * 1024UL) /**< GB */
#endif

/**
 * \namespace hermes
 */
namespace hermes {

typedef uint8_t u8;   /**< 8-bit unsigned integer */
typedef uint16_t u16; /**< 16-bit unsigned integer */
typedef uint32_t u32; /**< 32-bit unsigned integer */
typedef uint64_t u64; /**< 64-bit unsigned integer */
typedef int8_t i8;    /**< 8-bit signed integer */
typedef int16_t i16;  /**< 16-bit signed integer */
typedef int32_t i32;  /**< 32-bit signed integer */
typedef int64_t i64;  /**< 64-bit signed integer */
typedef float f32;    /**< 32-bit float */
typedef double f64;   /**< 64-bit float */

/** Identifier of the Hermes allocator */
extern const hipc::allocator_id_t main_alloc_id;

/** Hermes server environment variable */
extern const char* kHermesServerConf;

/** Hermes client environment variable */
extern const char* kHermesClientConf;

/** Hermes adapter mode environment variable */
extern const char* kHermesAdapterMode;

/** Filesystem page size environment variable */
extern const char* kHermesPageSize;

/** Stop daemon environment variable */
extern const char* kHermesStopDaemon;

/** Maximum path length environment variable */
extern const size_t kMaxPathLength;

/** The mode Hermes is launched in */
enum class HermesType {
  kNone,
  kServer,
  kClient
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

/** The types of I/O that can be performed (for IoCall RPC) */
enum class IoType {
  kRead,
  kWrite,
  kNone
};

typedef u16 DeviceID; /**< device id in unsigned 16-bit integer */

/** The types of topologies */
enum class TopologyType {
  Local,
  Neighborhood,
  Global,

  kCount
};

/** Represents unique ID for BlobId and TagId */
template<int TYPE>
struct UniqueId {
  u64 unique_;   /**< A unique id for the blob */
  i32 node_id_;  /**< The node the content is on */

  bool IsNull() const { return unique_ == 0; }

  UniqueId() = default;

  UniqueId(u64 unique, i32 node_id) : unique_(unique), node_id_(node_id) {}

  static inline UniqueId GetNull() {
    static const UniqueId id(0, 0);
    return id;
  }

  i32 GetNodeId() const { return node_id_; }

  bool operator==(const UniqueId &other) const {
    return unique_ == other.unique_ && node_id_ == other.node_id_;
  }

  bool operator!=(const UniqueId &other) const {
    return unique_ != other.unique_ || node_id_ != other.node_id_;
  }
};
typedef UniqueId<1> BlobId;
typedef UniqueId<2> TagId;
typedef UniqueId<3> TraitId;

/** Allow unique ids to be printed as strings */
template<int num>
std::ostream &operator<<(std::ostream &os, UniqueId<num> const &obj) {
  return os << (std::to_string(obj.node_id_) + "."
               + std::to_string(obj.unique_));
}

/** Indicates a PUT or GET for a particular blob */
struct IoStat {
  IoType type_;
  BlobId blob_id_;
  TagId tag_id_;
  size_t blob_size_;
  int rank_;
};

/** Used as hints to the prefetcher */
struct IoTrace {
  i32 node_id_;
  IoType type_;
  std::string blob_name_;
  std::string tag_name_;
  size_t blob_size_;
  int organize_next_n_;
  float score_;
  int rank_;
};

/** A definition for logging something that is not yet implemented */
#define HERMES_NOT_IMPLEMENTED_YET \
  HELOG(kFatal, "not implemented yet")

/** A TargetId uniquely identifies a buffering target within the system. */
union TargetId {
  /** The Target ID as bitfield */
  struct {
    /** The ID of the node in charge of this target. */
    i32 node_id_;
    /** The ID of the virtual device that backs this target. It is an index into
     * the Device array. */
    u16 device_id_;
    /** The index into the Target array. */
    u16 index_;
  } bits_;

  /** The TargetId as a unsigned 64-bit integer */
  u64 as_int_;

  TargetId() = default;

  TargetId(i32 node_id, u16 device_id, u16 index) {
    bits_.node_id_ = node_id;
    bits_.device_id_ = device_id;
    bits_.index_ = index;
  }

  TargetId(const TargetId &other) {
    as_int_ = other.as_int_;
  }

  i32 GetNodeId() {
    return bits_.node_id_;
  }

  u16 GetDeviceId() {
    return bits_.device_id_;
  }

  u16 GetIndex() {
    return bits_.index_;
  }

  bool IsNull() const  { return as_int_ == 0; }

  bool operator==(const TargetId &other) const {
    return as_int_ == other.as_int_;
  }

  bool operator!=(const TargetId &other) const {
    return as_int_ != other.as_int_;
  }
};

/**
 * Represents the fraction of a blob to place
 * on a particular target during data placement
 * */
struct SubPlacement {
  size_t size_;   /**< Size (bytes) */
  TargetId tid_;  /**< Target destination of data */

  SubPlacement() = default;

  explicit SubPlacement(size_t size, TargetId tid)
      : size_(size), tid_(tid) {}
};

/**
 * The organization of a particular blob in the storage
 * hierarchy during data placement.
 */
struct PlacementSchema {
  std::vector<SubPlacement> plcmnts_;

  void AddSubPlacement(size_t size, TargetId tid) {
    plcmnts_.emplace_back(size, tid);
  }

  void Clear() {
    plcmnts_.clear();
  }
};

/**
 * A structure to represent thesholds with mimimum and maximum values
 */
struct Thresholds {
  float min_; /**< minimum threshold value */
  float max_; /**< maximum threshold value */
};

}  // namespace hermes


namespace hermes::api {

/** A blob is an uniterpreted array of bytes */
typedef hshm::charbuf Blob;

/** Supported data placement policies */
enum class PlacementPolicy {
  kRandom,         /**< Random blob placement */
  kRoundRobin,     /**< Round-Robin (around devices) blob placement */
  kMinimizeIoTime, /**< LP-based blob placement, minimize I/O time */
  kNone,           /**< No DPE for cases we want it disabled */
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

/**
   A structure to represent MinimizeIOTime options
*/
struct MinimizeIoTimeOptions {
  double minimum_remaining_capacity; /**<  minimum remaining capacity */
  double capacity_change_threshold;  /**<  threshold for capacity change  */
  bool use_placement_ratio;          /**<  use placement ratio or not */

  /** A function for initialization */
  MinimizeIoTimeOptions(double minimum_remaining_capacity_ = 0.0,
                        double capacity_change_threshold_ = 0.0,
                        bool use_placement_ratio_ = false)
      : minimum_remaining_capacity(minimum_remaining_capacity_),
        capacity_change_threshold(capacity_change_threshold_),
        use_placement_ratio(use_placement_ratio_) {}
};

enum class PrefetchHint {
  kNone,
  kFileSequential,
  kApriori,

  kFileStrided,
  kMachineLearning
};

/** Hermes API call context */
struct Context {
  /** The blob placement policy */
  PlacementPolicy policy;

  /** Options for controlling the MinimizeIoTime PlacementPolicy */
  MinimizeIoTimeOptions minimize_io_time_options;

  /** Whether random splitting of blobs is enabled for Round-Robin */
  bool rr_split;

  /** Whether Round-Robin can be retried after failure */
  bool rr_retry;

  /** Whether swapping is disabled */
  bool disable_swap;

  /** The blob's score */
  float blob_score_;

  Context();
};

/** \brief Trait types.
 *
 */
enum class TraitType : u8 {
  META = 0, /**< The Trait only modifies metadata. */
  DATA = 1, /**< The Trait modifies raw data (Blob%s). */
  PERSIST = 2,
};

}  // namespace hermes::api

namespace hermes {

/** Namespace simplification for Blob */
using api::Blob;

/** Namespace simplification for Context */
using api::Context;

}  // namespace hermes


/**
 * HASH FUNCTIONS
 * */

namespace std {
template <int TYPE>
struct hash<hermes::UniqueId<TYPE>> {
  std::size_t operator()(const hermes::UniqueId<TYPE> &key) const {
    return
        std::hash<hermes::u64>{}(key.unique_) +
        std::hash<hermes::i32>{}(key.node_id_);
  }
};
}  // namespace std

namespace hermes {

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
#endif  // HERMES_TYPES_H_
