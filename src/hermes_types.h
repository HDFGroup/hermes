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

#include <glog/logging.h>
#include <stdint.h>

#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "hermes_version.h"
#include "data_structures.h"
#include "constants.h"

/**
 * \file hermes_types.h
 * Types used in Hermes.
 */

#ifndef KILOBYTES
#define KILOBYTES(n) (((size_t)n) * 1024)                     /**< KB */
#endif

#ifndef MEGABYTES
#define MEGABYTES(n) (((size_t)n) * 1024 * 1024)              /**< MB */
#endif

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

/** The mode Hermes is launched in */
enum class HermesType {
  kServer,
  kClient,
  kColocated
};

/** The types of I/O that can be performed (for IoCall RPC) */
enum class IoType {
  kRead,
  kWrite
};

typedef u16 DeviceID; /**< device id in unsigned 16-bit integer */

union BucketID {
  /** The Bucket ID as bitfield */
  struct {
    /** The index into the Target array starting at BufferPool::targets_offset
     * (on the node with ID node_id). */
    u32 index;
    /** The ID of the node in charge of this bucket. */
    u32 node_id;
  } bits;

  /** The BucketID as a unsigned 64-bit integer */
  u64 as_int;

  bool IsNull() const { return as_int == 0; }
};

// NOTE(chogan): We reserve sizeof(BucketID) * 2 bytes in order to embed the
// BucketID into the Blob name. See MakeInternalBlobName() for a description of
// why we need double the bytes of a BucketID.
constexpr int kBucketIdStringSize = sizeof(BucketID) * 2;
/**
 * The maximum size in bytes allowed for Blob names.
 */
constexpr int kMaxBlobNameSize = 64 - kBucketIdStringSize;

union VBucketID {
  /** The VBucket ID as bitfield */
  struct {
    /** The index into the Target array starting at BufferPool::targets_offset
     * (on the node with ID node_id). */
    u32 index;
    /** The ID of the node in charge of this vbucket. */
    u32 node_id;
  } bits;

  /** The VBucketID as a unsigned 64-bit integer */
  u64 as_int;

  bool IsNull() const { return as_int == 0; }
};

union BlobID {
  /** The Blob ID as bitfield */
  struct {
    /** The index into the Target array starting at BufferPool::targets_offset
     * (on the node with ID node_id). */
    u32 buffer_ids_offset;
    /** The ID of the node in charge of this bucket. (Negative when in swap
        space.) */
    i32 node_id;
  } bits;

  /** The BlobID as an unsigned 64-bit integer */
  u64 as_int;

  bool IsNull() const { return as_int == 0; }
  bool InSwap() const { return bits.node_id < 0; }
  i32 GetNodeId() const { return bits.node_id; }
};

/** A definition for logging something that is not yet implemented */
#define HERMES_NOT_IMPLEMENTED_YET \
  LOG(FATAL) << __func__ << " not implemented yet\n"

/** A definition for logging invalid code path */
#define HERMES_INVALID_CODE_PATH LOG(FATAL) << "Invalid code path." << std::endl

/** A TargetID uniquely identifies a buffering target within the system. */
union TargetID {
  /** The Target ID as bitfield */
  struct {
    /** The ID of the node in charge of this target. */
    u32 node_id;
    /** The ID of the virtual device that backs this target. It is an index into
     * the Device array starting at BufferPool::devices_offset (on the node with
     * ID node_id). */
    u16 device_id;
    /** The index into the Target array starting at BufferPool::targets_offset
     * (on the node with ID node_id). */
    u16 index;
  } bits;

  /** The TargetID as a unsigned 64-bit integer */
  u64 as_int;

  bool IsNull() const  { return as_int == 0; }
};

/**
 * A PlacementSchema is a vector of (size, target) pairs where size is the
 * number of bytes to buffer and target is the TargetID where to buffer those
 * bytes.
 */
using PlacementSchema = std::vector<std::pair<size_t, TargetID>>;

/**
 * A structure to represent thesholds with mimimum and maximum values
 */
struct Thresholds {
  float min; /**< minimum threshold value */
  float max; /**< maximum threshold value */
};

/** Trait ID type */
typedef u64 TraitID;

}  // namespace hermes


namespace hermes::api {

/**
 * A Blob is simply an uninterpreted vector of bytes.
 */
typedef std::vector<unsigned char> Blob;


/** Supported data placement policies */
enum class PlacementPolicy {
  kRandom,         /**< Random blob placement */
  kRoundRobin,     /**< Round-Robin (around devices) blob placement */
  kMinimizeIoTime, /**< LP-based blob placement, minimize I/O time */
  kNone,           /**< No DPE for cases we want it disabled */
};

/**
   A class to convert placement policy enum value to string
*/
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
    if (policy.find("kRandom") != std::string::npos) {
      return PlacementPolicy::kRandom;
    } else if (policy.find("kRoundRobin") != std::string::npos) {
      return PlacementPolicy::kRoundRobin;
    } else if (policy.find("kMinimizeIoTime") != std::string::npos) {
      return PlacementPolicy::kMinimizeIoTime;
    } else if (policy.find("kNone") != std::string::npos) {
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

struct PrefetchContext {
  PrefetchHint hint_;
  int read_ahead_;
  float decay_;
  PrefetchContext() : hint_(PrefetchHint::kNone), read_ahead_(1), decay_(.5) {}
};

/** Hermes API call context */
struct Context {
  /** The default maximum number of buffer organizer retries */
  static int default_buffer_organizer_retries;

  /** The default blob placement policy */
  static PlacementPolicy default_placement_policy;

  /** Whether random splitting of blobs is enabled for Round-Robin blob
   *  placement.
   */
  static bool default_rr_split;

  /** The blob placement policy */
  PlacementPolicy policy;

  /** Options for controlling the MinimizeIoTime PlacementPolicy */
  MinimizeIoTimeOptions minimize_io_time_options;

  /** The maximum number of buffer organizer retries */
  int buffer_organizer_retries;

  /** Whether random splitting of blobs is enabled for Round-Robin */
  bool rr_split;

  /** Whether Round-Robin can be retried after failure */
  bool rr_retry;

  /** Whether swapping is disabled */
  bool disable_swap;

  /** Prefetching hints */
  VBucketID vbkt_id_;
  PrefetchContext pctx_;

  Context() : policy(default_placement_policy),
              buffer_organizer_retries(default_buffer_organizer_retries),
              rr_split(default_rr_split),
              rr_retry(false),
              disable_swap(false),
              vbkt_id_({0, 0}) {}
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


/**
 * HASH FUNCTIONS
 * */

namespace std {
template <>
struct hash<hermes::BlobID> {
  std::size_t operator()(const hermes::BlobID &key) const {
    return std::hash<hermes::u64>{}(key.as_int);
  }
};

template <>
struct hash<hermes::BucketID> {
  std::size_t operator()(const hermes::BucketID &key) const {
    return std::hash<hermes::u64>{}(key.as_int);
  }
};

template <>
struct hash<hermes::VBucketID> {
  std::size_t operator()(const hermes::VBucketID &key) const {
    return std::hash<hermes::u64>{}(key.as_int);
  }
};
}  // namespace std
#endif  // HERMES_TYPES_H_
