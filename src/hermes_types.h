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

#include <stdint.h>

#include <string>
#include <utility>
#include <vector>
#include <functional>

#include <glog/logging.h>

#include "hermes_version.h"

/**
 * \file hermes_types.h
 * Types used in Hermes.
 */

#define KILOBYTES(n) (((size_t)n) * 1024)
#define MEGABYTES(n) (((size_t)n) * 1024 * 1024)
#define GIGABYTES(n) (((size_t)n) * 1024UL * 1024UL * 1024UL)

/**
 * \namespace hermes
 */
namespace hermes {

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef int8_t i8;
typedef int16_t i16;
typedef int32_t i32;
typedef int64_t i64;
typedef float f32;
typedef double f64;

typedef u16 DeviceID;

struct ChunkedIdList {
  u32 head_offset;
  u32 length;
  u32 capacity;
};

/**
 * \namespace api
 */
namespace api {

/**
 * A Blob is simply an uninterpreted vector of bytes.
 */
typedef std::vector<unsigned char> Blob;

/** Supported data placement policies */
enum class PlacementPolicy {
  kRandom,          /**< Random blob placement */
  kRoundRobin,      /**< Round-Robin (around devices) blob placement */
  kMinimizeIoTime,  /**< LP-based blob placement, minimize I/O time */
};

class PlacementPolicyConv {
 public:
  static std::string str(PlacementPolicy policy) {
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
    }
    return "PlacementPolicy::Invalid";
  }
};


struct MinimizeIoTimeOptions {
  double minimum_remaining_capacity;
  double capacity_change_threshold;
  bool use_placement_ratio;

  MinimizeIoTimeOptions(double minimum_remaining_capacity_ = 0.0,
                        double capacity_change_threshold_ = 0.0,
                        bool use_placement_ratio_ = false)
      : minimum_remaining_capacity(minimum_remaining_capacity_),
        capacity_change_threshold(capacity_change_threshold_),
        use_placement_ratio(use_placement_ratio_) {
  }
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

  Context() : policy(default_placement_policy),
              buffer_organizer_retries(default_buffer_organizer_retries),
              rr_split(default_rr_split),
              rr_retry(false),
              disable_swap(false) {}
};

}  // namespace api

// TODO(chogan): These constants impose limits on the number of slabs,
// devices, file path lengths, and shared memory name lengths, but eventually
// we should allow arbitrary sizes of each.
static constexpr int kMaxBufferPoolSlabs = 8;
constexpr int kMaxPathLength = 256;
constexpr int kMaxBufferPoolShmemNameLength = 64;
constexpr int kMaxDevices = 8;
constexpr int kMaxBucketNameSize = 256;
constexpr int kMaxVBucketNameSize = 256;

constexpr char kPlaceInHierarchy[] = "PlaceInHierarchy";

#define HERMES_NOT_IMPLEMENTED_YET                      \
  LOG(FATAL) << __func__ << " not implemented yet\n"

#define HERMES_INVALID_CODE_PATH                    \
  LOG(FATAL) << "Invalid code path." << std::endl

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
};

const TargetID kSwapTargetId = {{0, 0, 0}};

/**
 * A PlacementSchema is a vector of (size, target) pairs where size is the
 * number of bytes to buffer and target is the TargetID where to buffer those
 * bytes.
 */
using PlacementSchema = std::vector<std::pair<size_t, TargetID>>;

/**
 * Distinguishes whether the process (or rank) is part of the application cores
 * or the Hermes core(s).
 */
enum class ProcessKind {
  kApp,     /**< Application process */
  kHermes,  /**< Hermes core process */

  kCount    /**< Sentinel value */
};

/** Arena types */
enum ArenaType {
  kArenaType_BufferPool,      /**< Buffer pool: This must always be first! */
  kArenaType_MetaData,        /**< Metadata                                */
  kArenaType_Transient,       /**< Scratch space                           */

  kArenaType_Count            /**< Sentinel value                          */
};

struct Thresholds {
  float min;
  float max;
};

/**
 * System and user configuration that is used to initialize Hermes.
 */
struct Config {
  /** The total capacity of each buffering Device */
  size_t capacities[kMaxDevices];
  /** The block sizes of each Device */
  int block_sizes[kMaxDevices];
  /** The number of slabs that each Device has */
  int num_slabs[kMaxDevices];
  /** The unit of each slab, a multiple of the Device's block size */
  int slab_unit_sizes[kMaxDevices][kMaxBufferPoolSlabs];
  /** The percentage of space each slab should occupy per Device. The values
   * for each Device should add up to 1.0.
   */
  f32 desired_slab_percentages[kMaxDevices][kMaxBufferPoolSlabs];
  /** The bandwidth of each Device */
  f32 bandwidths[kMaxDevices];
  /** The latency of each Device */
  f32 latencies[kMaxDevices];
  /** The percentages of the total available Hermes memory allotted for each
   *  `ArenaType`
   */
  f32 arena_percentages[kArenaType_Count];
  /** The number of Devices */
  int num_devices;
  /** The number of Targets */
  int num_targets;

  /** The maximum number of buckets per node */
  u32 max_buckets_per_node;
  /** The maximum number of vbuckets per node */
  u32 max_vbuckets_per_node;
  /** The length of a view state epoch */
  u32 system_view_state_update_interval_ms;

  /** The mount point or desired directory for each Device. RAM Device should
   * be the empty string.
   */
  std::string mount_points[kMaxDevices];
  /** The mount point of the swap target. */
  std::string swap_mount;
  /** The number of times the BufferOrganizer will attempt to place a swap
   * blob into the hierarchy before giving up. */
  int num_buffer_organizer_retries;

  /** If non-zero, the device is shared among all nodes (e.g., burst buffs) */
  int is_shared_device[kMaxDevices];

  /** The name of a file that contains host names, 1 per line */
  std::string rpc_server_host_file;
  /** The hostname of the RPC server, minus any numbers that Hermes may
   * auto-generate when the rpc_hostNumber_range is specified. */
  std::string rpc_server_base_name;
  /** The list of numbers from all server names. E.g., '{1, 3}' if your servers
   * are named ares-comp-1 and ares-comp-3 */
  std::vector<std::string> host_numbers;
  /** The RPC server name suffix. This is appended to the base name plus host
      number. */
  std::string rpc_server_suffix;
  /** The parsed hostnames from the hermes conf */
  std::vector<std::string> host_names;
  /** The RPC protocol to be used. */
  std::string rpc_protocol;
  /** The RPC domain name for verbs transport. */
  std::string rpc_domain;
  /** The RPC port number. */
  int rpc_port;
  /** The RPC port number for the buffer organizer. */
  int buffer_organizer_port;
  /** The number of handler threads per RPC server. */
  int rpc_num_threads;
  /** The number of buffer organizer threads. */
  int bo_num_threads;
  /** The default blob placement policy. */
  api::PlacementPolicy default_placement_policy;
  /** Whether blob splitting is enabled for Round-Robin blob placement. */
  bool default_rr_split;
  /** The min and max capacity threshold in MiB for each device at which the
   * BufferOrganizer will trigger. */
  Thresholds bo_capacity_thresholds[kMaxDevices];
  /** A base name for the BufferPool shared memory segement. Hermes appends the
   * value of the USER environment variable to this string.
   */
  char buffer_pool_shmem_name[kMaxBufferPoolShmemNameLength];

  /**
   * Paths prefixed with the following directories are not tracked in Hermes
   * Exclusion list used by darshan at
   * darshan/darshan-runtime/lib/darshan-core.c
   */
  std::vector<std::string> path_exclusions;

  /**
   * Paths prefixed with the following directories are tracked by Hermes even if
   * they share a root with a path listed in path_exclusions
   */
  std::vector<std::string> path_inclusions;
};

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
};

/** Trait ID type */
typedef u64 TraitID;

namespace api {

/** Trait types */
enum class TraitType : u8 {
  META = 0,
  DATA = 1,
  PERSIST = 2,
};

}  // namespace api
}  // namespace hermes
#endif  // HERMES_TYPES_H_
