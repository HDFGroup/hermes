#ifndef HERMES_TYPES_H_
#define HERMES_TYPES_H_

#include <stdint.h>

#include <string>
#include <utility>
#include <vector>
#include <functional>

#include "glog/logging.h"

#define KILOBYTES(n) ((n) * 1024)
#define MEGABYTES(n) ((n) * 1024 * 1024)
#define GIGABYTES(n) ((n) * 1024UL * 1024UL * 1024UL)

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

namespace api {
typedef int Status;
typedef std::vector<unsigned char> Blob;
}  // namespace api

// TODO(chogan): These constants impose limits on the number of slabs, devices,
// file path lengths, and shared memory name lengths, but eventually we should
// allow arbitrary sizes of each.
static constexpr int kMaxBufferPoolSlabs = 8;
constexpr int kMaxPathLength = 256;
constexpr int kMaxBufferPoolShmemNameLength = 64;
constexpr int kMaxDevices = 8;
constexpr int kMaxBucketNameSize = 256;
constexpr int kMaxBlobNameSize = 64;
constexpr int kMaxVBucketNameSize = 256;

constexpr char kPlaceInHierarchy[] = "PlaceInHierarchy";

#define HERMES_NOT_IMPLEMENTED_YET \
  LOG(FATAL) << __func__ << " not implemented yet\n"

#define HERMES_INVALID_CODE_PATH \
  LOG(FATAL) << "Invalid code path." << std::endl

/** A TargetID uniquely identifies a buffering target within the system. */
union TargetID {
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

  u64 as_int;
};

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
  kApp,
  kHermes,

  kCount
};

enum ArenaType {
  kArenaType_BufferPool,  // This must always be first
  kArenaType_MetaData,
  kArenaType_Transient,
  kArenaType_TransferWindow,

  kArenaType_Count
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
  /** The unit of each slab, which is a multiplier of the Device's block size */
  int slab_unit_sizes[kMaxDevices][kMaxBufferPoolSlabs];
  /** The percentage of space each slab should occupy per Device. The values for
   * each Device should add up to 1.0.
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

  u32 max_buckets_per_node;
  u32 max_vbuckets_per_node;
  u32 system_view_state_update_interval_ms;

  /** The mount point or desired directory for each Device. RAM Device should be the
   * empty string.
   */
  std::string mount_points[kMaxDevices];
  /** The mount point of the swap target. */
  std::string swap_mount;
  /** The number of times the BufferOrganizer will attempt to place a swap blob
   * into the hierarchy before giving up.*/
  int num_buffer_organizer_retries;

  /** The hostname of the RPC server, minus any numbers that Hermes may
   * auto-generate when the rpc_hostNumber_range is specified. */
  std::string rpc_server_base_name;
  std::string rpc_server_suffix;
  std::string rpc_protocol;
  std::string rpc_domain;
  int rpc_port;
  int buffer_organizer_port;
  int rpc_host_number_range[2];
  int rpc_num_threads;

  /** A base name for the BufferPool shared memory segement. Hermes appends the
   * value of the USER environment variable to this string.
   */
  char buffer_pool_shmem_name[kMaxBufferPoolShmemNameLength];
};

union BucketID {
  struct {
    u32 index;
    u32 node_id;
  } bits;

  u64 as_int;
};

union VBucketID {
  struct {
    u32 index;
    u32 node_id;
  } bits;

  u64 as_int;
};

union BlobID {
  struct {
    u32 buffer_ids_offset;
    i32 node_id;
  } bits;

  u64 as_int;
};

/**
 * Trait types
 */
typedef u64 TraitID;

enum class TraitType : u8 { META = 0, DATA = 1, FILE_MAPPING = 2 };

struct TraitIdArray {
    TraitID *ids;
    u32 length;
};



}  // namespace hermes
#endif  // HERMES_TYPES_H_
