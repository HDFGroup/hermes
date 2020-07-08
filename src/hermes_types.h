#ifndef HERMES_TYPES_H_
#define HERMES_TYPES_H_

#include <stdint.h>

#include <string>
#include <utility>
#include <vector>

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

typedef u16 TierID;

// TODO(chogan): These constants impose limits on the number of slabs, tiers,
// file path lengths, and shared memory name lengths, but eventually we should
// allow arbitrary sizes of each.
static constexpr int kMaxBufferPoolSlabs = 8;
constexpr int kMaxPathLength = 256;
constexpr int kMaxBufferPoolShmemNameLength = 64;
constexpr int kMaxTiers = 8;

/**
 * A TieredSchema is a vector of (size, tier) pairs where size is the number of
 * bytes to buffer and tier is the Tier ID where to buffer those bytes.
 */
using TieredSchema = std::vector<std::pair<size_t, TierID>>;

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
  kArenaType_BufferPool,
  kArenaType_MetaData,
  kArenaType_Transient,
  kArenaType_TransferWindow,

  kArenaType_Count
};

/**
 * System and user configuration that is used to initialize Hermes.
 */
struct Config {
  /** The total capacity of each buffering Tier */
  size_t capacities[kMaxTiers];
  /** The block sizes of each Tier */
  int block_sizes[kMaxTiers];
  /** The number of slabs that each Tier has */
  int num_slabs[kMaxTiers];
  /** The unit of each slab, which is a multiplier of the Tier's block size */
  int slab_unit_sizes[kMaxTiers][kMaxBufferPoolSlabs];
  /** The percentage of space each slab should occupy per Tier. The values for
   * each Tier should add up to 1.0.
   */
  f32 desired_slab_percentages[kMaxTiers][kMaxBufferPoolSlabs];
  /** The bandwidth of each Tier */
  f32 bandwidths[kMaxTiers];
  /** The latency of each Tier */
  f32 latencies[kMaxTiers];
  /** The percentages of the total available Hermes memory allotted for each
   *  `ArenaType`
   */
  f32 arena_percentages[kArenaType_Count];
  /** The number of Tiers */
  int num_tiers;
  /** The total number of nodes in this Hermes run. */
  int num_nodes;

  u32 max_buckets_per_node;
  u32 max_vbuckets_per_node;

  /** The mount point or desired directory for each Tier. RAM Tier should be the
   * empty string.
   */
  std::string mount_points[kMaxTiers];
  /** The IP address and port number of the BufferPool RPC server in a format
   * that Thallium understands. For example, tcp://172.20.101.25:8080.
   */
  std::string rpc_server_base_name;
  std::string rpc_protocol;
  int rpc_port;
  int rpc_host_number_range[2];

  /** A base name for the BufferPool shared memory segement. Hermes appends the
   * value of the USER environment variable to this string.
   */
  char buffer_pool_shmem_name[kMaxBufferPoolShmemNameLength];
};

}  // namespace hermes
#endif  // HERMES_TYPES_H_
