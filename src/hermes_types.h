#ifndef HERMES_TYPES_H_
#define HERMES_TYPES_H_

#include <stdint.h>
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

}  // namespace hermes
#endif  // HERMES_TYPES_H_
