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

#ifndef HERMES_UTILS_H_
#define HERMES_UTILS_H_

#include <assert.h>

#include <chrono>
#include <map>

#include "hermes_types.h"

/**
 * @file utils.h
 *
 * Utility classes and functions for Hermes.
 */

#if HERMES_ENABLE_TIMING

#define HERMES_BEGIN_TIMED_BLOCK(func_name)                                   \
  auto hermes_timed_block_start_ = std::chrono::high_resolution_clock::now(); \
  const char *hermes_timed_block_func_name_ = (func_name);

// TODO(chogan): Probably want to prepend rank to output
#define HERMES_END_TIMED_BLOCK()                                            \
  auto hermes_timed_block_end_ = std::chrono::high_resolution_clock::now(); \
  auto hermes_timed_block_elapsed_ =                                        \
      hermes_timed_block_end_ - hermes_timed_block_start_;                  \
  double hermes_timed_block_seconds_ =                                      \
      std::chrono::duration<double>(hermes_timed_block_elapsed_).count();   \
  VLOG(1) << hermes_timed_block_func_name_ << " took "                      \
          << hermes_timed_block_seconds_ << " seconds\n";

#else
#define HERMES_BEGIN_TIMED_BLOCK(func_name) /**< begin timing */
#define HERMES_END_TIMED_BLOCK()            /**< end timing */
#endif                                      // HERMES_ENABLE_TIMING

namespace hermes {

/**
 * Rounds a value up to the next given multiple.
 *
 * Returns the original value if it is already divisible by multiple.
 *
 * Example:
 * ```cpp
 * size_t result = RoundUpToMultiple(2000, 4096);
 * assert(result == 4096);
 * ```
 *
 * @param val The initial value to round.
 * @param multiple The multiple to round up to.
 *
 * @return The next multiple of multiple that is greater than or equal to val.
 */
size_t RoundUpToMultiple(size_t val, size_t multiple);

/**
 * Rounds a value down to the previous given multiple.
 *
 * Returns the original value if it is already divisible by multiple.
 *
 * Example:
 * ```cpp
 *   size_t result = RoundDownToMultiple(4097, 4096);
 *   assert(result == 4096);
 * ```
 *
 * @param val The initial value to round.
 * @param multiple The multiple to round down to.
 *
 * @return The previous multiple of multiple that is less than or equal to val.
 */
size_t RoundDownToMultiple(size_t val, size_t multiple);

/**
 * Fills out a Config struct with default values.
 *
 * @param config The Config to populate.
 */
void InitDefaultConfig(Config *config);

void FailedLibraryCall(std::string func);

namespace testing {
/**
   A class to represent BLOB size
 */
enum class BlobSizeRange {
  kSmall,
  kMedium,
  kLarge,
  kXLarge,
  kHuge,
};

/**
   A structure to represent target view state
*/
struct TargetViewState {
  std::vector<hermes::u64> bytes_capacity;  /**< a vector of capacities */
  std::vector<hermes::u64> bytes_available; /**< a vector of available bytes */
  std::vector<hermes::f32> bandwidth;       /**< a vector of bandwidths */
  int num_devices;                          /**< number of devices */
};

/**
 * Initialize device state with default values.
 *
 * @param total_target Total number of target.
 *
 * @param homo_dist The device distribution is homogeneous or not.
 *
 * @return The TargetViewState struct with device information.
 */
TargetViewState InitDeviceState(u64 total_target = 4, bool homo_dist = true);

/**
 * Update device state.
 *
 * @param schema The PlacementSchema return from a data placement
 *               engine calculation.
 *
 * @param node_state The device status after schema is placed.
 *
 * @return Total size of placed blobs.
 */
u64 UpdateDeviceState(PlacementSchema &schema, TargetViewState &node_state);

/**
 * Print device state.
 *
 * @param node_state The TargetViewState with current state.
 */
void PrintNodeState(TargetViewState &node_state);

/**
 * Get default targets.
 *
 * @param n The number of target type.
 *
 @ @return The vector of default targets.
 */
std::vector<TargetID> GetDefaultTargets(size_t n);

/**
 * Generate a vector of blob size with fixed total blob size.
 *
 * @param total_size The number of target type.
 *
 * @param range The blob size range for test.
 *
 * @ @return The vector blob size.
 */
std::vector<size_t> GenFixedTotalBlobSize(size_t total_size,
                                          BlobSizeRange range);

}  // namespace testing
}  // namespace hermes
#endif  // HERMES_UTILS_H_
