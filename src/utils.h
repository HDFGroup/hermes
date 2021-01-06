#ifndef HERMES_UTILS_H_
#define HERMES_UTILS_H_

#include <assert.h>

#include <chrono>

#include "hermes_types.h"

/**
 * @file utils.h
 *
 * Utility classes and functions for Hermes.
 */

#if HERMES_ENABLE_TIMING

#define HERMES_BEGIN_TIMED_BLOCK(func_name)                 \
  auto hermes_timed_block_start_ =                          \
    std::chrono::high_resolution_clock::now();              \
  const char *hermes_timed_block_func_name_ = (func_name);

// TODO(chogan): Probably want to prepend rank to output
#define HERMES_END_TIMED_BLOCK()                                        \
  auto hermes_timed_block_end_ =                                        \
    std::chrono::high_resolution_clock::now();                          \
  auto hermes_timed_block_elapsed_ =                                    \
    hermes_timed_block_end_ - hermes_timed_block_start_;                \
  double hermes_timed_block_seconds_ =                                  \
    std::chrono::duration<double>(hermes_timed_block_elapsed_).count(); \
  VLOG(1) << hermes_timed_block_func_name_ << " took "                  \
          << hermes_timed_block_seconds_ << " seconds\n";

#else
#define HERMES_BEGIN_TIMED_BLOCK(func_name)
#define HERMES_END_TIMED_BLOCK()
#endif  // HERMES_ENABLE_TIMING

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

}  // namespace hermes
#endif  // HERMES_UTILS_H_
