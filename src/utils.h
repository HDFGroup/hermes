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
