#ifndef HERMES_STDIO_COMMON_CONSTANTS_H
#define HERMES_STDIO_COMMON_CONSTANTS_H

/**
 * Standard header
 */

/**
 * Dependent library header
 */

/**
 * Internal header
 */
#include <hermes/adapter/constants.h>
#include <hermes/adapter/stdio/common/enumerations.h>

/**
 * Constants file for STDIO adapter.
 */
using hermes::adapter::stdio::MapperType;

/**
 * Which mapper to be used by STDIO adapter.
 */
const MapperType kMapperType = MapperType::BALANCED;
/**
 * Define kPageSize for balanced mapping.
 */
const size_t kPageSize = 1024 * 1024;

#endif  // HERMES_STDIO_COMMON_CONSTANTS_H
