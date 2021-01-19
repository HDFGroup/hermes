#ifndef HERMES_STDIO_COMMON_ENUMERATIONS_H
#define HERMES_STDIO_COMMON_ENUMERATIONS_H
/**
 * Enumeration for STDIO adapter.
 */
namespace hermes::adapter::stdio {
/**
 * Define different types of mappers supported by STDIO Adapter.
 * Also define its construction in the MapperFactory.
 */
enum MapperType { BALANCED = 0 /* Balanced Mapping */ };
}  // namespace hermes::adapter::stdio
#endif  // HERMES_STDIO_COMMON_ENUMERATIONS_H
