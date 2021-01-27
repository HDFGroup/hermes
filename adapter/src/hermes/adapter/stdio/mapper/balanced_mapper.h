#ifndef HERMES_BALANCED_MAPPER_H
#define HERMES_BALANCED_MAPPER_H

/**
 * Standard header
 */
#include <vector>

/**
 * Dependent library header
 */

/**
 * Internal header
 */
#include <hermes/adapter/stdio/common/constants.h>

#include "abstract_mapper.h"

namespace hermes::adapter::stdio {
/**
 * Implement balanced mapping
 */
class BalancedMapper : public AbstractMapper {
 public:
  /**
   * This method maps the current Operation to Hermes data structures.
   *
   * @param file_op, FileStruct, operations for which we are mapping.
   * @return a map of FileStruct to Hermes Struct
   */
  MapperReturnType map(const FileStruct& file_op) override;
};
}  // namespace hermes::adapter::stdio

#endif  // HERMES_BALANCED_MAPPER_H
