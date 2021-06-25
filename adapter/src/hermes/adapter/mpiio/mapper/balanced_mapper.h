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

#ifndef HERMES_MPIIO_ADAPTER_BALANCED_MAPPER_H
#define HERMES_MPIIO_ADAPTER_BALANCED_MAPPER_H

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
#include <hermes/adapter/mpiio/common/constants.h>

#include "abstract_mapper.h"

namespace hermes::adapter::mpiio {
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
}  // namespace hermes::adapter::mpiio

#endif  // HERMES_MPIIO_ADAPTER_BALANCED_MAPPER_H
