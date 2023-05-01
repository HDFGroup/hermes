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

#ifndef HERMES_BALANCED_MAPPER_H
#define HERMES_BALANCED_MAPPER_H

#include <vector>

#include "abstract_mapper.h"

namespace hermes::adapter {
/**
 * Implement balanced mapping
 */
class BalancedMapper : public AbstractMapper {
 public:
  /** Virtual destructor */
  virtual ~BalancedMapper() = default;

  /** Divides an I/O size evenly by into units of page_size */
  void map(size_t off, size_t size, size_t page_size,
           BlobPlacements &ps) override;
};
}  // namespace hermes::adapter

#endif  // HERMES_BALANCED_MAPPER_H
