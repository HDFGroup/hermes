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

//
// Created by manihariharan on 12/23/20.
//

#ifndef HERMES_ABSTRACT_ADAPTER_H
#define HERMES_ABSTRACT_ADAPTER_H

#include "posix/datastructures.h"

/**
 * Typedef to simplify the return types
 */
typedef std::vector<std::pair<hermes::adapter::posix::FileStruct,
                              hermes::adapter::posix::HermesStruct>>
    MapperReturnType;

namespace hermes::adapter::posix {
/**
 * Interface to define a mapper.
 */
class AbstractMapper {
 public:
  /**
   * This method maps the current Operation to Hermes data structures.
   *
   * @param file_op, FileStruct, operations for which we are mapping.
   * @return a map of FileStruct to Hermes Struct
   */
  virtual MapperReturnType map(const FileStruct& file_op) = 0;
};
}  // namespace hermes::adapter::posix

#endif  // HERMES_ABSTRACT_ADAPTER_H
