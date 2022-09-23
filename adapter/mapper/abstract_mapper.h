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

namespace hermes::adapter {

/**
 * Define different types of mappers supported by POSIX Adapter.
 * Also define its construction in the MapperFactory.
 */
enum MapperType {
  BALANCED = 0 /* Balanced Mapping */
};

struct BlobPlacement {
  size_t bucket_off_;       // Offset from file start (for FS)
  size_t blob_off_;         // Offset from blob start
  size_t blob_size_;        // Size after offset to read
  std::string blob_name_;   // Name of the blob
};

typedef std::vector<BlobPlacement> BlobPlacements;

class AbstractMapper {
 public:
  /**
   * This method maps the current Operation to Hermes data structures.
   *
   * @param file_op, FileStruct, operations for which we are mapping.
   * @return a map of FileStruct to Hermes Struct
   */
  virtual void map(size_t off, size_t size, BlobPlacements &ps) = 0;
};
}  // namespace hermes::adapter

#endif  // HERMES_ABSTRACT_ADAPTER_H
