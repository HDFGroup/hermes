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

#ifndef HERMES_ABSTRACT_MAPPER_H
#define HERMES_ABSTRACT_MAPPER_H

#include "utils.h"

namespace hermes::adapter {

/**
 * Define different types of mappers supported by POSIX Adapter.
 * Also define its construction in the MapperFactory.
 */
enum class MapperType {
  kBalancedMapper
};

/**
 A structure to represent BLOB placement
*/
struct BlobPlacement {
  size_t page_;       /**< The index in the array placements */
  size_t bucket_off_; /**< Offset from file start (for FS) */
  size_t blob_off_;   /**< Offset from BLOB start */
  size_t blob_size_;  /**< Size after offset to read */
  int time_;          /**< The order of the blob in a list of blobs */

  /** create a BLOB name from index. */
  lipc::charbuf CreateBlobName() const {
    lipc::charbuf buf(sizeof(page_));
    memcpy(buf.data_mutable(), &page_, sizeof(page_));
    return buf;
  }

  /** decode \a blob_name BLOB name to index.  */
  void DecodeBlobName(const lipc::charbuf &blob_name) {
    memcpy(&page_, blob_name.data(), sizeof(page_));
  }
};

typedef std::vector<BlobPlacement> BlobPlacements;

/**
   A class to represent abstract mapper
*/
class AbstractMapper {
 public:
  /**
   * This method maps the current operation to Hermes data structures.
   *
   * @param off offset
   * @param size size
   * @param page_size the page division factor
   * @param ps BLOB placement
   *
   */
  virtual void map(size_t off, size_t size, size_t page_size,
                   BlobPlacements &ps) = 0;
};
}  // namespace hermes::adapter

#endif  // HERMES_ABSTRACT_MAPPER_H
