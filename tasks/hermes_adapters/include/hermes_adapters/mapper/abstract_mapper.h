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

#include "hermes/hermes_types.h"

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

  /** create a BLOB name from index. */
  static hshm::charbuf CreateBlobName(size_t page) {
    hshm::charbuf buf(sizeof(page));
    labstor::LocalSerialize srl(buf);
    srl << page;
    return buf;
  }

  /** create a BLOB name from index. */
  hshm::charbuf CreateBlobName() const {
    hshm::charbuf buf(sizeof(page_));
    labstor::LocalSerialize srl(buf);
    srl << page_;
    return buf;
  }

  /** decode \a blob_name BLOB name to index.  */
  template<typename StringT>
  void DecodeBlobName(const StringT &blob_name, size_t page_size) {
    labstor::LocalDeserialize srl(blob_name);
    srl >> page_;
    bucket_off_ = page_ * page_size;
    blob_off_ = 0;
  }
};

typedef std::vector<BlobPlacement> BlobPlacements;

/**
   A class to represent abstract mapper
*/
class AbstractMapper {
 public:
  /** Virtual destructor */
  virtual ~AbstractMapper() = default;

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
