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

#include "interceptor.h"

namespace hermes::adapter {

/**
 * Define different types of mappers supported by POSIX Adapter.
 * Also define its construction in the MapperFactory.
 */
enum MapperType {
  BALANCED = 0 /* Balanced Mapping */
};

/**
 A structure to represent BLOB placement
*/
struct BlobPlacement {
  int rank_;          /**< The rank of the process producing the BLOB */
  size_t page_;       /**< The index in the array placements */
  size_t bucket_off_; /**< Offset from file start (for FS) */
  size_t blob_off_;   /**< Offset from BLOB start */
  size_t blob_size_;  /**< Size after offset to read */
  int time_;          /**< The order of the blob in a list of blobs */

  /** create a BLOB name from index. */
  std::string CreateBlobName() const { return std::to_string(page_); }

  /** decode \a blob_name BLOB name to index.  */
  void DecodeBlobName(const std::string &blob_name) {
    std::stringstream(blob_name) >> page_;
  }

  /** create a log entry for this BLOB using \a time. */
  std::string CreateBlobNameLogEntry(int time) const {
    std::stringstream ss;
    ss << std::to_string(page_);
    ss << "#" << std::to_string(blob_off_);
    ss << "#" << std::to_string(blob_size_);
    ss << "#" << std::to_string(rank_);
    ss << "#" << std::to_string(time);
    return ss.str();
  }

  /** decode \a blob_name BLOB name by splitting it into index, offset, size,
        and rank. */
  void DecodeBlobNameLogEntry(const std::string &blob_name) {
    auto str_split =
        hermes::adapter::StringSplit(blob_name.data(), '#');
    std::stringstream(str_split[0]) >> page_;
    std::stringstream(str_split[1]) >> blob_off_;
    std::stringstream(str_split[2]) >> blob_size_;
    std::stringstream(str_split[3]) >> rank_;
    std::stringstream(str_split[4]) >> time_;
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
   * @param ps BLOB placement
   *
   */
  virtual void map(size_t off, size_t size, BlobPlacements &ps) = 0;
};
}  // namespace hermes::adapter

#endif  // HERMES_ABSTRACT_ADAPTER_H
