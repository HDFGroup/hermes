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

#include "balanced_mapper.h"
#include "api/hermes.h"

namespace hermes::adapter {

/**
 * Convert a range defined by \a off and \a size into specific
 * blob placements.
 */
void BalancedMapper::map(size_t off, size_t size,
                         size_t page_size,
                         BlobPlacements &ps) {
  HILOG(kDebug, "Mapping File with offset {} and size {}", off, size);
  size_t kPageSize = page_size;
  size_t size_mapped = 0;
  while (size > size_mapped) {
    BlobPlacement p;
    p.bucket_off_ = off + size_mapped;
    p.page_ = p.bucket_off_ / kPageSize;
    p.page_size_ = page_size;
    p.blob_off_ = p.bucket_off_ % kPageSize;
    auto left_size_page = kPageSize - p.blob_off_;
    p.blob_size_ = left_size_page < size - size_mapped ? left_size_page
      : size - size_mapped;
    ps.emplace_back(p);
    size_mapped += p.blob_size_;
  }
}


}  // namespace hermes::adapter
