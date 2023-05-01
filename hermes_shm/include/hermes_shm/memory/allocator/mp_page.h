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

#ifndef HERMES_INCLUDE_HERMES_MEMORY_ALLOCATOR_MP_PAGE_H_
#define HERMES_INCLUDE_HERMES_MEMORY_ALLOCATOR_MP_PAGE_H_

#include "hermes_shm/data_structures/ipc/iqueue.h"

namespace hshm::ipc {

struct MpPage {
  iqueue_entry entry_;  /**< Position of page in free list */
  size_t page_size_;    /**< The size of the page allocated */
  int flags_;           /**< Page flags (e.g., is_allocated?) */
  uint32_t off_;        /**< The offset within the page */

  void SetAllocated() {
    flags_ = 0x1;
  }

  void UnsetAllocated() {
    flags_ = 0;
  }

  bool IsAllocated() const {
    return flags_ & 0x1;
  }
};

}  // namespace hshm::ipc

#endif  // HERMES_INCLUDE_HERMES_MEMORY_ALLOCATOR_MP_PAGE_H_
