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
  bitfield32_t flags_;  /**< Flags of the page (e.g., free/alloc) */
  /** Offset from the start of the page to the beginning of this header */
  uint32_t off_;
  size_t page_size_;    /**< The total size of the page allocated */

  HSHM_ALWAYS_INLINE void SetAllocated() {
    flags_.SetBits(0x1);
  }

  HSHM_ALWAYS_INLINE void UnsetAllocated() {
    flags_.Clear();
  }

  HSHM_ALWAYS_INLINE bool IsAllocated() const {
    return flags_.All(0x1);
  }
};

}  // namespace hshm::ipc

#endif  // HERMES_INCLUDE_HERMES_MEMORY_ALLOCATOR_MP_PAGE_H_
