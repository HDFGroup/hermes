//
// Created by lukemartinlogan on 12/27/22.
//

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_MEMORY_ALLOCATOR_MP_PAGE_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_MEMORY_ALLOCATOR_MP_PAGE_H_

namespace hermes_shm::ipc {

struct MpPage {
  int flags_;           /**< Page flags (e.g., is_allocated?) */
  size_t page_size_;    /**< The size of the page allocated */
  uint32_t off_;        /**< The offset within the page */
  uint32_t page_idx_;   /**< The id of the page in the mp free list */

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

}  // namespace hermes_shm::ipc

#endif  // HERMES_SHM_INCLUDE_HERMES_SHM_MEMORY_ALLOCATOR_MP_PAGE_H_
