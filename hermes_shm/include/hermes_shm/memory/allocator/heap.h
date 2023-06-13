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

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_MEMORY_ALLOCATOR_HEAP_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_MEMORY_ALLOCATOR_HEAP_H_

#include "allocator.h"
#include "hermes_shm/thread/lock.h"

namespace hshm::ipc {

struct HeapAllocator {
  std::atomic<size_t> heap_off_;
  size_t heap_size_;

  /** Default constructor */
  HeapAllocator() : heap_off_(0), heap_size_(0) {}

  /** Emplace constructor */
  explicit HeapAllocator(size_t heap_off, size_t heap_size)
  : heap_off_(heap_off), heap_size_(heap_size) {}

  /** Explicit initialization */
  void shm_init(size_t heap_off, size_t heap_size) {
    heap_off_ = heap_off;
    heap_size_ = heap_size;
  }

  /** Allocate off heap */
  HSHM_ALWAYS_INLINE OffsetPointer AllocateOffset(size_t size) {
    size_t off = heap_off_.fetch_add(size);
    if (off + size > heap_size_) {
      throw OUT_OF_MEMORY.format();
    }
    return OffsetPointer(off);
  }
};

}  // namespace hshm::ipc

#endif  // HERMES_SHM_INCLUDE_HERMES_SHM_MEMORY_ALLOCATOR_HEAP_H_
