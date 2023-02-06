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

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_MEMORY_ALLOCATOR_MULTI_PAGE_ALLOCATOR_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_MEMORY_ALLOCATOR_MULTI_PAGE_ALLOCATOR_H_

#include "allocator.h"
#include "hermes_shm/thread/lock.h"
#include "mp_page.h"

namespace hermes_shm::ipc {

struct MultiPageFreeList {
  /// Lock the list
  Mutex lock_;
  /// Free list for different page sizes
  /// Stack allocator
  size_t region_off_, region_size_;
  /// Number of bytes currently free in this free list
  size_t free_size_;
  /// Total number of bytes alloc'd from this list
  size_t total_alloced_;
  /// Total number of bytes freed to this list
  size_t total_freed_;

  /**
   * The total amount of space allocated by the MultiPageFree list when
   * there is \a num_page_caches number of page size free lists
   * */
  static size_t GetSizeBytes(size_t num_page_caches) {
    return 0;
  }

  /**
   * Initialize the free list array
   * */
  void shm_init(size_t mp_free_list_size,
                char *region_start,
                size_t region_off, size_t region_size) {
  }

  /** Get the free list at index i */
};

struct MultiPageAllocatorHeader : public AllocatorHeader {
  /// Number of threads to initially assume
  std::atomic<uint32_t> concurrency_;
  /// Bytes to dedicate to per-thread free list tables
  size_t thread_table_size_;
  /// Cache every page between these sizes
  size_t mp_free_list_size_;
  size_t min_page_size_, max_page_size_;
  uint32_t min_page_log_, max_page_log_, last_page_idx_;
  /// The page sizes to cache
  RealNumber growth_rate_;
  /// The minimum number of free bytes before a coalesce can be triggered
  size_t coalesce_min_size_;
  /// The percentage of fragmentation before a coalesce is triggered
  RealNumber coalesce_frac_;

  MultiPageAllocatorHeader() = default;

  void Configure(allocator_id_t alloc_id,
                 size_t custom_header_size,
                 size_t min_page_size,
                 size_t max_page_size,
                 RealNumber growth_rate,
                 size_t coalesce_min_size,
                 RealNumber coalesce_frac,
                 size_t thread_table_size,
                 uint32_t concurrency) {
  }
};

class MultiPageAllocator : public Allocator {
 private:
  MultiPageAllocatorHeader *header_;

 public:
  /**
   * Allocator constructor
   * */
  MultiPageAllocator()
    : header_(nullptr) {}

  /**
   * Get the ID of this allocator from shared memory
   * */
  allocator_id_t GetId() override {
    return header_->allocator_id_;
  }

  /**
   * Initialize the allocator in shared memory
   * */
  void shm_init(MemoryBackend *backend,
                allocator_id_t alloc_id,
                size_t custom_header_size = 0,
                size_t min_page_size = 64,
                size_t max_page_size = KILOBYTES(32),
                RealNumber growth_rate = RealNumber(5, 4),
                size_t coalesce_min_size = MEGABYTES(20),
                RealNumber coalesce_frac = RealNumber(2, 1),
                size_t thread_table_size = MEGABYTES(1),
                uint32_t concurrency = 4);

  /**
   * Attach an existing allocator from shared memory
   * */
  void shm_deserialize(MemoryBackend *backend) override;

  /**
   * Allocate a memory of \a size size. The page allocator cannot allocate
   * memory larger than the page size.
   * */
  OffsetPointer AllocateOffset(size_t size) override;

  /**
   * Allocate a memory of \a size size, which is aligned to \a
   * alignment.
   * */
  OffsetPointer AlignedAllocateOffset(size_t size, size_t alignment) override;

  /**
   * Reallocate \a p pointer to \a new_size new size.
   *
   * @return whether or not the pointer p was changed
   * */
  OffsetPointer ReallocateOffsetNoNullCheck(
    OffsetPointer p, size_t new_size) override;

  /**
   * Free \a ptr pointer. Null check is performed elsewhere.
   * */
  void FreeOffsetNoNullCheck(OffsetPointer p) override;

  /**
   * Get the current amount of data allocated. Can be used for leak
   * checking.
   * */
  size_t GetCurrentlyAllocatedSize() override;

 private:
  /** Get the index of "x" within the page free list array */
  inline uint32_t IndexLogarithm(size_t x, size_t &round);

  /** Get the pointer to the mp_free_lists_ array */
  void* GetMpFreeListStart();

  /** Set the page's header */
  void _AllocateHeader(Pointer &p,
                       size_t page_size,
                       size_t page_size_idx,
                       size_t off);

  /** Set the page's header + change allocator stats*/
  void _AllocateHeader(Pointer &p,
                       MultiPageFreeList &mp_free_list,
                       size_t page_size,
                       size_t page_size_idx,
                       size_t off);

  /** Allocate a page from a thread's mp free list */
  Pointer _Allocate(MultiPageFreeList &free_list,
                    size_t page_size_idx, size_t page_size);

  /** Allocate a large, cached page */
  bool _AllocateLargeCached(MultiPageFreeList &mp_free_list,
                            size_t page_size_idx,
                            size_t page_size,
                            Pointer &ret);

  /** Allocate a cached page */
  bool _AllocateCached(MultiPageFreeList &mp_free_list,
                       size_t page_size_idx,
                       size_t page_size,
                       Pointer &ret);

  /** Allocate and divide a cached page larger than page_size */
  bool _AllocateBorrowCached(MultiPageFreeList &mp_free_list,
                             size_t page_size_idx,
                             size_t page_size,
                             Pointer &ret);

  /** Allocate a page from the segment */
  bool _AllocateSegment(MultiPageFreeList &mp_free_list,
                        size_t page_size_idx,
                        size_t page_size,
                        Pointer &ret);

  /** Reorganize free space to minimize fragmentation */
  void _Coalesce(MultiPageFreeList &to, tid_t tid);

  /** Create a new thread allocator by borrowing from other allocators */
  void _AddThread();

  /** Free a page to a free list */
  void _Free(MultiPageFreeList &free_list, Pointer &p);
};

}  // namespace hermes_shm::ipc

#endif //HERMES_SHM_INCLUDE_HERMES_SHM_MEMORY_ALLOCATOR_MULTI_PAGE_ALLOCATOR_H_
