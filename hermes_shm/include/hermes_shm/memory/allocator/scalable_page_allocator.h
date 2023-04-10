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


#ifndef HERMES_MEMORY_ALLOCATOR_SCALABLE_PAGE_ALLOCATOR_H
#define HERMES_MEMORY_ALLOCATOR_SCALABLE_PAGE_ALLOCATOR_H

#include "allocator.h"
#include "hermes_shm/thread/lock.h"
#include "hermes_shm/data_structures/ipc/pair.h"
#include "hermes_shm/data_structures/ipc/vector.h"
#include "hermes_shm/data_structures/ipc/list.h"
#include "hermes_shm/data_structures/ipc/pair.h"
#include <hermes_shm/memory/allocator/stack_allocator.h>
#include "mp_page.h"

namespace hshm::ipc {

struct FreeListStats {
  size_t page_size_;  /**< Page size stored in this free list */
  size_t cur_alloc_;  /**< Number of pages currently allocated */
  size_t max_alloc_;  /**< Maximum number of pages allocated at a time */
  Mutex lock_;        /**< The enqueue / dequeue lock */

  /** Default constructor */
  FreeListStats() = default;

  /** Copy constructor */
  FreeListStats(const FreeListStats &other) {
    strong_copy(other);
  }

  /** Copy assignment operator */
  FreeListStats& operator=(const FreeListStats &other) {
    strong_copy(other);
    return *this;
  }

  /** Move constructor */
  FreeListStats(FreeListStats &&other) {
    strong_copy(other);
  }

  /** Move assignment operator */
  FreeListStats& operator=(FreeListStats &&other) {
    strong_copy(other);
    return *this;
  }

  /** Internal copy */
  void strong_copy(const FreeListStats &other) {
    page_size_ = other.page_size_;
    cur_alloc_ = other.cur_alloc_;
    max_alloc_ = other.max_alloc_;
  }

  /** Increment allocation count */
  void AddAlloc() {
    cur_alloc_ += 1;
    if (cur_alloc_ > max_alloc_) {
      max_alloc_ += 1;
    }
  }

  /** Decrement allocation count */
  void AddFree() {
    cur_alloc_ -= 1;
  }
};

struct ScalablePageAllocatorHeader : public AllocatorHeader {
  ShmArchive<vector<pair<FreeListStats, iqueue<MpPage>>>> free_lists_;
  std::atomic<size_t> total_alloc_;
  size_t coalesce_trigger_;
  size_t coalesce_window_;
  RwLock coalesce_lock_;

  ScalablePageAllocatorHeader() = default;

  void Configure(allocator_id_t alloc_id,
                 size_t custom_header_size,
                 Allocator *alloc,
                 size_t buffer_size,
                 RealNumber coalesce_trigger,
                 size_t coalesce_window) {
    AllocatorHeader::Configure(alloc_id,
                               AllocatorType::kScalablePageAllocator,
                               custom_header_size);
    HSHM_MAKE_AR0(free_lists_, alloc)
    total_alloc_ = 0;
    coalesce_trigger_ = (coalesce_trigger * buffer_size).as_int();
    coalesce_window_ = coalesce_window;
    coalesce_lock_.Init();
  }
};

class ScalablePageAllocator : public Allocator {
 private:
  ScalablePageAllocatorHeader *header_;
  vector<pair<FreeListStats, iqueue<MpPage>>> *free_lists_;
  StackAllocator alloc_;
  /** The power-of-two exponent of the minimum size that can be cached */
  static const size_t min_cached_size_exp_ = 5;
  /** The minimum size that can be cached directly (32 bytes) */
  static const size_t min_cached_size_ = (1 << min_cached_size_exp_);
  /** The power-of-two exponent of the minimum size that can be cached */
  static const size_t max_cached_size_exp_ = 14;
  /** The maximum size that can be cached directly (16KB) */
  static const size_t max_cached_size_ = (1 << max_cached_size_exp_);
  /** Cache every size between 16 (2^4) BYTES and 16KB (2^14): (11 entries) */
  static const size_t num_caches_ = 14 - 5 + 1;
  /**
   * The last free list stores sizes larger than 16KB or sizes which are
   * not exactly powers-of-two.
   * */
  static const size_t num_free_lists_ = num_caches_ + 1;

 public:
  /**
   * Allocator constructor
   * */
  ScalablePageAllocator()
  : header_(nullptr) {}

  /**
   * Get the ID of this allocator from shared memory
   * */
  allocator_id_t &GetId() override {
    return header_->allocator_id_;
  }

  /**
   * Initialize the allocator in shared memory
   * */
  void shm_init(allocator_id_t id,
                size_t custom_header_size,
                char *buffer,
                size_t buffer_size,
                RealNumber coalesce_trigger = RealNumber(1, 5),
                size_t coalesce_window = MEGABYTES(1));

  /**
   * Attach an existing allocator from shared memory
   * */
  void shm_deserialize(char *buffer,
                       size_t buffer_size) override;

  /**
   * Allocate a memory of \a size size. The page allocator cannot allocate
   * memory larger than the page size.
   * */
  OffsetPointer AllocateOffset(size_t size) override;

 private:
  /** Check if a cached page can be re-used */
  MpPage *CheckCaches(size_t size_mp);

  /**
   * Find the first fit of an element in a free list
   * */
  MpPage* FindFirstFit(size_t size_mp,
                       FreeListStats &stats,
                       iqueue<MpPage> &free_list);

  /**
   * Divide a page into smaller pages and cache them
   * */
  void DividePage(FreeListStats &stats,
                  iqueue<MpPage> &free_list,
                  MpPage *fit_page,
                  MpPage *&rem_page,
                  size_t size_mp,
                  size_t max_divide);


 public:
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
  /** Round a number up to the nearest page size. */
  size_t RoundUp(size_t num, size_t &exp);
};

}  // namespace hshm::ipc

#endif  // HERMES_MEMORY_ALLOCATOR_SCALABLE_PAGE_ALLOCATOR_H
