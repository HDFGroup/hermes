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

struct ScalablePageAllocatorHeader : public AllocatorHeader {
  ShmArchive<vector<iqueue<MpPage>>> free_lists_;
  std::atomic<size_t> total_alloc_;
  size_t coalesce_trigger_;
  size_t coalesce_window_;
  Mutex lock_;

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
    lock_.Init();
  }
};

class ScalablePageAllocator : public Allocator {
 private:
  ScalablePageAllocatorHeader *header_;
  std::vector<iqueue<MpPage>*> free_lists_;
  StackAllocator alloc_;
  /** The power-of-two exponent of the minimum size that can be cached */
  static const size_t min_cached_size_exp_ = 6;
  /** The minimum size that can be cached directly (64 bytes) */
  static const size_t min_cached_size_ =
    (1 << min_cached_size_exp_) + sizeof(MpPage);
  /** The power-of-two exponent of the minimum size that can be cached */
  static const size_t max_cached_size_exp_ = 24;
  /** The maximum size that can be cached directly (16MB) */
  static const size_t max_cached_size_ =
    (1 << max_cached_size_exp_) + sizeof(MpPage);
  /** Cache every size between 64 (2^6) BYTES and 16MB (2^24): (19 entries) */
  static const size_t num_caches_ = 24 - 6 + 1;
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
   * Cache the free lists
   * */
  HSHM_ALWAYS_INLINE void CacheFreeLists() {
    vector<iqueue<MpPage>> *free_lists = header_->free_lists_.get();
    free_lists_.reserve(free_lists->size());
    for (iqueue<MpPage> &free_list : *free_lists) {
      free_lists_.emplace_back(&free_list);
    }
  }

  /**
   * Allocate a memory of \a size size. The page allocator cannot allocate
   * memory larger than the page size.
   * */
  OffsetPointer AllocateOffset(size_t size) override;

 private:
  /** Check if a cached page on this core can be re-used */
  HSHM_ALWAYS_INLINE MpPage* CheckLocalCaches(size_t size_mp, size_t exp) {
    MpPage *page;
    ScopedMutex lock(header_->lock_, 0);

    // Check the small buffer caches
    if (size_mp <= max_cached_size_) {
      // Check the nearest buffer cache
      iqueue<MpPage> *free_list = free_lists_[exp];
      if (free_list->size()) {
        page = free_list->dequeue();
        return page;
      } else {
        return nullptr;
      }
    } else {
      // Check the arbitrary buffer cache
      iqueue<MpPage> *last_free_list = free_lists_[num_caches_];
      page = FindFirstFit(size_mp,
                          *last_free_list);
      return page;
    }
  }

  /**
   * Find the first fit of an element in a free list
   * */
  HSHM_ALWAYS_INLINE MpPage* FindFirstFit(size_t size_mp,
                                          iqueue<MpPage> &free_list) {
    for (auto iter = free_list.begin(); iter != free_list.end(); ++iter) {
      MpPage *fit_page = *iter;
      MpPage *rem_page;
      if (fit_page->page_size_ >= size_mp) {
        DividePage(free_list, fit_page, rem_page, size_mp, 0);
        free_list.dequeue(iter);
        if (rem_page) {
          free_list.enqueue(rem_page);
        }
        return fit_page;
      }
    }
    return nullptr;
  }

  /**
   * Divide a page into smaller pages and cache them
   * */
  void DividePage(iqueue<MpPage> &free_list,
                  MpPage *fit_page,
                  MpPage *&rem_page,
                  size_t size_mp,
                  size_t max_divide) {
    // Get space remaining after size_mp is allocated
    size_t rem_size;
    rem_size = fit_page->page_size_ - size_mp;

    // Case 1: The remaining size can't be cached
    rem_page = nullptr;
    if (rem_size < min_cached_size_) {
      return;
    }

    // Case 2: Divide the remaining space into units of size_mp
    fit_page->page_size_ = size_mp;
    rem_page = (MpPage *) ((char *) fit_page + size_mp);
    if (max_divide > 0 && rem_size >= size_mp) {
      size_t num_divisions = (rem_size - size_mp) / size_mp;
      if (num_divisions > max_divide) { num_divisions = max_divide; }
      for (size_t i = 0; i < num_divisions; ++i) {
        rem_page->page_size_ = size_mp;
        rem_page->flags_.Clear();
        rem_page->off_ = 0;
        free_list.enqueue(rem_page);
        rem_page = (MpPage *) ((char *) rem_page + size_mp);
        rem_size -= size_mp;
      }
    }

    // Case 3: There is still remaining space after the divisions
    if (rem_size > 0) {
      rem_page->page_size_ = rem_size;
      rem_page->flags_.Clear();
      rem_page->off_ = 0;
    } else {
      rem_page = nullptr;
    }
  }


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
  HSHM_ALWAYS_INLINE size_t RoundUp(size_t num, size_t &exp) {
    size_t round;
    for (exp = 0; exp < num_caches_; ++exp) {
      round = 1 << (exp + min_cached_size_exp_);
      round += sizeof(MpPage);
      if (num <= round) {
        return round;
      }
    }
    return num;
  }
};

}  // namespace hshm::ipc

#endif  // HERMES_MEMORY_ALLOCATOR_SCALABLE_PAGE_ALLOCATOR_H
