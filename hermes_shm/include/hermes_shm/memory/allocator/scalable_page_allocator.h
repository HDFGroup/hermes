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

struct FreeListSetIpc : public ShmContainer {
 SHM_CONTAINER_TEMPLATE(FreeListSetIpc, FreeListSetIpc)
  ShmArchive<vector<pair<Mutex, iqueue<MpPage>>>> lists_;
  std::atomic<uint16_t> rr_free_;
  std::atomic<uint16_t> rr_alloc_;

  /** SHM constructor. Default. */
  explicit FreeListSetIpc(Allocator *alloc) {
    shm_init_container(alloc);
    HSHM_MAKE_AR0(lists_, alloc)
    SetNull();
  }

  /** SHM emplace constructor */
  explicit FreeListSetIpc(Allocator *alloc, size_t conc) {
    shm_init_container(alloc);
    HSHM_MAKE_AR(lists_, alloc, conc)
    SetNull();
  }

  /** SHM copy constructor. */
  explicit FreeListSetIpc(Allocator *alloc, const FreeListSetIpc &other) {
    shm_init_container(alloc);
    SetNull();
  }

  /** SHM copy assignment operator */
  FreeListSetIpc& operator=(const FreeListSetIpc &other) {
    if (this != &other) {
      shm_destroy();
      SetNull();
    }
    return *this;
  }

  /** Destructor. */
  void shm_destroy_main() {
    lists_->shm_destroy();
  }

  /** Check if Null */
  HSHM_ALWAYS_INLINE bool IsNull() {
    return false;
  }

  /** Set to null */
  HSHM_ALWAYS_INLINE void SetNull() {
    rr_free_ = 0;
    rr_alloc_ = 0;
  }
};

struct FreeListSet {
  std::vector<std::pair<Mutex*, iqueue<MpPage>*>> lists_;
  std::atomic<uint16_t> *rr_free_;
  std::atomic<uint16_t> *rr_alloc_;
};

struct ScalablePageAllocatorHeader : public AllocatorHeader {
  ShmArchive<vector<FreeListSetIpc>> free_lists_;
  std::atomic<size_t> total_alloc_;
  size_t coalesce_trigger_;
  size_t coalesce_window_;

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
  }
};

class ScalablePageAllocator : public Allocator {
 private:
  ScalablePageAllocatorHeader *header_;
  std::vector<FreeListSet> free_lists_;
  StackAllocator alloc_;
  /** The power-of-two exponent of the minimum size that can be cached */
  static const size_t min_cached_size_exp_ = 6;
  /** The minimum size that can be cached directly (64 bytes) */
  static const size_t min_cached_size_ =
    (1 << min_cached_size_exp_) + sizeof(MpPage);
  /** The power-of-two exponent of the minimum size that can be cached (16KB) */
  static const size_t max_cached_size_exp_ = 24;
  /** The maximum size that can be cached directly */
  static const size_t max_cached_size_ =
    (1 << max_cached_size_exp_) + sizeof(MpPage);
  /** The number of well-defined caches */
  static const size_t num_caches_ =
    max_cached_size_exp_ - min_cached_size_exp_ + 1;
  /** An arbitrary free list */
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
    vector<FreeListSetIpc> *free_lists = header_->free_lists_.get();
    free_lists_.reserve(free_lists->size());
    // Iterate over page cache sets
    for (FreeListSetIpc &free_list_set_ipc : *free_lists) {
      free_lists_.emplace_back();
      FreeListSet &free_list_set = free_lists_.back();
      vector<pair<Mutex, iqueue<MpPage>>> &lists_ipc =
        *free_list_set_ipc.lists_;
      std::vector<std::pair<Mutex*, iqueue<MpPage>*>> &lists =
        free_list_set.lists_;
      free_list_set.lists_.reserve(free_list_set_ipc.lists_->size());
      free_list_set.rr_alloc_ = &free_list_set_ipc.rr_alloc_;
      free_list_set.rr_free_ = &free_list_set_ipc.rr_free_;
      // Iterate over single page cache lane
      for (pair<Mutex, iqueue<MpPage>> &free_list_pair_ipc : lists_ipc) {
        Mutex &lock_ipc = free_list_pair_ipc.GetFirst();
        iqueue<MpPage> &free_list_ipc = free_list_pair_ipc.GetSecond();
        lists.emplace_back(&lock_ipc, &free_list_ipc);
      }
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

    // Check the small buffer caches
    if (size_mp <= max_cached_size_) {
      // Get buffer cache at exp
      FreeListSet &free_list_set = free_lists_[exp];
      uint16_t conc = free_list_set.rr_alloc_->fetch_add(1) %
        free_list_set.lists_.size();
      std::pair<Mutex*, iqueue<MpPage>*> free_list_pair =
        free_list_set.lists_[conc];
      Mutex &lock = *free_list_pair.first;
      iqueue<MpPage> &free_list = *free_list_pair.second;
      ScopedMutex scoped_lock(lock, 0);

      // Check buffer cache
      if (free_list.size()) {
        page = free_list.dequeue();
        return page;
      } else {
        return nullptr;
      }
    } else {
      // Get buffer cache at exp
      FreeListSet &free_list_set = free_lists_[num_caches_];
      uint16_t conc = free_list_set.rr_alloc_->fetch_add(1) %
        free_list_set.lists_.size();
      std::pair<Mutex*, iqueue<MpPage>*> free_list_pair =
        free_list_set.lists_[conc];
      Mutex &lock = *free_list_pair.first;
      iqueue<MpPage> &free_list = *free_list_pair.second;
      ScopedMutex scoped_lock(lock, 0);

      // Check the arbitrary buffer cache
      page = FindFirstFit(size_mp,
                          free_list);
      return page;
    }
  }

  /** Find the first fit of an element in a free list */
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
