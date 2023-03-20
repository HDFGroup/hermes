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


#include <hermes_shm/memory/allocator/scalable_page_allocator.h>
#include <hermes_shm/memory/allocator/mp_page.h>

namespace hermes_shm::ipc {

void ScalablePageAllocator::shm_init(allocator_id_t id,
                                     size_t custom_header_size,
                                     char *buffer,
                                     size_t buffer_size,
                                     RealNumber coalesce_trigger,
                                     size_t coalesce_window) {
  Allocator *alloc = &alloc_;
  buffer_ = buffer;
  buffer_size_ = buffer_size;
  header_ = reinterpret_cast<ScalablePageAllocatorHeader*>(buffer_);
  custom_header_ = reinterpret_cast<char*>(header_ + 1);
  size_t region_off = (custom_header_ - buffer_) + custom_header_size;
  size_t region_size = buffer_size_ - region_off;
  alloc_.shm_init(id, 0, buffer + region_off, region_size);
  header_->Configure(id, custom_header_size, &alloc_,
                     buffer_size, coalesce_trigger, coalesce_window);
  free_lists_ = make_ref<vector<pair<FreeListStats, iqueue<MpPage>>>>(
    header_->free_lists_, alloc);
  // Cache every power-of-two between 32B and 16KB
  size_t ncpu = HERMES_SYSTEM_INFO->ncpu_;
  free_lists_->resize(ncpu * num_free_lists_);
  for (size_t i = 0; i < HERMES_SYSTEM_INFO->ncpu_; ++i) {
    for (size_t j = 0; j < num_free_lists_; ++j) {
      hipc::Ref<pair<FreeListStats, iqueue<MpPage>>>
        free_list = (*free_lists_)[i * num_free_lists_ + j];
      free_list->first_->page_size_ = sizeof(MpPage) +
        min_cached_size_ * (1 << j);
      free_list->first_->lock_.Init();
      free_list->first_->cur_alloc_ = 0;
      free_list->first_->max_alloc_ =
        max_cached_size_ / free_list->first_->page_size_;
      if (free_list->first_->max_alloc_ > 16) {
        free_list->first_->max_alloc_ = 16;
      }
    }
  }
}

void ScalablePageAllocator::shm_deserialize(char *buffer,
                                            size_t buffer_size) {
  Allocator *alloc = &alloc_;
  buffer_ = buffer;
  buffer_size_ = buffer_size;
  header_ = reinterpret_cast<ScalablePageAllocatorHeader*>(buffer_);
  custom_header_ = reinterpret_cast<char*>(header_ + 1);
  size_t region_off = (custom_header_ - buffer_) + header_->custom_header_size_;
  size_t region_size = buffer_size_ - region_off;
  alloc_.shm_deserialize(buffer + region_off, region_size);
  free_lists_ = Ref<vector<pair<FreeListStats, iqueue<MpPage>>>>(
    header_->free_lists_, alloc);
}

size_t ScalablePageAllocator::GetCurrentlyAllocatedSize() {
  return header_->total_alloc_;
}

/** Round a number up to the nearest page size. */
size_t ScalablePageAllocator::RoundUp(size_t num, int &exp) {
  int round;
  for (exp = 0; exp < num_caches_; ++exp) {
    round = 1 << (exp + min_cached_size_exp_);
    round += sizeof(MpPage);
    if (num <= round) {
      return round;
    }
  }
  return num;
}

OffsetPointer ScalablePageAllocator::AllocateOffset(size_t size) {
  MpPage *page = nullptr;
  size_t size_mp = size + sizeof(MpPage);

  // Case 1: Can we re-use an existing page?
  page = CheckCaches(size_mp);

  // Case 2: Coalesce if enough space is being wasted
  // if (page == nullptr) {}

  // Case 3: Allocate from stack if no page found
  if (page == nullptr) {
    auto off = alloc_.AllocateOffset(size);
    if (!off.IsNull()) {
      page = alloc_.Convert<MpPage>(off - sizeof(MpPage));
    }
  }

  // Case 4: Completely out of memory
  if (page == nullptr) {
    throw OUT_OF_MEMORY;
  }

  // Mark as allocated
  header_->total_alloc_.fetch_add(page->page_size_);
  auto p = Convert<MpPage, OffsetPointer>(page);
  page->SetAllocated();
  return p + sizeof(MpPage);
}

MpPage *ScalablePageAllocator::CheckCaches(size_t size_mp) {
  MpPage *page;
  ScopedRwReadLock coalesce_lock(header_->coalesce_lock_);
  uint32_t cpu = NodeThreadId().hash() % HERMES_SYSTEM_INFO->ncpu_;
  uint32_t cpu_start = cpu * num_free_lists_;
  hipc::Ref<pair<FreeListStats, iqueue<MpPage>>> first_free_list =
    (*free_lists_)[cpu_start];
  ScopedMutex first_list_lock(first_free_list->first_->lock_);

  // Check the small buffer caches
  if (size_mp <= max_cached_size_) {
    int exp;
    // Check the nearest buffer cache
    size_mp = RoundUp(size_mp, exp);
    hipc::Ref<pair<FreeListStats, iqueue<MpPage>>> free_list =
      (*free_lists_)[cpu_start + exp];
    if (free_list->second_->size()) {
      MpPage *rem_page;
      page = free_list->second_->dequeue();
      DividePage(free_list->first_,
                 free_list->second_,
                 page, rem_page, size_mp,
                 16);
      return page;
    }

    // Check all upper buffer caches
    for (int i = exp + 1; i < num_caches_; ++i) {
      hipc::Ref<pair<FreeListStats, iqueue<MpPage>>> high_free_list =
        (*free_lists_)[cpu_start + i];
      if (high_free_list->second_->size()) {
        MpPage *rem_page;
        page = high_free_list->second_->dequeue();
        DividePage(free_list->first_,
                   free_list->second_,
                   page, rem_page, size_mp,
                   16);
        if (rem_page) {
          hipc::Ref<pair<FreeListStats, iqueue<MpPage>>> last_free_list =
            (*free_lists_)[cpu_start + num_caches_];
          last_free_list->second_->enqueue(rem_page);
        }
        return page;
      }
    }
  }

  // Check the arbitrary buffer cache
  hipc::Ref<pair<FreeListStats, iqueue<MpPage>>> last_free_list =
    (*free_lists_)[cpu_start + num_caches_];
  page = FindFirstFit(size_mp, last_free_list->first_, last_free_list->second_);
  return page;
}

MpPage* ScalablePageAllocator::FindFirstFit(
  size_t size_mp,
  hipc::Ref<FreeListStats> &stats,
  hipc::Ref<iqueue<MpPage>> &free_list) {
  for (auto iter = free_list->begin(); iter != free_list->end(); ++iter) {
    MpPage *fit_page = *iter;
    MpPage *rem_page;
    if (fit_page->page_size_ >= size_mp) {
      DividePage(stats, free_list, fit_page, rem_page, size_mp, 0);
      free_list->dequeue(iter);
      if (rem_page) {
        free_list->enqueue(rem_page);
      }
      return fit_page;
    }
  }
  return nullptr;
}

void ScalablePageAllocator::DividePage(hipc::Ref<FreeListStats> &stats,
                                       hipc::Ref<iqueue<MpPage>> &free_list,
                                       MpPage *fit_page,
                                       MpPage *&rem_page,
                                       size_t size_mp,
                                       size_t max_divide) {
  // Get space remaining after size_mp is allocated
  size_t rem_size;
  rem_size = fit_page->page_size_ - size_mp;
  stats->AddAlloc();

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
      rem_page->flags_ = 0;
      rem_page->off_ = 0;
      stats->AddAlloc();
      free_list->enqueue(rem_page);
      rem_page = (MpPage *) ((char *) rem_page + size_mp);
      rem_size -= size_mp;
    }
  }

  // Case 3: There is still remaining space after the divisions
  if (rem_size > 0) {
    rem_page->page_size_ = rem_size;
    rem_page->flags_ = 0;
    rem_page->off_ = 0;
  } else {
    rem_page = nullptr;
  }
}

OffsetPointer ScalablePageAllocator::AlignedAllocateOffset(size_t size,
                                                        size_t alignment) {
  throw ALIGNED_ALLOC_NOT_SUPPORTED.format();
}

OffsetPointer ScalablePageAllocator::ReallocateOffsetNoNullCheck(
    OffsetPointer p, size_t new_size) {
  OffsetPointer new_p;
  void *ptr = AllocatePtr<void*, OffsetPointer>(new_size, new_p);
  MpPage *hdr = Convert<MpPage>(p - sizeof(MpPage));
  void *old = (void*) (hdr + 1);
  memcpy(ptr, old, hdr->page_size_ - sizeof(MpPage));
  FreeOffsetNoNullCheck(p);
  return new_p;
}

void ScalablePageAllocator::FreeOffsetNoNullCheck(OffsetPointer p) {
  // Mark as free
  auto hdr_offset = p - sizeof(MpPage);
  auto hdr = Convert<MpPage>(hdr_offset);
  if (!hdr->IsAllocated()) {
    throw DOUBLE_FREE.format();
  }
  hdr->UnsetAllocated();
  header_->total_alloc_.fetch_sub(hdr->page_size_);

  // Get the free list to start from
  uint32_t cpu = NodeThreadId().hash() % HERMES_SYSTEM_INFO->ncpu_;
  uint32_t cpu_start = cpu * num_free_lists_;
  hipc::Ref<pair<FreeListStats, iqueue<MpPage>>> first_free_list =
    (*free_lists_)[cpu_start];
  ScopedMutex first_list_lock(first_free_list->first_->lock_);

  // Append to small buffer cache free list
  if (hdr->page_size_ <= max_cached_size_) {
    for (size_t i = 0; i < num_caches_; ++i) {
      hipc::Ref<pair<FreeListStats, iqueue<MpPage>>> free_list =
        (*free_lists_)[cpu_start + i];
      size_t page_size = free_list->first_->page_size_;
      if (page_size == hdr->page_size_) {
        free_list->second_->enqueue(hdr);
        return;
      }
    }
  }

  // Append to arbitrary free list
  hipc::Ref<pair<FreeListStats, iqueue<MpPage>>> last_free_list =
    (*free_lists_)[cpu_start + num_caches_];
  last_free_list->second_->enqueue(hdr);
}

}  // namespace hermes_shm::ipc
