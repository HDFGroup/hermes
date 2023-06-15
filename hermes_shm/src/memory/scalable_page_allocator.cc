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

namespace hshm::ipc {

void ScalablePageAllocator::shm_init(allocator_id_t id,
                                     size_t custom_header_size,
                                     char *buffer,
                                     size_t buffer_size,
                                     RealNumber coalesce_trigger,
                                     size_t coalesce_window) {
  buffer_ = buffer;
  buffer_size_ = buffer_size;
  header_ = reinterpret_cast<ScalablePageAllocatorHeader*>(buffer_);
  custom_header_ = reinterpret_cast<char*>(header_ + 1);
  size_t region_off = (custom_header_ - buffer_) + custom_header_size;
  size_t region_size = buffer_size_ - region_off;
  allocator_id_t sub_id(id.bits_.major_, id.bits_.minor_ + 1);
  alloc_.shm_init(sub_id, 0, buffer + region_off, region_size);
  HERMES_MEMORY_REGISTRY_REF.RegisterAllocator(&alloc_);
  header_->Configure(id, custom_header_size, &alloc_,
                     buffer_size, coalesce_trigger, coalesce_window);
  vector<iqueue<MpPage>> *free_lists = header_->free_lists_.get();
  free_lists->resize(num_free_lists_);
  CacheFreeLists();
  // size_t ncpu = HERMES_SYSTEM_INFO->ncpu_;
}

void ScalablePageAllocator::shm_deserialize(char *buffer,
                                            size_t buffer_size) {
  buffer_ = buffer;
  buffer_size_ = buffer_size;
  header_ = reinterpret_cast<ScalablePageAllocatorHeader*>(buffer_);
  custom_header_ = reinterpret_cast<char*>(header_ + 1);
  size_t region_off = (custom_header_ - buffer_) + header_->custom_header_size_;
  size_t region_size = buffer_size_ - region_off;
  alloc_.shm_deserialize(buffer + region_off, region_size);
  HERMES_MEMORY_REGISTRY_REF.RegisterAllocator(&alloc_);
  CacheFreeLists();
}

size_t ScalablePageAllocator::GetCurrentlyAllocatedSize() {
  return header_->total_alloc_;
}

OffsetPointer ScalablePageAllocator::AllocateOffset(size_t size) {
  MpPage *page = nullptr;
  size_t exp;
  size_t size_mp = RoundUp(size + sizeof(MpPage), exp);

  // Case 1: Can we re-use an existing page?
  page = CheckLocalCaches(size_mp, exp);

  // Case 2: Coalesce if enough space is being wasted
  // if (page == nullptr) {}

  // Case 3: Allocate from stack if no page found
  if (page == nullptr) {
    auto off = alloc_.AllocateOffset(size_mp);
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
  page->page_size_ = size_mp;
  page->SetAllocated();
  return p + sizeof(MpPage);
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
  void *old = (void*)(hdr + 1);
  memcpy(ptr, old, hdr->page_size_ - sizeof(MpPage));
  FreeOffsetNoNullCheck(p);
  return new_p;
}

void ScalablePageAllocator::FreeOffsetNoNullCheck(OffsetPointer p) {
  ScopedMutex lock(header_->lock_, 0);
  // Mark as free
  auto hdr_offset = p - sizeof(MpPage);
  auto hdr = Convert<MpPage>(hdr_offset);
  if (!hdr->IsAllocated()) {
    throw DOUBLE_FREE.format();
  }
  hdr->UnsetAllocated();
  header_->total_alloc_.fetch_sub(hdr->page_size_);
  size_t exp;
  RoundUp(hdr->page_size_, exp);

  // Append to small buffer cache free list
  if (hdr->page_size_ <= max_cached_size_) {
    iqueue<MpPage> *free_list = free_lists_[exp];
    free_list->enqueue(hdr);
  } else {
    iqueue<MpPage> *last_free_list = free_lists_[num_caches_];
    last_free_list->enqueue(hdr);
  }
}

}  // namespace hshm::ipc
