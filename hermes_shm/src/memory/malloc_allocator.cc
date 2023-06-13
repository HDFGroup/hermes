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


#include <hermes_shm/memory/allocator/malloc_allocator.h>

namespace hshm::ipc {

struct MallocPage {
  size_t page_size_;
};

void MallocAllocator::shm_init(allocator_id_t id,
                               size_t custom_header_size,
                               size_t buffer_size) {
  buffer_ = nullptr;
  buffer_size_ = buffer_size;
  header_ = reinterpret_cast<MallocAllocatorHeader*>(
    malloc(sizeof(MallocAllocatorHeader) + custom_header_size));
  custom_header_ = reinterpret_cast<char*>(header_ + 1);
  header_->Configure(id, custom_header_size);
}

void MallocAllocator::shm_deserialize(char *buffer,
                                      size_t buffer_size) {
  throw NOT_IMPLEMENTED.format("MallocAllocator::shm_deserialize");
}

size_t MallocAllocator::GetCurrentlyAllocatedSize() {
  return header_->total_alloc_size_;
}

OffsetPointer MallocAllocator::AllocateOffset(size_t size) {
  auto page = reinterpret_cast<MallocPage*>(
    malloc(sizeof(MallocPage) + size));
  page->page_size_ = size;
  header_->total_alloc_size_ += size;
  return OffsetPointer((size_t)(page + 1));
}

OffsetPointer MallocAllocator::AlignedAllocateOffset(size_t size,
                                                     size_t alignment) {
  auto page = reinterpret_cast<MallocPage*>(
    aligned_alloc(alignment, sizeof(MallocPage) + size));
  page->page_size_ = size;
  header_->total_alloc_size_ += size;
  return OffsetPointer(size_t(page + 1));
}

OffsetPointer MallocAllocator::ReallocateOffsetNoNullCheck(OffsetPointer p,
                                                           size_t new_size) {
  // Get the input page
  auto page = reinterpret_cast<MallocPage*>(
    p.off_.load() - sizeof(MallocPage));
  header_->total_alloc_size_ += new_size - page->page_size_;

  // Reallocate the input page
  auto new_page = reinterpret_cast<MallocPage*>(
    realloc(page, sizeof(MallocPage) + new_size));
  new_page->page_size_ = new_size;

  // Create the pointer
  return OffsetPointer(size_t(new_page + 1));
}

void MallocAllocator::FreeOffsetNoNullCheck(OffsetPointer p) {
  auto page = reinterpret_cast<MallocPage*>(
    p.off_.load() - sizeof(MallocPage));
  header_->total_alloc_size_ -= page->page_size_;
  free(page);
}

}  // namespace hshm::ipc
