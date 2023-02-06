/*
 * Copyright (C) 2022  SCS Lab <scslab@iit.edu>,
 * Luke Logan <llogan@hawk.iit.edu>,
 * Jaime Cernuda Garcia <jcernudagarcia@hawk.iit.edu>
 * Jay Lofstead <gflofst@sandia.gov>,
 * Anthony Kougkas <akougkas@iit.edu>,
 * Xian-He Sun <sun@iit.edu>
 *
 * This file is part of HermesShm
 *
 * HermesShm is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */


#include <hermes_shm/memory/allocator/malloc_allocator.h>

namespace hermes_shm::ipc {

struct MallocPage {
  size_t page_size_;
};

void MallocAllocator::shm_init(MemoryBackend *backend,
                               allocator_id_t id,
                               size_t custom_header_size) {
  backend_ = backend;
  header_ = reinterpret_cast<MallocAllocatorHeader*>(
    malloc(sizeof(MallocAllocatorHeader) + custom_header_size));
  custom_header_ = reinterpret_cast<char*>(header_ + 1);
  header_->Configure(id, custom_header_size);
}

void MallocAllocator::shm_deserialize(MemoryBackend *backend) {
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
  return OffsetPointer(size_t(page + 1));
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

}  // namespace hermes_shm::ipc
