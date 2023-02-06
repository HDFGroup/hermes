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

#include <hermes_shm/memory/allocator/stack_allocator.h>
#include <hermes_shm/memory/allocator/mp_page.h>

namespace hermes_shm::ipc {

void StackAllocator::shm_init(MemoryBackend *backend,
                             allocator_id_t id,
                             size_t custom_header_size) {
  backend_ = backend;
  header_ = reinterpret_cast<StackAllocatorHeader*>(backend_->data_);
  custom_header_ = reinterpret_cast<char*>(header_ + 1);
  size_t region_off = (custom_header_ - backend_->data_) + custom_header_size;
  size_t region_size = backend_->data_size_ - region_off;
  header_->Configure(id, custom_header_size, region_off, region_size);
}

void StackAllocator::shm_deserialize(MemoryBackend *backend) {
  backend_ = backend;
  header_ = reinterpret_cast<StackAllocatorHeader*>(backend_->data_);
  custom_header_ = reinterpret_cast<char*>(header_ + 1);
}

size_t StackAllocator::GetCurrentlyAllocatedSize() {
  return header_->total_alloc_;
}

OffsetPointer StackAllocator::AllocateOffset(size_t size) {
  size += sizeof(MpPage);
  OffsetPointer p(header_->region_off_.fetch_add(size));
  auto hdr = Convert<MpPage>(p);
  hdr->SetAllocated();
  hdr->page_size_ = size;
  header_->region_size_.fetch_sub(hdr->page_size_);
  header_->total_alloc_.fetch_add(hdr->page_size_);
  return p + sizeof(MpPage);
}

OffsetPointer StackAllocator::AlignedAllocateOffset(size_t size,
                                                    size_t alignment) {
  throw ALIGNED_ALLOC_NOT_SUPPORTED.format();
}

OffsetPointer StackAllocator::ReallocateOffsetNoNullCheck(OffsetPointer p,
                                                          size_t new_size) {
  OffsetPointer new_p;
  void *src = Convert<void>(p);
  auto hdr = Convert<MpPage>(p - sizeof(MpPage));
  size_t old_size = hdr->page_size_ - sizeof(MpPage);
  void *dst = AllocatePtr<void, OffsetPointer>(new_size, new_p);
  memcpy(dst, src, old_size);
  Free(p);
  return new_p;
}

void StackAllocator::FreeOffsetNoNullCheck(OffsetPointer p) {
  auto hdr = Convert<MpPage>(p - sizeof(MpPage));
  if (!hdr->IsAllocated()) {
    throw DOUBLE_FREE.format();
  }
  hdr->UnsetAllocated();
  header_->total_alloc_.fetch_sub(hdr->page_size_);
}

}  // namespace hermes_shm::ipc
