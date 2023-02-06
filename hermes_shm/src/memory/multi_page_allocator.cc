//
// Created by lukemartinlogan on 12/25/22.
//

#include "hermes_shm/memory/allocator/multi_page_allocator.h"

namespace hermes_shm::ipc {

/**
 * base^y = x; solve for y (round up)
 * */
uint32_t CountLogarithm(RealNumber base, size_t x, size_t &round) {
  uint32_t y = 1;
  RealNumber compound(base);
  while (compound.as_int() < x) {
    compound *= base;
    RealNumber compound_exp = compound * compound;
    y += 1;
    if (compound_exp.as_int() <= x) {
      y *= 2;
      compound = compound_exp;
    }
  }
  round = compound.as_int();
  return y;
}

/**
 * The index of the free list to allocate pages from
 * */
inline uint32_t MultiPageAllocator::IndexLogarithm(size_t x, size_t &round) {
  if (x <= header_->min_page_size_) {
    round = header_->min_page_size_;
    return 0;
  } else if (x > header_->max_page_size_){
    round = x;
    return header_->last_page_idx_;
  }
  size_t log = CountLogarithm(header_->growth_rate_, x, round);
  return log - header_->min_page_log_;
}

/**
 * Initialize the allocator in shared memory
 * */
void MultiPageAllocator::shm_init(MemoryBackend *backend,
                                  allocator_id_t alloc_id,
                                  size_t custom_header_size,
                                  size_t min_page_size,
                                  size_t max_page_size,
                                  RealNumber growth_rate,
                                  size_t coalesce_min_size,
                                  RealNumber coalesce_frac,
                                  size_t thread_table_size,
                                  uint32_t concurrency) {
}

/**
 * Attach an existing allocator from shared memory
 * */
void MultiPageAllocator::shm_deserialize(MemoryBackend *backend) {
}

/**
 * Allocate a memory of \a size size.
 * */
OffsetPointer MultiPageAllocator::AllocateOffset(size_t size) {
  return OffsetPointer();
}

/**
 * Allocate a memory of \a size size, which is aligned to \a
 * alignment.
 * */
OffsetPointer MultiPageAllocator::AlignedAllocateOffset(size_t size,
                                                        size_t alignment) {
  return OffsetPointer();
}

/**
 * Reallocate \a p pointer to \a new_size new size.
 *
 * @return whether or not the pointer p was changed
 * */
OffsetPointer MultiPageAllocator::ReallocateOffsetNoNullCheck(OffsetPointer p,
                                                     size_t new_size) {
  return OffsetPointer();
}

/**
 * Free \a ptr pointer. Null check is performed elsewhere.
 * */
void MultiPageAllocator::FreeOffsetNoNullCheck(OffsetPointer p) {
}

/**
 * Get the current amount of data allocated. Can be used for leak
 * checking.
 * */
size_t MultiPageAllocator::GetCurrentlyAllocatedSize() {
  return 0;
}

}  // namespace hermes_shm::ipc