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

#include <malloc.h>
#include <stdlib.h>
#include "hermes_shm/memory/memory_manager.h"

using hermes_shm::ipc::Pointer;
using hermes_shm::ipc::Allocator;

/** Allocate SIZE bytes of memory. */
void* malloc(size_t size) {
  auto alloc = HERMES_SHM_MEMORY_MANAGER->GetDefaultAllocator();
  return alloc->AllocatePtr<void>(size);
}

/** Allocate NMEMB elements of SIZE bytes each, all initialized to 0. */
void* calloc(size_t nmemb, size_t size) {
  auto alloc = HERMES_SHM_MEMORY_MANAGER->GetDefaultAllocator();
  return alloc->ClearAllocatePtr<void>(nmemb * size);
}

/**
 * Re-allocate the previously allocated block in ptr, making the new
 * block SIZE bytes long.
 * */
void* realloc(void *ptr, size_t size) {
  Pointer p = HERMES_SHM_MEMORY_MANAGER->Convert(ptr);
  auto alloc = HERMES_SHM_MEMORY_MANAGER->GetAllocator(p.allocator_id_);
  return alloc->AllocatePtr<void>(size);
}

/**
 * Re-allocate the previously allocated block in PTR, making the new
 * block large enough for NMEMB elements of SIZE bytes each.
 * */
void* reallocarray(void *ptr, size_t nmemb, size_t size) {
  return realloc(ptr, nmemb * size);
}

/** Free a block allocated by `malloc', `realloc' or `calloc'. */
void free(void *ptr) {
  Pointer p = HERMES_SHM_MEMORY_MANAGER->Convert(ptr);
  auto alloc = HERMES_SHM_MEMORY_MANAGER->GetAllocator(p.allocator_id_);
  alloc->Free(p);
}

/** Allocate SIZE bytes allocated to ALIGNMENT bytes. */
void* memalign(size_t alignment, size_t size) {
  // TODO(llogan): need to add an aligned allocator
  return malloc(size);
}

/** Allocate SIZE bytes on a page boundary. */
void* valloc(size_t size) {
  return memalign(HERMES_SHM_SYSTEM_INFO->page_size_, size);
}

/**
 * Equivalent to valloc(minimum-page-that-holds(n)),
 * that is, round up size to nearest pagesize.
 * */
void* pvalloc(size_t size) {
  size_t new_size = hermes_shm::ipc::NextPageSizeMultiple(size);
  return valloc(new_size);
}

/**
 * Allocates size bytes and places the address of the
 * allocated memory in *memptr. The address of the allocated memory
 * will be a multiple of alignment, which must be a power of two and a multiple
 * of sizeof(void *). Returns NULL if size is 0. */
int posix_memalign(void **memptr, size_t alignment, size_t size) {
  (*memptr) = memalign(alignment, size);
  return 0;
}

/**
 * Aligned to an alignment with a size that is a multiple of the
 * alignment
 * */
void *aligned_alloc(size_t alignment, size_t size) {
  return memalign(alignment, hermes_shm::ipc::NextAlignmentMultiple(alignment, size));
}
