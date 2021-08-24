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

#include <stdio.h>

#include "test_utils.h"
#include "memory_management.h"

#define STBDS_ASSERT(x) Assert((x))

#define STBDS_REALLOC(heap, ptr, size) hermes::HeapRealloc(heap, ptr, size)
#define STBDS_FREE(heap, ptr) hermes::HeapFree(heap, ptr)

#define STBDS_UNIT_TESTS
#define STB_DS_IMPLEMENTATION
#include "stb_ds.h"

using namespace hermes;  // NOLINT(*)

int main() {
  Arena arena = InitArenaAndAllocate(MEGABYTES(64));
  TemporaryMemory temp_memory = BeginTemporaryMemory(&arena);
  Heap *heap = InitHeapInArena(&arena, true, 8);
  stbds_unit_tests(heap);
  EndTemporaryMemory(&temp_memory);

  temp_memory = BeginTemporaryMemory(&arena);
  Heap *reverse_heap = InitHeapInArena(&arena, false, 8);
  stbds_unit_tests(reverse_heap);
  EndTemporaryMemory(&temp_memory);

  DestroyArena(&arena);

  return 0;
}
