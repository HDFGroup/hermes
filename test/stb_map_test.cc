#include <stdio.h>

#include "test_utils.h"
#include "memory_arena.h"

#define STBDS_ASSERT(x) Assert((x))

#define STBDS_REALLOC(heap, ptr, size) hermes::HeapRealloc(heap, ptr, size)
#define STBDS_FREE(heap, ptr) hermes::HeapFree(heap, ptr)

#define STBDS_UNIT_TESTS
#define STB_DS_IMPLEMENTATION
#include "stb_ds.h"

using namespace hermes;

int main() {

  Arena arena = InitArenaAndAllocate(MEGABYTES(32));
  Heap *heap = InitHeapInArena(&arena, true, 8);

  stbds_unit_tests(heap);

  DestroyArena(&arena);

  return 0;
}
