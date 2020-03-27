#include "memory_arena.h"

/**
 * @file memory_arena.cc
 *
 * Basic Arena implementation.
 */

namespace hermes {

void InitArena(Arena *arena, size_t bytes, u8 *base) {
  arena->used = 0;
  arena->capacity = bytes;
  arena->base = base;
}

void DestroyArena(Arena *arena) {
  // TODO(chogan): Check for temp count?
  free(arena->base);
  arena->base = 0;
  arena->used = 0;
  arena->capacity = 0;
}

void GrowArena(Arena *arena, size_t new_size) {
  void *new_base = (u8 *)realloc(arena->base, new_size);
  if (new_base != arena->base) {
    arena->base = (u8 *)new_base;
    arena->capacity = new_size;
  } else {
    // TODO(chogan): @errorhandling
    assert(!"GrowArena failed\n");
  }
}

u8 *PushSize(Arena *arena, size_t size) {
  u8 *result = 0;
  assert(size + arena->used <= arena->capacity);
  result = arena->base + arena->used;
  arena->used += size;

  return result;
}

u8 *PushSizeAndClear(Arena *arena, size_t size) {
  u8 *result = PushSize(arena, size);
  if (result) {
    memset(result, 0, size);
  }

  return result;
}

}  // namespace hermes
