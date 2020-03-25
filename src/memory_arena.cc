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
