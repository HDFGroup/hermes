#include "memory_arena.h"

/**
 * @file memory_arena.cc
 *
 * Basic Arena implementation.
 */

namespace hermes {

bool IsPowerOfTwo(size_t val) {
  bool result = (val & (val - 1)) == 0;

  return result;
}

uintptr_t AlignForward(uintptr_t addr, size_t alignment) {
  assert(IsPowerOfTwo(alignment));

  uintptr_t result = addr;
  uintptr_t a = (uintptr_t)alignment;
  uintptr_t remainder = addr & (a - 1);

  if (remainder != 0) {
    result += a - remainder;
  }

  return result;
}

void InitArena(Arena *arena, size_t bytes, u8 *base) {
  arena->base = base;
  arena->used = 0;
  arena->capacity = bytes;
  arena->temp_count = 0;
}

Arena InitArenaAndAllocate(size_t bytes) {
  Arena result = {};
  result.base = (u8 *)malloc(bytes);
  result.capacity = bytes;

  return result;
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

u8 *PushSize(Arena *arena, size_t size, size_t alignment) {
  // TODO(chogan): Account for possible size increase due to alignment
  // bool can_fit = GetAlignedSize(arena, size, alignment);
  bool can_fit = size + arena->used <= arena->capacity;

  if (!can_fit && arena->error_handler) {
    arena->error_handler(arena);
  }

  assert(can_fit);

  u8 *base_result = 0;
  base_result = arena->base + arena->used;
  u8 *result = (u8 *)AlignForward((uintptr_t)base_result, alignment);

  if (base_result != result) {
    ptrdiff_t alignment_size = result - base_result;
    arena->used += alignment_size;
    DLOG(INFO) << "PushSize added " << alignment_size
               << " bytes of padding for alignment" << std::endl;
  }
  arena->used += size;

  return result;
}

u8 *PushSizeAndClear(Arena *arena, size_t size, size_t alignment) {
  size_t previously_used = arena->used;
  u8 *result = PushSize(arena, size, alignment);
  // NOTE(chogan): Account for case when `size` is increased for alignment
  size_t bytes_to_clear = arena->used - previously_used;

  if (result) {
    memset(result, 0, bytes_to_clear);
  }

  return result;
}

void BeginTicketMutex(TicketMutex *mutex) {
  u32 ticket = mutex->ticket.fetch_add(1);
  while (ticket != mutex->serving.load()) {
    // TODO(chogan): @optimization This seems to be necessary if we expect
    // oversubscription. As soon as we have more MPI ranks than logical
    // cores, the performance of the ticket mutex drops dramatically. We
    // lose about 8% when yielding and not oversubscribed, but without
    // yielding, it is unusable when oversubscribed. I want to implement a
    // ticket mutex with a waiting array at some point:
    // https://arxiv.org/pdf/1810.01573.pdf. It looks like that should give
    // us the best of both worlds.
    sched_yield();
  }
}

void EndTicketMutex(TicketMutex *mutex) {
  mutex->serving.fetch_add(1);
}

}  // namespace hermes
