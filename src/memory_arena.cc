#include "memory_arena.h"

#include <cmath>

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

uintptr_t AlignBackward(uintptr_t addr, size_t alignment) {
  assert(IsPowerOfTwo(alignment));

  uintptr_t result = addr;
  uintptr_t a = (uintptr_t)alignment;
  uintptr_t remainder = addr & (a - 1);

  if (remainder != 0) {
    result -= remainder;
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

u8 *PushSize(Arena *arena, size_t size, bool grows_up, size_t alignment) {
  // TODO(chogan): Account for possible size increase due to alignment
  // bool can_fit = GetAlignedSize(arena, size, alignment);
  bool can_fit = size + arena->used <= arena->capacity;

  if (!can_fit && arena->error_handler) {
    arena->error_handler();
  }

  assert(can_fit);

  u8 *base_result = 0;
  u8 *result = 0;

  if (grows_up) {
    base_result = arena->base + arena->used;
    result = (u8 *)AlignForward((uintptr_t)base_result, alignment);
  } else {
    base_result = arena->base - arena->used;
    result = (u8 *)AlignBackward((uintptr_t)base_result, alignment);
  }


  if (base_result != result) {
    ptrdiff_t alignment_size;
    if (grows_up) {
      alignment_size = result - base_result;
    } else {
      alignment_size = base_result - result;
    }
    arena->used += alignment_size;
    DLOG(INFO) << "PushSize added " << alignment_size
               << " bytes of padding for alignment" << std::endl;
  }
  arena->used += size;

  return result;
}

u8 *PushSizeAndClear(Arena *arena, size_t size, bool grows_up,
                     size_t alignment) {
  size_t previously_used = arena->used;
  u8 *result = PushSize(arena, size, grows_up, alignment);
  // NOTE(chogan): Account for case when `size` is increased for alignment
  size_t bytes_to_clear = arena->used - previously_used;

  if (result) {
    memset(result, 0, bytes_to_clear);
  }

  return result;
}

u8 *GetHeapMemory(Heap *heap) {
  u8 *result = (u8 *)heap + heap->base_offset;

  return result;
}

static inline FreeBlock *GetHeapFreeList(Heap *heap) {
  FreeBlock *result = 0;
  if (heap->free_list_offset) {
    if (heap->grows_up) {
      result = (FreeBlock *)(GetHeapMemory(heap) + heap->free_list_offset);
    } else {
      result = (FreeBlock *)(GetHeapMemory(heap) - heap->free_list_offset -
                             sizeof(FreeBlock));
    }
  }

  return result;
}

static inline FreeBlock *NextFreeBlock(Heap *heap, FreeBlock *block) {
  FreeBlock *result = 0;
  if (heap->grows_up) {
    result = (FreeBlock *)(GetHeapMemory(heap) + block->next_offset);
  } else {
    result = (FreeBlock *)(GetHeapMemory(heap) - block->next_offset -
                           sizeof(FreeBlock));
  }

  return result;
}

static inline u32 GetHeapOffset(Heap *heap, FreeBlock *block) {
  int adjustment = heap->grows_up ? 0 : sizeof(FreeBlock);
  ptrdiff_t signed_result = (u8 *)block - GetHeapMemory(heap);
  u32 result = (u32)std::abs(signed_result) + adjustment;

  return result;
}

void HeapErrorHandler() {
  LOG(FATAL) << "Heap out of memory. Increase size of Metadata Arena\n"
             << std::endl;
}

Heap *InitHeapInArena(Arena *arena, bool grows_up, u16 alignment) {
  Heap *result = 0;
  if (grows_up) {
    result = PushClearedStruct<Heap>(arena);
  } else {
    result = (Heap *)((arena->base + arena->capacity) - sizeof(Heap));
    memset(result, 0, sizeof(Heap));
  }

  size_t heap_size = arena->capacity - arena->used;
  result->base_offset = grows_up ? (u8 *)(result + 1) - (u8 *)result : 0;
  result->error_handler = HeapErrorHandler;
  result->alignment = alignment;
  result->free_list_offset = alignment;
  result->grows_up = grows_up;

  FreeBlock *first_free_block = 0;
  if (grows_up) {
    first_free_block = (FreeBlock *)(GetHeapMemory(result) + alignment);
  } else {
    first_free_block =
      (FreeBlock *)(GetHeapMemory(result) - sizeof(FreeBlock) - alignment);
  }

  // NOTE(chogan): offset 0 represents NULL
  first_free_block->next_offset = 0;
  // NOTE(chogan): Subtract alignment since we're using the first `alignment`
  // sized block as the NULL block.
  first_free_block->size = heap_size - alignment;

  return result;
}

FreeBlock *FindFirstFit(Heap *heap, u32 desired_size) {
  FreeBlock *result = 0;
  FreeBlock *prev = 0;
  FreeBlock *head = GetHeapFreeList(heap);

  while (head) {
    if (head->size >= desired_size) {
      result = head;
      u32 remaining_size = head->size - desired_size;
      u32 next_offset = 0;

      if (remaining_size) {
        // NOTE(chogan): Split the remaining size off into a new FreeBlock
        FreeBlock *split_block = 0;
        if (heap->grows_up) {
          split_block = (FreeBlock *)((u8 *)result + desired_size);
        } else {
          split_block = (FreeBlock *)((u8 *)result - desired_size -
                                      sizeof(FreeBlock));
        }
        split_block->size = remaining_size;
        split_block->next_offset = result->next_offset;
        u32 split_block_offset = GetHeapOffset(heap, split_block);
        next_offset = split_block_offset;
      } else {
        next_offset = result->next_offset;
      }

      if (prev) {
        prev->next_offset = next_offset;
      } else {
        heap->free_list_offset = next_offset;
      }

      break;
    }
    prev = head;
    head = NextFreeBlock(heap, head);
  }

  if (!result && heap->error_handler) {
    heap->error_handler();
  }

  return result;
}

#if 0
FreeBlock *FindBestFit(FreeBlock *head, size_t desired_size, u32 threshold=0) {
  FreeBlock *result = 0;
  FreeBlock *prev = head;
  FreeBlock *smallest_prev = head;
  u32 smallest_wasted = 0xFFFFFFFF;

  while(head && smallest_wasted > threshold) {
    if (head->size >= desired_size) {
      u32 wasted_space = head->size - desired_size ;
      if (wasted_space < smallest_wasted) {
        smallest_wasted = wasted_space;
        result = head;
        smallest_prev = prev;
      }
    }
    prev = head;
    head = head->next;
  }

  if (result) {
    smallest_prev->next = result->next;
  }

  return result;
}
#endif

u8 *HeapPushSize(Heap *heap, u32 size) {
  u8 *result = 0;

  // TODO(chogan): Check for collision between Map Heap and ID List Heap

  // TODO(chogan): Think about lock granularity
  BeginTicketMutex(&heap->mutex);
  FreeBlock *first_fit = FindFirstFit(heap, size + sizeof(FreeBlockHeader));
  if (first_fit) {
    FreeBlockHeader *header = (FreeBlockHeader *)first_fit;
    header->size = size;
    result = (u8 *)(header + 1);
  } else {
    // TODO(chogan): @errorhandling
  }

  EndTicketMutex(&heap->mutex);

  return result;
}

void HeapFree(Heap *heap, void *ptr) {
  BeginTicketMutex(&heap->mutex);
  if (heap && ptr) {
    FreeBlockHeader *header = (FreeBlockHeader *)ptr - 1;
    u32 size = header->size;
    FreeBlock *new_block = (FreeBlock *)(header);
    new_block->size = size + sizeof(FreeBlockHeader);
    new_block->next_offset = heap->free_list_offset;
    heap->free_list_offset = GetHeapOffset(heap, new_block);
  }
  EndTicketMutex(&heap->mutex);
}

void *HeapRealloc(Heap *heap, void *ptr, size_t size) {

  if (ptr) {
    // Only support malloc behavior since the max Heap size can never grow
    // TODO(chogan): @errorhandling
    assert(!"Can't realloc in Heap\n");
  }

  void *result = HeapPushSize(heap, size);

  return result;
}

void CoalesceFreeBlocks(Heap *heap) {
  // TODO(chogan):
  if (heap->grows_up) {
  } else {
  }
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
