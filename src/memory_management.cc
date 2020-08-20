#include "memory_management.h"

#include <cmath>

/**
 * @file memory_management.cc
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

size_t GetRemainingCapacity(Arena *arena) {
  size_t result = arena->capacity - arena->used;

  return result;
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

TemporaryMemory BeginTemporaryMemory(Arena *arena) {
  TemporaryMemory result = {};
  result.arena = arena;
  result.used = arena->used;
  if (++arena->temp_count > 1) {
    HERMES_NOT_IMPLEMENTED_YET;
  }

  return result;
}

void EndTemporaryMemory(TemporaryMemory *temp_memory) {
  temp_memory->arena->used = temp_memory->used;
  temp_memory->arena->temp_count--;
  assert(temp_memory->arena->temp_count >= 0);
}

u8 *PushSize(Arena *arena, size_t size, size_t alignment) {
  // TODO(chogan): Account for possible size increase due to alignment
  // bool can_fit = GetAlignedSize(arena, size, alignment);
  bool can_fit = size + arena->used <= arena->capacity;

  if (!can_fit && arena->error_handler) {
    arena->error_handler();
  }

  assert(can_fit);

  u8 *base_result = arena->base + arena->used;
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

u8 *GetHeapMemory(Heap *heap) {
  u8 *result = (u8 *)heap + heap->base_offset;

  return result;
}

FreeBlock *GetHeapFreeList(Heap *heap) {
  FreeBlock *result = 0;
  if (heap->free_list_offset) {
    if (heap->grows_up) {
      result = (FreeBlock *)(GetHeapMemory(heap) + heap->free_list_offset);
    } else {
      result = (FreeBlock *)(GetHeapMemory(heap) - heap->free_list_offset);
    }
  }

  return result;
}

FreeBlock *NextFreeBlock(Heap *heap, FreeBlock *block) {
  FreeBlock *result = 0;

  if (block->next_offset) {
    if (heap->grows_up) {
      result = (FreeBlock *)(GetHeapMemory(heap) + block->next_offset);
    } else {
      result = (FreeBlock *)(GetHeapMemory(heap) - block->next_offset);
    }
  }

  return result;
}

u8 *HeapOffsetToPtr(Heap *heap, u32 offset) {
  u8 *result = 0;
  if (heap->grows_up) {
    result = GetHeapMemory(heap) + offset;
  } else {
    result = GetHeapMemory(heap) - offset;
  }

  return result;
}

u32 GetHeapOffset(Heap *heap, u8 *ptr) {
  ptrdiff_t signed_result = (u8 *)ptr - GetHeapMemory(heap);
  u32 result = (u32)std::abs(signed_result);

  return result;
}

void HeapErrorHandler() {
  LOG(FATAL) << "Heap out of memory. Increase metadata_arena_percentage "
             << "in Hermes configuration." << std::endl;
}

u32 ComputeHeapExtent(Heap *heap, void *item, u32 size) {
  u32 result = 0;
  if (heap->grows_up) {
    result = ((u8 *)item + size) - GetHeapMemory(heap);
  } else {
    result = GetHeapMemory(heap) - (u8 *)item;
  }

  return result;
}

u8 *HeapExtentToPtr(Heap *heap) {
  u8 *result = 0;
  BeginTicketMutex(&heap->mutex);
  if (heap->grows_up) {
    result = GetHeapMemory(heap) + heap->extent;
  } else {
    result = GetHeapMemory(heap) - heap->extent;
  }
  EndTicketMutex(&heap->mutex);

  return result;
}

Heap *InitHeapInArena(Arena *arena, bool grows_up, u16 alignment) {
  Heap *result = 0;

  if (arena->capacity >= 4UL * 1024UL * 1024UL * 1024UL) {
    LOG(FATAL) << "Metadata heap cannot be larger than 4GB. Decrease "
               << "metadata_arena_percentage in the Hermes configuration."
               << std::endl;
  }

  size_t heap_size = 0;
  if (grows_up) {
    result = PushClearedStruct<Heap>(arena);
    heap_size = GetRemainingCapacity(arena);
  } else {
    result = (Heap *)((arena->base + arena->capacity) - sizeof(Heap));
    heap_size = GetRemainingCapacity(arena) - sizeof(Heap);
    memset((void *)result, 0, sizeof(Heap));
  }

  HERMES_DEBUG_SERVER_INIT(grows_up);

  result->base_offset = grows_up ? (u8 *)(result + 1) - (u8 *)result : 0;
  result->error_handler = HeapErrorHandler;
  result->alignment = alignment;
  result->free_list_offset = alignment + (grows_up ? 0 : sizeof(FreeBlock));
  result->grows_up = grows_up;

  FreeBlock *first_free_block = GetHeapFreeList(result);

  // NOTE(chogan): offset 0 represents NULL
  first_free_block->next_offset = 0;
  // NOTE(chogan): Subtract alignment since we're using the first `alignment`
  // sized block as the NULL block.
  first_free_block->size = heap_size - alignment;

  result->extent = ComputeHeapExtent(result, first_free_block,
                                     sizeof(*first_free_block));

  return result;
}

FreeBlock *FindFirstFit(Heap *heap, u32 desired_size) {
  const u32 min_free_block_size = 8 + sizeof(FreeBlock);
  FreeBlock *result = 0;
  FreeBlock *prev = 0;
  FreeBlock *head = GetHeapFreeList(heap);

  while (head) {
    if (head->size >= desired_size) {
      result = head;
      u32 remaining_size = head->size - desired_size;
      result->size = desired_size;
      u32 next_offset = 0;

      if (remaining_size >= min_free_block_size) {
        // NOTE(chogan): Split the remaining size off into a new FreeBlock
        FreeBlock *split_block = 0;
        if (heap->grows_up) {
          split_block = (FreeBlock *)((u8 *)result + desired_size);
        } else {
          split_block = (FreeBlock *)((u8 *)result - desired_size);
        }
        split_block->size = remaining_size;
        split_block->next_offset = result->next_offset;
        u32 split_block_offset = GetHeapOffset(heap, (u8 *)split_block);
        next_offset = split_block_offset;
      } else {
        next_offset = result->next_offset;
        result->size += remaining_size;
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

  HERMES_DEBUG_CLIENT_INIT();

  if (size) {
    BeginTicketMutex(&heap->mutex);
    // TODO(chogan): Respect heap->alignment
    FreeBlock *first_fit = FindFirstFit(heap, size + sizeof(FreeBlockHeader));
    EndTicketMutex(&heap->mutex);

    if (first_fit) {
      u32 actual_size = first_fit->size - sizeof(FreeBlockHeader);
      FreeBlockHeader *header = 0;
      if (heap->grows_up) {
        header = (FreeBlockHeader *)first_fit;
      } else {
        header = (FreeBlockHeader *)((u8 *)first_fit - actual_size);
      }
      header->size = actual_size;
      result = (u8 *)(header + 1);

      HERMES_DEBUG_TRACK_ALLOCATION(header, header->size + sizeof(FreeBlockHeader),
                             heap->grows_up);

      u32 extent_adustment = heap->grows_up ? 0 : sizeof(FreeBlock);

      BeginTicketMutex(&heap->mutex);
      u32 this_extent =
        ComputeHeapExtent(heap, header, header->size + sizeof(FreeBlockHeader));
      heap->extent = std::max(heap->extent, this_extent);
      if (heap->extent == heap->free_list_offset - extent_adustment) {
        heap->extent += sizeof(FreeBlock);
      }
      EndTicketMutex(&heap->mutex);
    } else {
      // TODO(chogan): @errorhandling
      heap->error_handler();
    }
  }

  return result;
}

void HeapFree(Heap *heap, void *ptr) {
  if (heap && ptr) {
    FreeBlockHeader *header = (FreeBlockHeader *)ptr - 1;
    u32 size = header->size;
    FreeBlock *new_block = 0;
    if (heap->grows_up) {
      new_block = (FreeBlock *)header;
    } else {
      new_block = (FreeBlock *)((u8 *)(header + 1) + header->size - sizeof(FreeBlock));
    }
    new_block->size = size + sizeof(FreeBlockHeader);

    HERMES_DEBUG_TRACK_FREE(header, new_block->size, heap->grows_up);

    BeginTicketMutex(&heap->mutex);
    u32 extent = ComputeHeapExtent(heap, ptr, size);
    if (extent == heap->extent) {
      assert(new_block->size < heap->extent);
      heap->extent -= new_block->size;
    }

    new_block->next_offset = heap->free_list_offset;
    heap->free_list_offset = GetHeapOffset(heap, (u8 *)new_block);
    EndTicketMutex(&heap->mutex);
  }
}

void *HeapRealloc(Heap *heap, void *ptr, size_t size) {

  if (ptr) {
    // NOTE(chogan): We only support malloc behavior for now. The stb_ds.h hash
    // map uses STBDS_REALLOC for its malloc style allocations. We just give the
    // initial map a large enough size so that it never needs to realloc.

    // TODO(chogan): @errorhandling
    assert(!"Can't realloc in Heap\n");
  }

  void *result = HeapPushSize(heap, size);

  return result;
}

void CoalesceFreeBlocks(Heap *heap) {
  (void)heap;
  // TODO(chogan): This will run as a deferred coalesce. How often?
  // Test case: Lots of blocks are free but none of the pointers are adjacent
  // X: A used block
  // n: A link to free block with index n
  // /: End of the free list

  //  0 1 2 3 4 5 6 7 8
  // |2|/|4|1|6|3|8|X|5|
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
