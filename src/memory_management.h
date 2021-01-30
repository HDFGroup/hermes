/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Distributed under BSD 3-Clause license.                                   *
* Copyright by The HDF Group.                                               *
* Copyright by the Illinois Institute of Technology.                        *
* All rights reserved.                                                      *
*                                                                           *
* This file is part of Hermes. The full Hermes copyright notice, including  *
* terms governing use, modification, and redistribution, is contained in    *
* the COPYFILE, which can be found at the top directory. If you do not have *
* access to either file, you may request a copy from help@hdfgroup.org.     *
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#ifndef HERMES_MEMORY_MANAGEMENT_H_
#define HERMES_MEMORY_MANAGEMENT_H_

#include <assert.h>

#include <atomic>

#include "hermes_types.h"

/**
 * @file memory_management.h
 *
 * Structures for memory management.
 */

namespace hermes {

typedef void (ArenaErrorFunc)();

/**
 * Implements a ticket lock as described at
 * https://en.wikipedia.org/wiki/Ticket_lock.
 */
struct TicketMutex {
  std::atomic<u32> ticket;
  std::atomic<u32> serving;
};

struct ArenaInfo {
  size_t sizes[kArenaType_Count];
  size_t total;
};

/**
 * A block of memory that can be divided up dynamically.
 *
 * An Arena must be initialized from a contiguous block of allocated memory.
 * After it's initialized, you can use it like `new` via the PushStruct and
 * PushArray calls. Note that this Arena is designed to persist for the
 * application lifetime, so we don't have a need to free anything out of it.
 *
 * Example:
 * ```cpp
 * size_t arena_size = 4 * 1024;
 * void *block_base = malloc(arena_size);
 * Arena arena = {};
 * InitArena(&arena, arena_size, block_base);
 * MyStruct *s = PushStruct<MyStruct>(&arena);
 * ```
 */
struct Arena {
  /** The beginning of the memory block tracked by this Arena */
  u8 *base;
  /** Number of bytes currently in use */
  size_t used;
  /** Total number of bytes in the block tracked by this Arena */
  size_t capacity;
  /** Per-arena error handling function */
  ArenaErrorFunc *error_handler;
  /** The number of ScopedTemporaryMemory instances that are using this Arena */
  i32 temp_count;
};

struct Heap {
  ArenaErrorFunc *error_handler;
  TicketMutex mutex;
  /** Offset of the beginning of this heap's memory, relative to the Heap */
  u32 base_offset;
  /** Offset of the head of the free list, relative to base_offset */
  u32 free_list_offset;
  u32 extent;
  u16 alignment;
  u16 grows_up;
};

struct FreeBlockHeader {
  size_t size;
};

struct FreeBlock {
  /* The offset of the next FreeBlock in the list. Offset 0 represents NULL */
  u32 next_offset;
  /* The size of the next FreeBlock in the list. */
  u32 size;
};

struct TemporaryMemory {
  Arena *arena;
  size_t used;
};

/**
 * A block of memory that can be used dynamically, but is deleted when exiting
 * the current scope.
 *
 * ScopedTemporaryMemory wraps an existing Arena. You can pass an instance of it
 * as you would normally pass an Arena instance to PushSize and PushArray. The
 * difference is that the memory is reclaimed once the scope in which
 * ScopedTemporaryMemory is declared is exited. Note that there is no actual
 * `free` happening. Rather, the existing Arena's `used` parameter is reset to
 * where it was before the ScopedTemporaryMemory was created.
 *
 * Example:
 * ```cpp
 * {
 *   ScopedTemporaryMemory temp_memory(existing_arena_ptr);
 *   MyStruct *scoped_array = PushArray<MyStruct>(temp_memory, num_elements);
 *   for (int i = 0; i < num_elements; ++i) {
 *     // ...
 *   }
 * } // scoped_array memory is reclaimed here
 * ```
 */
struct ScopedTemporaryMemory {
  /** An existing, backing Arena */
  Arena *arena;
  /** The stored number of bytes in use by the backing Arena */
  size_t used;

  ScopedTemporaryMemory() = delete;
  ScopedTemporaryMemory(const ScopedTemporaryMemory &) = delete;
  ScopedTemporaryMemory& operator=(const ScopedTemporaryMemory &) = delete;

  /**
   * Creates a ScopedTemporaryMemory from an existing, backing Arena.
   *
   * @param backing_arena The existing Arena backing the temporary memory.
   */
  explicit ScopedTemporaryMemory(Arena *backing_arena)
      : arena(backing_arena), used(backing_arena->used) {
    // TODO(chogan): Currently not threadsafe unless each thread has a different
    // `backing_arena`
    if (++backing_arena->temp_count > 1) {
      HERMES_NOT_IMPLEMENTED_YET;
    }
  }

  /**
   * Destructor. Restores the backing Arena's bytes in use to the saved `used`
   * value, effectively reclaiming the memory that was used in this scope.
   */
  ~ScopedTemporaryMemory() {
    assert(arena->used >= used);
    arena->used = used;
    assert(arena->temp_count > 0);
    arena->temp_count--;
  }

  /**
   * Allows passing a ScopedTemporaryMemory to functions that take an Arena
   * without an explicit cast.
   */
  operator Arena*() {
    return arena;
  }
};

/**
 *
 */
TemporaryMemory BeginTemporaryMemory(Arena *arena);

/**
 *
 */
void EndTemporaryMemory(TemporaryMemory *temp_memory);

/**
 * Initializes an Arena with a starting size and base pointer. The @p base
 * parameter must point to a contiguous region of allocated memory of at least
 * @p bytes bytes.
 *
 * @param[in,out] arena The Arena to initialize.
 * @param[in] bytes The desired size in bytes of the Arena's backing memory.
 * @param[in] base A pointer to the beginning of the Arena's backing memory.
 */
void InitArena(Arena *arena, size_t bytes, u8 *base);

/**
 * Initializes an Arena with a starting size. This function uses malloc to
 * allocate a contiguous region of length @p bytes for the arena.
 *
 * @param[in] bytes The desired size in bytes to allocate.
 *
 * @return An arena with capacity @p bytes.
 */
Arena InitArenaAndAllocate(size_t bytes);

/**
 * Frees the memory backing the Arena, and zeros out all its fields.
 *
 * Only Arenas whose backing memory was created by malloc should be destroyed
 * with this function. Arenas using shared memory as the backing store should
 * not be destroyed. The backing memory is reclaimed when the shared memory is
 * unlinked.
 *
 * @param[in,out] arena The Arena to destroy.
 */
void DestroyArena(Arena *arena);

/**
 * Returns the amount of free space left in an Arena.
 *
 * @param[in] arena The Arena to query.
 *
 * @return The number of free bytes remaining
 */
size_t GetRemainingCapacity(Arena *arena);

/**
 * Expands the backing memory for an arena to be `new_size` bytes.
 *
 * Becuase this function uses `realloc`, it will only work for Arenas whose
 * backing store was created with malloc. It cannot be used with Arenas whose
 * backing store is shared memory (e.g., the BufferPool arena).
 *
 * @param arena The arena to expand.
 * @param new_size The new (larger) size that the Arena should occupy.
 */
void GrowArena(Arena *arena, size_t new_size);

/**
 * Returns a pointer to a raw region of @p size bytes.
 *
 * This is the lower level Arena allocation function. The higher level
 * PushStruct and PushArray should be used by clients.
 *
 * @param[in,out] arena The Arena to allocate from.
 * @param[in] size The requested memory size in bytes.
 * @param[in] alignment Align the result to a desired multiple.
 *
 * @return A pointer to the beginning of a contiguous region of @p size bytes.
 */
u8 *PushSize(Arena *arena, size_t size, size_t alignment = 8);

/**
 * Returns a pointer to a raw region of bytes that are cleared to zero.
 *
 * Like PushSize, but clears the requested region to zero.
 *
 * @param[in,out] arena The Arena to allocate from.
 * @param[in] size The requested memory size in bytes.
 * @param[in] alignment Align the result to a desired multiple.
 *
 * @return A pointer to the beginning of a contiguous region of @p size zeros.
 */
u8 *PushSizeAndClear(Arena *arena, size_t size, size_t alignment = 8);

/**
 * Reserves space for and returns a pointer to a T instance.
 *
 * Note that the T instance will be uninitialized. The caller is responsible for
 * any initialization.
 *
 * @param[in,out] arena The backing Arena from which to reserve space.
 * @param[in] alignment Align the result to a desired multiple.
 *
 * @return A pointer to an uninitialized `T` instance.
 */
template<typename T>
inline T *PushStruct(Arena *arena, size_t alignment = 8) {
  T *result = reinterpret_cast<T *>(PushSize(arena, sizeof(T), alignment));

  return result;
}

/**
 * Reserves space for, clears to zero, and returns a pointer to a T instance.
 *
 * Like PushStruct, but all the object's members are initialized to zero.
 * Note: assumes 0/NULL is a valid value for all the object's members.
 *
 * @param[in,out] arena The backing Arena from which to reserve space.
 * @param[in] alignment Align the result to a desired multiple.
 *
 * @return A pointer to a `T` instance with all members initialized to zero.
 */
template<typename T>
inline T *PushClearedStruct(Arena *arena, size_t alignment = 8) {
  T *result = reinterpret_cast<T *>(PushSizeAndClear(arena, sizeof(T),
                                                     alignment));

  return result;
}

/**
 * Reserves space for @p count `T` objects and returns a pointer to the first
 * one.
 *
 * The objects will be uninitialized.
 *
 * @param[in,out] arena The backing Arena from which to reserve space.
 * @param[in] count The number of objects to allocate.
 * @param[in] alignment Align the result to a desired multiple.
 *
 * @return A pointer to the first `T` instance in the uninitialized array.
 */
template<typename T>
inline T *PushArray(Arena *arena, int count, size_t alignment = 8) {
  T *result = reinterpret_cast<T *>(PushSize(arena, sizeof(T) * count,
                                             alignment));

  return result;
}

/**
 * Reserves space for @p count `T` objects, clears them to zero, and returns a
 * pointer to the first one.
 *
 * @param[in,out] arena The backing Arena from which to reserve space.
 * @param[in] count The number of objects to allocate.
 * @param[in] alignment Align the result to a desired multiple.
 *
 * @return A pointer to the first `T` instance in the array.
 */
template<typename T>
inline T *PushClearedArray(Arena *arena, int count, size_t alignment = 8) {
  T *result = reinterpret_cast<T *>(PushSizeAndClear(arena, sizeof(T) * count,
                                                     alignment));

  return result;
}
u8 *HeapPushSize(Heap *heap, u32 size);

template<typename T>
inline T *HeapPushStruct(Heap *heap) {
  T *result = reinterpret_cast<T *>(HeapPushSize(heap, sizeof(T)));

  return result;
}

template<typename T>
inline T *HeapPushArray(Heap *heap, u32 count) {
  T *result = reinterpret_cast<T *>(HeapPushSize(heap, count * sizeof(T)));

  return result;
}

Heap *InitHeapInArena(Arena *arena, bool grows_up = true, u16 alignment = 8);
void HeapFree(Heap *heap, void *ptr);
void *HeapRealloc(Heap *heap, void *ptr, size_t size);
u32 GetHeapOffset(Heap *heap, u8 *ptr);
FreeBlock *NextFreeBlock(Heap *heap, FreeBlock *block);
FreeBlock *GetHeapFreeList(Heap *heap);
u8 *HeapOffsetToPtr(Heap *heap, u32 offset);
u8 *HeapExtentToPtr(Heap *heap);

void BeginTicketMutex(TicketMutex *mutex);
void EndTicketMutex(TicketMutex *mutex);

}  // namespace hermes

#endif  // HERMES_MEMORY_MANAGEMENT_H_
