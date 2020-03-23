#ifndef HERMES_MEMORY_ARENA_H_
#define HERMES_MEMORY_ARENA_H_

#include <assert.h>

#include "hermes_types.h"

/**
 * @file memory_arena.h
 *
 * Structures for memory management.
 */

namespace hermes {

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
  /** The number of ScopedTemporaryMemory instances that are using this Arena */
  i32 temp_count;
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
      assert(!"ScopedTemporaryMemory is not threadsafe yet\n");
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
 * Returns a pointer to a raw region of @p size bytes.
 *
 * This is the lower level Arena allocation function. The higher level
 * PushStruct and PushArray should be used by clients.
 *
 * @param[in,out] arena The Arena to allocate from.
 * @param[in] size The requested memory size in bytes.
 *
 * @return A pointer to the beginning of a contiguous region of @p size bytes.
 */
u8 *PushSize(Arena *arena, size_t size);

/**
 * Returns a pointer to a raw region of bytes that are cleared to zero.
 *
 * Like PushSize, but clears the requested region to zero.
 *
 * @param[in,out] arena The Arena to allocate from.
 * @param[in] size The requested memory size in bytes.
 *
 * @return A pointer to the beginning of a contiguous region of @p size zeros.
 */
u8 *PushSizeAndClear(Arena *arena, size_t size);

/**
 * Reserves space for and returns a pointer to a T instance.
 *
 * Note that the T instance will be uninitialized. The caller is responsible for
 * any initialization.
 *
 * @param arena The backing Arena from which to reserve space.
 *
 * @return A pointer to an uninitialized `T` instance.
 */
template<typename T>
inline T *PushStruct(Arena *arena) {
  T *result = reinterpret_cast<T *>(PushSize(arena, sizeof(T)));

  return result;
}

/**
 * Reserves space for, clears to zero, and returns a pointer to a T instance.
 *
 * Like PushStruct, but all the object's members are initialized to zero.
 * Note: assumes 0/NULL is a valid value for all the object's members.
 *
 * @param arena The backing Arena from which to reserve space.
 *
 * @return A pointer to a `T` instance with all members initialized to zero.
 */
template<typename T>
inline T *PushClearedStruct(Arena *arena) {
  T *result = reinterpret_cast<T *>(PushSizeAndClear(arena, sizeof(T)));

  return result;
}

/**
 * Reserves space for @p count `T` objects and returns a pointer to the first
 * one.
 *
 * The objects will be uninitialized.
 *
 * @param arena The backing Arena from which to reserve space.
 * @param count The number of objects to allocate.
 *
 * @return A pointer to the first `T` instance in the uninitialized array.
 */
template<typename T>
inline T *PushArray(Arena *arena, int count) {
  T *result = reinterpret_cast<T *>(PushSize(arena, sizeof(T) * count));

  return result;
}

}  // namespace hermes

#endif  // HERMES_MEMORY_ARENA_H_
