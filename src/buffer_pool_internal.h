#ifndef HERMES_BUFFER_POOL_INTERNAL_H_
#define HERMES_BUFFER_POOL_INTERNAL_H_

#include "buffer_pool.h"
#include "memory_arena.h"

/**
 * @file buffer_pool_internal.h
 *
 * This file contains the developer interface to the BufferPool. It describes
 * the API available to direct consumers of the BufferPool which include buffer
 * pool tests, the buffer pool visualizer, and the BufferPool itself. For the
 * "public" API, see buffer_pool.h.
 */

namespace hermes {

/**
 * Rounds a value up to the next given multiple.
 *
 * Returns the original value if it is already divisible by multiple.
 *
 * Example:
 * ```cpp
 * size_t result = RoundUpToMultiple(2000, 4096);
 * assert(result == 4096);
 * ```
 *
 * @param val The initial value to round.
 * @param multiple The multiple to round up to.
 *
 * @return The next multiple of multiple that is greater than or equal to val.
 */
size_t RoundUpToMultiple(size_t val, size_t multiple);

/**
 * Rounds a value down to the previous given multiple.
 *
 * Returns the original value if it is already divisible by multiple.
 *
 * Example:
 * ```cpp
 *   size_t result = RoundDownToMultiple(4097, 4096);
 *   assert(result == 4096);
 * ```
 *
 * @param val The initial value to round.
 * @param multiple The multiple to round down to.
 *
 * @return The previous multiple of multiple that is less than or equal to val.
 */
size_t RoundDownToMultiple(size_t val, size_t multiple);

/**
 * Initializes a BufferPool inside an existing shared memory segment.
 *
 * Divides the shared memory segment pointed to by hermes_memory into
 * 1) An array of RAM buffers.
 * 2) An array of BufferHeaders.
 * 3) An array of Tiers.
 * 4) The BufferPool struct, which contains pointers (really offsets) to the
 *    data above.
 *
 * The sizes of each of these sections is controlled by the config parameter.
 * Each BufferHeader is initialized to point to a specfic offset. In the case of
 * a RAM buffer, this offset is from the beginning of the shared memory. In the
 * case of a file buffer, the offset is from the beginning of a file. Free lists
 * for each slab in each Tier are also constructed.
 *
 * @param hermes_memory The base pointer to a shared memory segment.
 * @param buffer_pool_arena An Arena backed by the shared memory segment pointed
 * to by hermes_shared_memory.
 * @param scratch_arena An arena for temporary allocations that are destroyed
 * when this function exits.
 * @param node_id The identifier of the node this function is running on.
 * @param config Configuration that specifies how the BufferPool is constructed.
 *
 * @return The offset of the beginning of the BufferPool from the beginning of
 * shared memory.
 */
ptrdiff_t InitBufferPool(u8 *hermes_memory, Arena *buffer_pool_arena,
                         Arena *scratch_arena, i32 node_id, Config *config);

/**
 * Obtains a pointer to the BufferPool constructed in shared memory.
 *
 * Since the BufferPool lives in shared memory, this pointer should never be
 * freed. It is only destroyed when the Hermes core closes the shared memory.
 *
 * @param context The shared memory context for accessing the BufferPool.
 *
 * @return A pointer to the BufferPool constructed in the shared memory
 * represented by @p context.
 */
BufferPool *GetBufferPoolFromContext(SharedMemoryContext *context);

/**
 * Returns a pointer to the Tier with index tier_id in the shared memory
 * context.
 *
 * This pointer should never be freed, since it lives in shared memory and is
 * managed by the Hermes core.
 *
 * @param context The shared memory context where the Tiers are stored.
 * @param tier_id An identifier for the desired Tier. This is an index into an
 * array of Tiers.
 *
 * @return A pointer to the Tier with ID tier_id.
 */
Tier *GetTierById(SharedMemoryContext *context, TierID tier_id);

/**
 * Returns a pointer to the first BufferHeader in the array of BufferHeaders
 * constructed in the shared memory context.
 *
 * This pointer should never be freed, as it is managed by the Hermes core.
 * Indexing off this pointer, one can easily iterate through all BufferHeaders.
 * When retrieving a specific header, use GetHeaderByBufferId.
 *
 * Example:
 * ```cpp
 * BufferHeader *headers = GetHeadersBase(context);
 * for (u32 i = 0; i < pool->num_headers[tier_id]; ++i) {
 *   BufferHeader *header = headers + i;
 *   // ...
 * }
 * ```
 *
 * @param context The shared memory context where the BufferHeaders live.
 *
 * @return A pointer to the first BufferHeader in the array of BufferHeaders.
 */
BufferHeader *GetHeadersBase(SharedMemoryContext *context);

/**
 * Retrieves the BufferHeader that corresponds to BufferID @p id.
 *
 * This pointer should never be freed. The BufferHeaders are managed by the
 * Hermes core.
 *
 * @param context The shared memory context for accessing the BufferHeaders.
 * @param id The desired BufferID to retrieve.
 *
 * @return A pointer to the BufferHeader that corresponds to @p id.
 */
BufferHeader *GetHeaderByBufferId(SharedMemoryContext *context, BufferID id);

/**
 * Returns whether or not @p header is currently backed by physical storage.
 *
 * A dormant header has no storage associated with it. It exists to facilitate
 * the splitting and merging mechanisms. Splitting buffers into smaller buffers
 * requires more headers. When that happens, dormant headers become associated
 * with backing storage, and become "live."
 *
 * @param header The header to check.
 *
 * @return true if @p header is not backed by physical storage, false otherwise.
 */
bool HeaderIsDormant(BufferHeader *header);

/**
 * TODO(chogan):
 *
 * @param context The shared memory context for accessing the BufferPool.
 * @param slab_index The 0-based index of the slab to split.
 */
void SplitRamBufferFreeList(SharedMemoryContext *context, int slab_index);

/**
 * TODO(chogan):
 *
 * @param context The shared memory context for accessing the BufferPool
 * @param slab_index The 0-based index of the slab to merge.
 */
void MergeRamBufferFreeList(SharedMemoryContext *context, int slab_index);

/**
 *
 */
BufferID PeekFirstFreeBufferId(SharedMemoryContext *context, TierID tier_id,
                               int slab_index);

/**
 *
 */
void LocalReleaseBuffer(SharedMemoryContext *context, BufferID buffer_id);

/**
 *
 */
void LocalReleaseBuffers(SharedMemoryContext *context,
                         const std::vector<BufferID> &buffer_ids);
/**
 *
 */
i32 GetSlabUnitSize(SharedMemoryContext *context, TierID tier_id,
                    int slab_index);

/**
 *
 */
i32 GetSlabBufferSize(SharedMemoryContext *context, TierID tier_id,
                      int slab_index);

/**
 *
 */
void SerializeBufferPoolToFile(SharedMemoryContext *context, FILE *file);

/**
 *
 */
void ParseConfig(Arena *arena, const char *path, Config *config);

/**
 *
 */
u8 *InitSharedMemory(const char *shmem_name, size_t total_size);

/**
 *  Lets Thallium know how to serialize a BufferID.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param buffer_id The BufferID to serialize.
 */
template<typename A>
void serialize(A &ar, BufferID &buffer_id) {
  ar & buffer_id.as_int;
}

}  // namespace hermes
#endif  // HERMES_BUFFER_POOL_INTERNAL_H_
