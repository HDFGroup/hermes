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

#ifndef HERMES_BUFFER_POOL_INTERNAL_H_
#define HERMES_BUFFER_POOL_INTERNAL_H_

#include "buffer_pool.h"
#include "memory_management.h"

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
 * Initializes a BufferPool inside an existing shared memory segment.
 *
 * Divides the shared memory segment pointed to by hermes_memory into
 * 1) An array of RAM buffers.
 * 2) An array of BufferHeaders.
 * 3) An array of Devices.
 * 4) The BufferPool struct, which contains pointers (really offsets) to the
 *    data above.
 *
 * The sizes of each of these sections is controlled by the config parameter.
 * Each BufferHeader is initialized to point to a specfic offset. In the case of
 * a RAM buffer, this offset is from the beginning of the shared memory. In the
 * case of a file buffer, the offset is from the beginning of a file. Free lists
 * for each slab in each Device are also constructed.
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
 * Returns a pointer to the Device with index device_id in the shared memory
 * context.
 *
 * This pointer should never be freed, since it lives in shared memory and is
 * managed by the Hermes core.
 *
 * @param context The shared memory context where the Devices are stored.
 * @param device_id An identifier for the desired Device. This is an index into
 * an array of Devices.
 *
 * @return A pointer to the Device with ID device_id.
 */
Device *GetDeviceById(SharedMemoryContext *context, DeviceID device_id);

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
 * for (u32 i = 0; i < pool->num_headers[device_id]; ++i) {
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
BufferID PeekFirstFreeBufferId(SharedMemoryContext *context, DeviceID device_id,
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
i32 GetSlabUnitSize(SharedMemoryContext *context, DeviceID device_id,
                    int slab_index);

/**
 *
 */
u32 LocalGetBufferSize(SharedMemoryContext *context, BufferID id);
/**
 *
 */
i32 GetSlabBufferSize(SharedMemoryContext *context, DeviceID device_id,
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
 *
 */
u8 *GetRamBufferPtr(SharedMemoryContext *context, BufferID buffer_id);

/**
 *
 */
Target *GetTarget(SharedMemoryContext *context, int index);

/**
 *
 */
Target *GetTargetFromId(SharedMemoryContext *context, TargetID id);

/**
 *
 */
std::atomic<u32> *GetAvailableBuffersArray(SharedMemoryContext *context,
                                           DeviceID device_id);
}  // namespace hermes

#endif  // HERMES_BUFFER_POOL_INTERNAL_H_
