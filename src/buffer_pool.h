#ifndef HERMES_BUFFER_POOL_H_
#define HERMES_BUFFER_POOL_H_

#include <assert.h>
#include <stddef.h>
#include <stdio.h>

#include <atomic>
#include <string>
#include <utility>
#include <vector>

#include "hermes_types.h"
#include "memory_management.h"
#include "communication.h"

/** @file buffer_pool.h
 *
 * The public structures and functions for interacting with the BufferPool as a
 * client. The interface includes hermes-specific application core
 * intialization, and the API through which the DataPlacementEngine and
 * BufferOrganizer interact with the BufferPool.
 */

namespace hermes {

struct RpcContext;

/**
 * Information about a specific hardware Device.
 *
 * This could represent local RAM, remote RAM, NVMe, burst buffers, a parallel
 * file system, etc. The Devices are initialized when the BufferPool is
 * initialized, and they remain throughout a Hermes run.
 */
struct Device {
  /** The device's theoretical bandwidth in MiB/second. */
  f32 bandwidth_mbps;
  /** The devices's theoretical latency in nanoseconds. */
  f32 latency_ns;
  /** The Device's identifier. This is an index into the array of Devices stored in
   * the BufferPool.
   */
  DeviceID id;
  /** True if the Device is a RAM Device (or other byte addressable, local or
   * remote)
   */
  bool is_byte_addressable;
  /** True if the functionality of `posix_fallocate` is available on this
   * Device
   */
  bool has_fallocate;
  /** The directory where buffering files can be created. Zero terminated. */
  char mount_point[kMaxPathLength];
};

union TargetID {
  struct {
    u32 node_id;
    u16 device_id;
    u16 index;
  } bits;

  u64 as_int;
};

struct Target {
  TargetID id;
  /** The total capacity of the Target. */
  u64 capacity;
  std::atomic<u64> remaining_space;
  std::atomic<u64> speed;
};

/**
 * A unique identifier for any buffer in the system.
 *
 * This number is unique for each buffer when read as a 64 bit integer. The top
 * 32 bits signify the node that "owns" the buffer, and the bottom 32 bits form
 * in index into the array of BufferHeaders in the BufferPool on that node. Node
 * indexing begins at 1 so that a BufferID of 0 represents the NULL BufferID.
 */
union BufferID {
  struct {
    /** An index into the BufferHeaders array in the BufferPool on node
     * `node_id`.
     */
    u32 header_index;
    /** The node identifier where this BufferID's BufferHeader resides
     * (1-based index).
     */
    u32 node_id;
  } bits;

  /** A single integer that uniquely identifies a buffer. */
  u64 as_int;
};

/**
 * Metadata for a Hermes buffer.
 *
 * An array of BufferHeaders is initialized during BufferPool initialization.
 * For a RAM Device, one BufferHeader is created for each block. This is to
 * facilitate splitting and merging. For non-RAM Devices, we only need one
 * BufferHeader per buffer. A typical workflow is to retrieve a BufferHeader
 * from a BufferID using the GetHeaderByBufferId function.
 */
struct BufferHeader {
  /** The unique identifier for this buffer. */
  BufferID id;
  /** The next free BufferID on the free list. */
  BufferID next_free;
  /** The offset (from the beginning of shared memory or a file) of the actual
   * buffered data.
   */
  ptrdiff_t data_offset;
  /** The number of bytes this buffer is actually using. */
  u32 used;
  /** The total capacity of this buffer. */
  u32 capacity;
  /** An index into the array of Devices in the BufferPool that represents this
   * buffer's Device.
   */
  DeviceID device_id;
  /** True if this buffer is being used by Hermes to buffer data, false if it is
   * free.
   */
  bool in_use;
  /** A simple lock that is atomically set to true when the data in this buffer
   * is being read or written by an I/O client or the BufferOrganizer.
   */
  std::atomic<bool> locked;
};

/**
 * Contains information about the layout of the buffers, BufferHeaders, and
 * Devices in shared memory.
 *
 * Some terminology:
 *   block - A contiguous range of buffer space of the smallest unit for a given
 *           Device. The size for each Device is specified by
 *           Config::block_sizes[device_id]. RAM is typically 4K, for example.
 *   buffer - Made up of 1 or more blocks: 1-block buffer, 4-block buffer, etc.
 *   slab - The collection of all buffers of a particular size. e.g., the
 *          4-block slab is made up of all the 4-block buffers.
 *
 * When Hermes initializes, the BufferPool is created and initialized in a
 * shared memory segment, which is then closed when Hermes shuts down. A pointer
 * to the BufferPool can be retrieved with the function
 * GetBufferPoolFromContext. All pointer values are stored as offsets from the
 * beginning of shared memory. The layout of the BufferPool is determined by the
 * Config struct that is passed to InitBufferPool. Multiple BufferPools can be
 * instantiated, but each must have a different buffer_pool_shmem_name, which is
 * a member of Config. Each BufferPool will then exist in its own shared memory
 * segment.
 */
struct BufferPool {
  /** The offset from the base of shared memory where the BufferHeader array
   * begins.
   */
  ptrdiff_t headers_offset;
  /** The offset from the base of shared memory where the Device array begins.
   */
  ptrdiff_t devices_offset;
  ptrdiff_t targets_offset;
  /** The offset from the base of shared memory where each Device's free list is
   * stored. Converting the offset to a pointer results in a pointer to an array
   * of N BufferIDs where N is the number of slabs in that Device.
   */
  ptrdiff_t free_list_offsets[kMaxDevices];
  /** The offset from the base of shared memory where each Device's list of slab
   * unit sizes is stored. Each offset can be converted to a pointer to an array
   * of N ints where N is the number of slabs in that Device. Each slab has its
   * own unit size x, where x is the number of blocks that make up a buffer.
   */
  ptrdiff_t slab_unit_sizes_offsets[kMaxDevices];
  /** The offset from the base of shared memory where each Device's list of slab
   * buffer sizes is stored. Each offset can be converted to a pointer to an
   * array of N ints where N is the number of slabs in that Device. A slab's
   * buffer size (in bytes) is the slab's unit size multiplied by the Device's
   * block size.
   */
  ptrdiff_t slab_buffer_sizes_offsets[kMaxDevices];
  /** The offset from the base of shared memory where each Device's list of
   * available buffers per slab is stored. Each offset can be converted to a
   * pointer to an arry of N (num_slabs[device_id]) std::atomic<u32>.
   */
  ptrdiff_t buffers_available_offsets[kMaxDevices];
  /** A ticket lock to syncrhonize access to free lists
   * TODO(chogan): @optimization One mutex per free list.
   */
  TicketMutex ticket_mutex;

  std::atomic<i64> capacity_adjustments[kMaxDevices];

  /** The block size for each Device. */
  i32 block_sizes[kMaxDevices];
  /** The number of slabs for each Device. */
  i32 num_slabs[kMaxDevices];
  /** The number of BufferHeaders for each Device. */
  u32 num_headers[kMaxDevices];
  /** The total number of Devices. */
  i32 num_devices;
  i32 num_targets;
  /** The total number of BufferHeaders in the header array. */
  u32 total_headers;
};

/**
 * Information that allows each process to access the shared memory and
 * BufferPool information.
 *
 * A SharedMemoryContext is passed as a parameter to all functions that interact
 * with the BufferPool. This context allows each process to find the location of
 * the BufferPool within its virtual address space. Acquiring the context via
 * GetSharedMemoryContext will map the BufferPool's shared memory into the
 * calling processes address space. An application core will acquire this
 * context on initialization. At shutdown, application cores will call
 * ReleaseSharedMemoryContext to unmap the shared memory segemnt (although, this
 * happens automatically when the process exits). The Hermes core is responsible
 * for creating and destroying the shared memory segment.
 *
 * Example:
 * ```cpp
 * SharedMemoryContext *context = GetSharedMemoryContext("hermes_shmem_name");
 * BufferPool *pool = GetBufferPoolFromContext(context);
 * ```
 */
struct SharedMemoryContext {
  /** A pointer to the beginning of shared memory. */
  u8 *shm_base;
  /** The offset from the beginning of shared memory to the BufferPool. */
  ptrdiff_t buffer_pool_offset;
  /** The offset from the beginning of shared memory to the Metadata Arena. */
  ptrdiff_t metadata_manager_offset;
  /** The total size of the shared memory (needed for munmap). */
  u64 shm_size;

  // TODO(chogan): Move these into a FileBufferingContext
  std::vector<std::vector<std::string>> buffering_filenames;
  FILE *open_streams[kMaxDevices][kMaxBufferPoolSlabs];
};

struct BufferIdArray;

/**
 *
 */
size_t GetBlobSize(SharedMemoryContext *context, RpcContext *rpc,
                   BufferIdArray *buffer_ids);

/**
 * Constructs a unique (among users) shared memory name from a base name.
 *
 * Copies the base name into the dest buffer and appends the value of the USER
 * envioronment variable. The dest buffer should be large enough to hold the
 * base name and the value of USER.
 *
 * @param[out] dest A buffer for the full shared memory name.
 * @param[in] base The base shared memory name.
 */
void MakeFullShmemName(char *dest, const char *base);

/**
 * Creates and opens all files that will be used for buffering. Stores open FILE
 * pointers in the @p context. The file buffering paradaigm uses one file per
 * slab for each Device. If `posix_fallocate` is available, and `make_space` is
 * `true`, each file's capacity is reserved. Otherwise, the files will be
 * initialized with 0 size.
 *
 * @param context The SharedMemoryContext in which to store the opened FILE
 * pointers.
 * @param make_space If true, attempts to reserve space on the * filesystem for
 * each file.
 */
void InitFilesForBuffering(SharedMemoryContext *context, bool make_space);

/**
 * Retrieves information required for accessing the BufferPool shared memory.
 *
 * Maps an existing shared memory segment into the calling process's address
 * space. Application cores will call this function, and the Hermes core
 * initialization will have already created the shared memory segment.
 * Application cores can then use the context to access the BufferPool.
 *
 * @param shmem_name The name of the shared memory segment.
 *
 * @return The shared memory context required to access the BufferPool.
 */
SharedMemoryContext GetSharedMemoryContext(char *shmem_name);

/**
 * Unmaps the shared memory represented by context and closes any open file
 * descriptors
 *
 * This isn't strictly necessary, since the segment will be unmapped when the
 * process exits and the file descriptors will be closed, but is here for
 * completeness.
 *
 * @param context The shared memory to unmap.
 */
void ReleaseSharedMemoryContext(SharedMemoryContext *context);

/**
 *
 */
void UnmapSharedMemory(SharedMemoryContext *context);

/**
 * Returns a vector of BufferIDs that satisfy the constrains of @p schema.
 *
 * If a request cannot be fulfilled, an empty list is returned. GetBuffers will
 * never partially satisfy a request. It is all or nothing. If @p schema
 * includes a remote Device, this function will make an RPC call to get BufferIDs
 * from a remote node.
 *
 * @param context The shared memory context for the BufferPool.
 * @param schema A description of the amount and Device of storage requested.
 *
 * @return A vector of BufferIDs that can be used for storage, and that satisfy
 * @p schema, or an empty vector if the request could not be fulfilled.
 */
std::vector<BufferID> GetBuffers(SharedMemoryContext *context,
                                 const PlacementSchema &schema);
/**
 * Returns buffer_ids to the BufferPool free lists so that they can be used
 * again. Data in the buffers is considered abandonded, and can be overwritten.
 *
 * @param context The shared memory context where the BufferPool lives.
 * @param rpc The RPC context to enable a remote call if necessary.
 * @param buffer_ids The list of buffer_ids to return to the BufferPool.
 */
void ReleaseBuffers(SharedMemoryContext *context, RpcContext *rpc,
                    const std::vector<BufferID> &buffer_ids);
/**
 * Starts an RPC server that will listen for remote requests directed to the
 * BufferPool.
 *
 * @param context The shared memory context where the BufferPool lives.
 * @param addr The address and port where the RPC server will listen.
 * This address must be a compatible with whatever the RPC implementation is.
 */
void StartBufferPoolRpcServer(SharedMemoryContext *context, const char *addr,
                              i32 num_rpc_threads);

/**
 * Free all resources held by Hermes.
 *
 * @param context The Hermes instance's shared memory context.
 * @param comm The Hermes instance's communication context.
 * @param rpc The Hermes instance's RPC context.
 * @param shmem_name The name of the shared memory.
 * @param trans_arena The instance's transient arena.
 * @param is_application_core Whether or not this rank is an app rank.
 */
void Finalize(SharedMemoryContext *context, CommunicationContext *comm,
              RpcContext *rpc, const char *shmem_name, Arena *trans_arena,
              bool is_application_core, bool force_rpc_shutdown);

// I/O Clients

/**
 * Description of user data.
 */
struct Blob {
  /** The beginning of the data */
  u8 *data;
  /** The size of the data in bytes */
  u64 size;
};

/**
 * Sketch of how an I/O client might write.
 *
 * Writes the blob to the collection of buffer_ids. The BufferIDs inform the
 * call whether it is writing locally, remotely, to RAM (or a byte addressable
 * Device) or to a file (block addressable Device).
 *
 * @param context The shared memory context needed to access BufferPool info.
 * @param blob The data to write.
 * @param buffer_ids The collection of BufferIDs that should buffer the blob.
 */
void WriteBlobToBuffers(SharedMemoryContext *context, RpcContext *rpc,
                        const Blob &blob,
                        const std::vector<BufferID> &buffer_ids);

/**
 * Sketch of how an I/O client might read.
 *
 * Reads the collection of buffer_ids into blob. The BufferIDs inform the
 * call whether it is reading locally, remotely, from RAM (or a byte addressable
 * Device) or to a file (block addressable Device).
 *
 * @param context The shared memory context needed to access BufferPool info.
 * @param rpc The RPC context needed to make a remote call if necessary.
 * @param blob A place to store the read data.
 * @param buffer_ids The collection of BufferIDs that hold the buffered blob.
 * @param buffer_sizes A list of sizes that correspond to each buffer in
 *        @p buffer_ids. Its length is the length of @p buffer_ids
 *
 * @return The total number of bytes read
 */
size_t ReadBlobFromBuffers(SharedMemoryContext *context, RpcContext *rpc,
                           Blob *blob, BufferIdArray *buffer_ids,
                           u32 *buffer_sizes);

size_t LocalWriteBufferById(SharedMemoryContext *context, BufferID id,
                            const Blob &blob, size_t offset);
size_t LocalReadBufferById(SharedMemoryContext *context, BufferID id,
                           Blob *blob, size_t offset);

/**
 * Returns a vector of bandwidths in MiB per second.
 *
 * The returned list can be indexed by DeviceID to get the bandwidth for a
 * specific Device.
 *
 * @param context The shared memory context needed to access BufferPool info.
 *
 * @return The list of bandwidths, one for each Device, in MiB/sec.
 */
std::vector<f32> GetBandwidths(SharedMemoryContext *context);

u32 GetBufferSize(SharedMemoryContext *context, RpcContext *rpc, BufferID id);
bool BufferIsByteAddressable(SharedMemoryContext *context, BufferID id);

}  // namespace hermes

#endif  // HERMES_BUFFER_POOL_H_
