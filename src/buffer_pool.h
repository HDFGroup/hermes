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
#include "memory_arena.h"
#include "communication.h"

/** @file buffer_pool.h
 *
 * The public structures and functions for interacting with the BufferPool as a
 * client. The interface includes hermes-specific application core
 * intialization, and the API through which the DataPlacementEngine and
 * BufferOrganizer interact with the BufferPool.
 */

namespace hermes {

/**
 * Implements a ticket lock as described at
 * https://en.wikipedia.org/wiki/Ticket_lock.
 */
struct TicketMutex {
  std::atomic<u32> ticket;
  std::atomic<u32> serving;
};

/**
 * System and user configuration that is used to initialize the BufferPool.
 */
struct Config {
  /** The total capacity of each buffering Tier */
  size_t capacities[kMaxTiers];
  /** The block sizes of each Tier */
  int block_sizes[kMaxTiers];
  /** The number of slabs that each Tier has */
  int num_slabs[kMaxTiers];
  /** The unit of each slab, which is a multiplier of the Tier's block size */
  int slab_unit_sizes[kMaxTiers][kMaxBufferPoolSlabs];
  /** The percentage of space each slab should occupy per Tier. The values for
   * each Tier should add up to 1.0.
   */
  f32 desired_slab_percentages[kMaxTiers][kMaxBufferPoolSlabs];
  /** The bandwidth of each Tier */
  f32 bandwidths[kMaxTiers];
  /** The latency of each Tier */
  f32 latencies[kMaxTiers];
  /** The percentage of the total available Hermes memory that is allotted for
   * RAM buffering.
   */
  f32 buffer_pool_memory_percent;
  /** The percentage of the total available Hermes memory that is allotted for
   * metadata.
   */
  f32 metadata_memory_percent;
  /** The percentage of the total available Hermes memory that is allotted as
   * scratch space for transferring data among Tiers.
   */
  f32 transfer_window_memory_percent;
  /** The percentage of the total available Hermes memory that is allotted for
   * transient storage.
   */
  f32 transient_memory_percent;
  /** The number of Tiers */
  int num_tiers;
  /** The mount point or desired directory for each Tier. RAM Tier should be the
   * empty string.
   */
  const char *mount_points[kMaxTiers];
  /** The IP address and port number of the BufferPool RPC server in a format
   * that Thallium understands. For example, tcp://172.20.101.25:8080.
   */
  const char *rpc_server_name;
  /** A base name for the BufferPool shared memory segement. Hermes appends the
   * value of the USER environment variable to this string.
   */
  char buffer_pool_shmem_name[kMaxBufferPoolShmemNameLength];
};

/**
 * Information about a specific hardware Tier.
 *
 * This could represent local RAM, remote RAM, NVMe, burst buffers, a parallel
 * file system, etc. The Tiers are initialized when the BufferPool is
 * initialized, and they remain throughout a Hermes run.
 */
struct Tier {
  /** The total capacity of the Tier. */
  u64 capacity;
  /** The theoretical (or perceived by Apollo?) bandwidth in MiB/second. */
  f32 bandwidth_mbps;
  /** The theoretical (or perceived by Apollo?) latency in nanoseconds. */
  f32 latency_ns;
  /** The Tier's identifier. This is an index into the array of Tiers stored in
   * the BufferPool.
   */
  TierID id;
  /** True if the Tier is a RAM Tier (or other byte addressable, local or
   * remote)
   */
  bool is_ram;
  /** True if the Tier represents a remote resource. (e.g., remote RAM or
   * NVMe).
   */
  bool is_remote;
  /** True if the functionality of `posix_fallocate` is available on this
   * Tier
   */
  bool has_fallocate;
  /** The directory where buffering files can be created. */
  char mount_point[kMaxPathLength];
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
 * For a RAM Tier, one BufferHeader is created for each block. This is to
 * facilitate splitting and merging. For non-RAM Tiers, we only need one
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
  /** An index into the array of Tiers in the BufferPool that represents this
   * buffer's Tier.
   */
  TierID tier_id;
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
 * Tiers in shared memory.
 *
 * Some terminology:
 *   block - A contiguous range of buffer space of the smallest unit for a given
 *           Tier. The size for each Tier is specified by
 *           Config::block_sizes[tier_id]. RAM is typically 4K, for example.
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
  ptrdiff_t header_storage_offset;
  /** The offset from the base of shared memory where the Tier array begins.
   */
  ptrdiff_t tier_storage_offset;
  /** The offset from the base of shared memory where each Tier's free list is
   * stored. Converting the offset to a pointer results in a pointer to an array
   * of N BufferIDs where N is the number of slabs in that Tier.
   */
  ptrdiff_t free_list_offsets[kMaxTiers];
  /** The offset from the base of shared memory where each Tier's list of slab
   * unit sizes is stored. Each offset can be converted to a pointer to an array
   * of N ints where N is the number of slabs in that Tier. Each slab has its
   * own unit size x, where x is the number of blocks that make up a buffer.
   */
  ptrdiff_t slab_unit_sizes_offsets[kMaxTiers];
  /** The offset from the base of shared memory where each Tier's list of slab
   * buffer sizes is stored. Each offset can be converted to a pointer to an
   * array of N ints where N is the number of slabs in that Tier. A slab's
   * buffer size (in bytes) is the slab's unit size multiplied by the Tier's
   * block size.
   */
  ptrdiff_t slab_buffer_sizes_offsets[kMaxTiers];
  /** A ticket lock to syncrhonize access to free lists
   * TODO(chogan): @optimization One mutex per free list.
   */
  TicketMutex ticket_mutex;
  /** The block size for each Tier. */
  i32 block_sizes[kMaxTiers];
  /** The number of slabs for each Tier. */
  i32 num_slabs[kMaxTiers];
  /* The number of BufferHeaders for each Tier. */
  u32 num_headers[kMaxTiers];
  /* The total number of Tiers. */
  i32 num_tiers;
  /* The total number of BufferHeaders in the header array. */
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
  /** The total size of the shared memory (needed for munmap). */
  u64 shm_size;

  // TEMP(chogan): These may get moved. Rather than this struct being
  // specifically for shared memory info, maybe it should represent any
  // information that is unique to each rank. Maybe CoreContext.
  std::vector<std::vector<std::string>> buffering_filenames;
  FILE *open_streams[kMaxTiers][kMaxBufferPoolSlabs];
  CommunicationAPI comm_api;
  CommunicationState comm_state;
};

/**
 *
 */
size_t GetBlobSize(SharedMemoryContext *context,
                   const std::vector<BufferID> &buffer_ids);

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
 * slab for each Tier. If `posix_fallocate` is available, each file's capacity
 * is reserved. Otherwise, the files will be initialized with 0 size.
 *
 * @param context The SharedMemoryContext in which to store the opened FILE
 * pointers.
 */
void InitFilesForBuffering(SharedMemoryContext *context);

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
 * Returns a vector of BufferIDs that satisfy the constrains of @p schema.
 *
 * If a request cannot be fulfilled, an empty list is returned. GetBuffers will
 * never partially satisfy a request. It is all or nothing. If @p schema
 * includes a remote Tier, this function will make an RPC call to get BufferIDs
 * from a remote node.
 *
 * @param context The shared memory context for the BufferPool.
 * @param schema A description of the amount and Tier of storage requested.
 *
 * @return A vector of BufferIDs that can be used for storage, and that satisfy
 * @p schema, or an empty vector if the request could not be fulfilled.
 */
std::vector<BufferID> GetBuffers(SharedMemoryContext *context,
                                 const TieredSchema &schema);
/**
 * Returns buffer_ids to the BufferPool free lists so that they can be used
 * again. Data in the buffers is considered abandonded, and can be overwritten.
 *
 * @param context The shared memory context where the BufferPool lives.
 * @param buffer_ids The list of buffer_ids to return to the BufferPool.
 */
void ReleaseBuffers(SharedMemoryContext *context,
                    const std::vector<BufferID> &buffer_ids);
/**
 * Starts an RPC server that will listen for remote requests directed to the
 * BufferPool.
 *
 * @param context The shared memory context where the BufferPool lives.
 * @param addr The address and port where the RPC server will listen.
 * Currently, this is a Thallium compatible address. For example,
 * tcp://127.1.1.1:8080
 */
void StartBufferPoolRpcServer(SharedMemoryContext *context, const char *addr,
                              i32 num_rpc_threads);

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
 * Tier) or to a file (block addressable Tier).
 *
 * @param context The shared memory context needed to access BufferPool info.
 * @param blob The data to write.
 * @param buffer_ids The collection of BufferIDs that should buffer the blob.
 */
void WriteBlobToBuffers(SharedMemoryContext *context, const Blob &blob,
                        const std::vector<BufferID> &buffer_ids);
/**
 * Sketch of how an I/O client might read.
 *
 * Reads the collection of buffer_ids into blob. The BufferIDs inform the
 * call whether it is reading locally, remotely, from RAM (or a byte addressable
 * Tier) or to a file (block addressable Tier).
 *
 * @param context The shared memory context needed to access BufferPool info.
 * @param blob A place to store the read data.
 * @param buffer_ids The collection of BufferIDs that hold the buffered blob.
 *
 * @return The total number of bytes read
 */
size_t ReadBlobFromBuffers(SharedMemoryContext *context, Blob *blob,
                           const std::vector<BufferID> &buffer_ids);

}  // namespace hermes

#endif  // HERMES_BUFFER_POOL_H_
