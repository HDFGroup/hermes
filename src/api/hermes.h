#ifndef HERMES_H_
#define HERMES_H_

#include <cstdint>
#include <string>
#include <set>
#include <iostream>

#include "glog/logging.h"

#include "id.h"

namespace hermes {

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef int8_t i8;
typedef int16_t i16;
typedef int32_t i32;
typedef int64_t i64;
typedef float f32;
typedef double f64;

typedef u16 TierID;

// TODO(chogan): These constants impose limits on the number of slabs, tiers,
// file path lengths, and shared memory name lengths, but eventually we should
// allow arbitrary sizes of each.
static constexpr int kMaxBufferPoolSlabs = 8;
constexpr int kMaxPathLength = 256;
constexpr int kMaxBufferPoolShmemNameLength = 64;
constexpr int kMaxTiers = 8;

/**
 * A TieredSchema is a vector of (size, tier) pairs where size is the number of
 * bytes to buffer and tier is the Tier ID where to buffer those bytes.
 */
using TieredSchema = std::vector<std::pair<size_t, TierID>>;

/**
 * Distinguishes whether the process (or rank) is part of the application cores
 * or the Hermes core(s).
 */
enum class ProcessKind {
  kApp,
  kHermes,

  kCount
};
}  // namespace hermes

#include "communication.h"

namespace hermes {

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

namespace api {

typedef int Status;

class Hermes {
 public:
  std::set<std::string> bucket_list_;
  std::set<std::string> vbucket_list_;
  hermes::SharedMemoryContext context_;

  /** if true will do more checks, warnings, expect slower code */
  const bool m_debug_mode_ = true;

  Hermes() {}

  Hermes(SharedMemoryContext context) : context_(context) {}

  void Display_bucket() {
    for (auto it = bucket_list_.begin(); it != bucket_list_.end(); ++it)
      std::cout << *it << '\t';
    std::cout << '\n';
  }

  void Display_vbucket() {
    for (auto it = vbucket_list_.begin(); it != vbucket_list_.end(); ++it)
      std::cout << *it << '\t';
    std::cout << '\n';
  }

  // MPI comms.
  // proxy/reference to Hermes core
};

class VBucket;

class Bucket;

typedef std::vector<unsigned char> Blob;

class Context {
};

struct TraitTag{};
typedef ID<TraitTag, int64_t, -1> THnd;

struct BucketTag{};
typedef ID<BucketTag, int64_t, -1> BHnd;

/** acquire a bucket with the default trait */
Bucket Acquire(const std::string &name, Context &ctx);

/** rename a bucket referred to by name only */
Status RenameBucket(const std::string &old_name,
                    const std::string &new_name,
                    Context &ctx);

/** transfer a blob between buckets */
Status TransferBlob(const Bucket &src_bkt,
                    const std::string &src_blob_name,
                    Bucket &dst_bkt,
                    const std::string &dst_blob_name,
                    Context &ctx);

}  // api namespace
}  // hermes namespace

#endif  // HERMES_H_
