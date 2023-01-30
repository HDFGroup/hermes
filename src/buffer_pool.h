//
// Created by lukemartinlogan on 12/19/22.
//

#ifndef HERMES_SRC_BUFFER_POOL_H_
#define HERMES_SRC_BUFFER_POOL_H_

#include "hermes_types.h"
#include "rpc.h"

namespace hermes {

class MetadataManager;
class BufferOrganizer;

struct BufferPoolAllocator {
  std::atomic<size_t> max_size_;
  std::atomic<size_t> cur_off_;
};

/**
 * The shared-memory representation of the BufferPool
 * */
struct BufferPoolShmHeader {
  lipc::TypedPointer<lipc::vector<BufferPoolAllocator>> alloc_ar_;
};

/**
 * Responsible for managing the buffering space of all node-local targets.
 * */
class BufferPool {
 private:
  MetadataManager *mdm_;
  BufferOrganizer *borg_;
  /** Per-target allocator */
  lipc::mptr<lipc::vector<BufferPoolAllocator>> target_allocs_;

 public:
  BufferPool() = default;

  /**
   * Initialize the BPM and its shared memory.
   * REQUIRES mdm to be initialized already.
   * */
  void shm_init(BufferPoolShmHeader *header);

  /** Destroy the BPM shared memory. */
  void shm_destroy();

  /** Store the BPM in shared memory */
  void shm_serialize(BufferPoolShmHeader *header);

  /** Deserialize the BPM from shared memory */
  void shm_deserialize(BufferPoolShmHeader *header);

  /**
   * Allocate buffers from the targets according to the schema
   * */
  RPC lipc::vector<BufferInfo>
  LocalAllocateAndSetBuffers(PlacementSchema &schema, const Blob &blob);

  /**
   * Free buffers from the BufferPool
   * */
  RPC bool LocalReleaseBuffers(lipc::vector<BufferInfo> &buffers);
};

}  // namespace hermes

#endif  // HERMES_SRC_BUFFER_POOL_H_
