//
// Created by lukemartinlogan on 12/19/22.
//

#ifndef HERMES_SRC_BUFFER_POOL_H_
#define HERMES_SRC_BUFFER_POOL_H_

#include "hermes_types.h"
#include "rpc.h"

class MetadataManager;

namespace hermes {

struct BufferPoolAllocator {
  std::atomic<size_t> max_size_;
  std::atomic<size_t> cur_off_;
};

/**
 * The shared-memory representation of the BufferPool
 * */
struct BufferPoolManagerShmHeader {
  lipc::ShmArchive<lipc::vector<BufferPoolAllocator>> alloc_ar_;
};

/**
 * Responsible for managing the buffering space of all node-local targets.
 * */
class BufferPoolManager {
 private:
  MetadataManager *mdm_;
  lipc::vector<BufferPoolAllocator> target_allocs_;  /**< Per-target allocator */

 public:
  BufferPoolManager() = default;

  /** Initialize the BPM and its shared memory. */
  void shm_init(MetadataManager *mdm);

  /** Store the BPM in shared memory */
  void shm_serialize(BufferPoolManagerShmHeader *header);

  /** Deserialize the BPM from shared memory */
  void shm_deserialize(BufferPoolManagerShmHeader *header);

  /**
   * Allocate buffers from the targets according to the schema
   * */
  RPC lipc::vector<BufferInfo>
  LocalAllocateAndSetBuffers(PlacementSchema &schema, Blob &blob);

  /**
   * Free buffers from the BufferPool
   * */
  RPC bool LocalReleaseBuffers(lipc::vector<BufferInfo> &buffers);
};

}  // namespace hermes

#endif  // HERMES_SRC_BUFFER_POOL_H_
