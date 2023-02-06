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

#ifndef HERMES_SRC_BUFFER_POOL_H_
#define HERMES_SRC_BUFFER_POOL_H_

#include "hermes_types.h"
#include "rpc.h"

namespace hermes {

class MetadataManager;
class BufferOrganizer;

struct BufferPoolAllocator {
  hipc::atomic<size_t> max_size_;
  hipc::atomic<size_t> cur_off_;

  /** Default constructor */
  BufferPoolAllocator() = default;

  /** Copy Constructor */
  BufferPoolAllocator(const BufferPoolAllocator &other)
  : max_size_(other.max_size_.load()), cur_off_(other.cur_off_.load()) {}

  /** Move Constructor */
  BufferPoolAllocator(BufferPoolAllocator &&other)
  : max_size_(other.max_size_.load()), cur_off_(other.cur_off_.load()) {}
};

/**
 * The shared-memory representation of the BufferPool
 * */
struct BufferPoolShmHeader {
  hipc::TypedPointer<hipc::vector<BufferPoolAllocator>> alloc_ar_;
};

/**
 * Responsible for managing the buffering space of all node-local targets.
 * */
class BufferPool {
 private:
  MetadataManager *mdm_;
  BufferOrganizer *borg_;
  /** Per-target allocator */
  hipc::mptr<hipc::vector<BufferPoolAllocator>> target_allocs_;

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
  RPC hipc::vector<BufferInfo>
  LocalAllocateAndSetBuffers(PlacementSchema &schema,
                             const Blob &blob);

  /**
   * Free buffers from the BufferPool
   * */
  RPC bool LocalReleaseBuffers(hipc::vector<BufferInfo> &buffers);
};

}  // namespace hermes

#endif  // HERMES_SRC_BUFFER_POOL_H_
