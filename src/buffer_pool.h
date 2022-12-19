//
// Created by lukemartinlogan on 12/19/22.
//

#ifndef HERMES_SRC_BUFFER_POOL_H_
#define HERMES_SRC_BUFFER_POOL_H_

#include "hermes_types.h"
#include "rpc.h"

class MetadataManager;

namespace hermes {

/**
 * The shared-memory representation of the BufferPool
 * */
struct BufferPoolManagerShmHeader {
  lipc::vector<int> targets_;
};

/**
 * Responsible for managing the buffering space of all node-local targets.
 * */
class BufferPoolManager {
 private:
  MetadataManager *mdm_;

 public:
  BufferPoolManager() = default;

  void shm_init();

  void shm_serialize();

  void shm_deserialize();

  /**
   * Allocate buffers from the targets according to the schema
   * */
  RPC lipc::vector<BufferInfo>
  LocalAllocateBuffers(PlacementSchema &schema, Blob &blob);

  /**
   * Free buffers from the BufferPool
   * */
  RPC bool LocalReleaseBuffers(lipc::vector<BufferInfo> &buffers);
};

}  // namespace hermes

#endif  // HERMES_SRC_BUFFER_POOL_H_
