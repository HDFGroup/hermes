//
// Created by lukemartinlogan on 12/19/22.
//

#ifndef HERMES_SRC_BUFFER_ORGANIZER_H_
#define HERMES_SRC_BUFFER_ORGANIZER_H_

#include "rpc.h"
#include "hermes_types.h"
#include "buffer_pool.h"

namespace hermes {

/**
 * Manages the organization of blobs in the hierarchy.
 * */
class BufferOrganizer {
 public:
  MetadataManager *mdm_;

 public:
  BufferOrganizer() = default;

  /**
   * Initialize the BORG
   * REQUIRES mdm to be initialized already.
   * */
  void shm_init();

  /** Finalize the BORG */
  void shm_destroy();

  /** Serialize the BORG into shared memory */
  void shm_serialize();

  /** Deserialize the BORG from shared memory */
  void shm_deserialize();

  /** Stores a blob into a set of buffers */
  RPC void LocalPlaceBlobInBuffers(Blob &blob,
                                   lipc::vector<BufferInfo> &buffers);

  /** Stores a blob into a set of buffers */
  RPC Blob LocalReadBlobFromBuffers(lipc::vector<BufferInfo> &buffers);

  /** Copies one buffer set into another buffer set */
  RPC void LocalCopyBuffers(lipc::vector<BufferInfo> &dst,
                            lipc::vector<BufferInfo> &src);
};

}  // namespace hermes

#endif  // HERMES_SRC_BUFFER_ORGANIZER_H_
