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

#ifndef HERMES_SRC_BUFFER_ORGANIZER_H_
#define HERMES_SRC_BUFFER_ORGANIZER_H_

#include "rpc.h"
#include "hermes_types.h"
#include "buffer_pool.h"

namespace hermes {

/** Calculates the total size of a blob's buffers */
static inline size_t SumBufferBlobSizes(hipc::vector<BufferInfo> &buffers) {
  size_t sum = 0;
  for (hipc::ShmRef<BufferInfo> buffer_ref : buffers) {
    sum += (*buffer_ref).blob_size_;
  }
  return sum;
}

/**
 * Manages the organization of blobs in the hierarchy.
 * */
class BufferOrganizer {
 public:
  MetadataManager *mdm_;
  RPC_TYPE *rpc_;

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
  RPC void LocalPlaceBlobInBuffers(const Blob &blob,
                                   hipc::vector<BufferInfo> &buffers);
  RPC void GlobalPlaceBlobInBuffers(const Blob &blob,
                                    hipc::vector<BufferInfo> &buffers);

  /** Stores a blob into a set of buffers */
  RPC Blob LocalReadBlobFromBuffers(hipc::vector<BufferInfo> &buffers);
  Blob GlobalReadBlobFromBuffers(hipc::vector<BufferInfo> &buffers);

  /** Copies one buffer set into another buffer set */
  RPC void LocalCopyBuffers(hipc::vector<BufferInfo> &dst,
                            hipc::vector<BufferInfo> &src);
};

}  // namespace hermes

#endif  // HERMES_SRC_BUFFER_ORGANIZER_H_
