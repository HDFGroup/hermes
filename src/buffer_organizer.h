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
static inline size_t SumBufferBlobSizes(std::vector<BufferInfo> &buffers) {
  size_t sum = 0;
  for (BufferInfo &buffer_ref : buffers) {
    sum += buffer_ref.blob_size_;
  }
  return sum;
}

/**
 * Any state needed by BORG in SHM
 * */
template<>
struct ShmHeader<BufferOrganizer> {
};

/**
 * Manages the organization of blobs in the hierarchy.
 * */
class BufferOrganizer : public hipc::ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE(BufferOrganizer,
                         BufferOrganizer,
                         ShmHeader<BufferOrganizer>)
  MetadataManager *mdm_;
  RPC_TYPE *rpc_;

 public:
  /**====================================
   * Default Constructor
   * ===================================*/

  /**
   * Initialize the BORG
   * REQUIRES mdm to be initialized already.
   * */
  explicit BufferOrganizer(ShmHeader<BufferOrganizer> *header,
                           hipc::Allocator *alloc);

  /**====================================
   * SHM Deserialization
   * ===================================*/

  /** Deserialize the BORG from shared memory */
  void shm_deserialize_main();

  /**====================================
   * Destructors
   * ===================================*/

  /** Check if BORG is NULL */
  bool IsNull() const {
    return false;
  }

  /** Set BORG to NULL */
  void SetNull() {}

  /** Finalize the BORG */
  void shm_destroy_main();

  /**====================================
   * BORG Methods
   * ===================================*/

  /** Stores a blob into a set of buffers */
  RPC void LocalPlaceBlobInBuffers(const Blob &blob,
                                   std::vector<BufferInfo> &buffers);
  RPC void GlobalPlaceBlobInBuffers(const Blob &blob,
                                    std::vector<BufferInfo> &buffers);

  /** Stores a blob into a set of buffers */
  RPC Blob LocalReadBlobFromBuffers(std::vector<BufferInfo> &buffers);
  Blob GlobalReadBlobFromBuffers(std::vector<BufferInfo> &buffers);

  /** Copies one buffer set into another buffer set */
  RPC void LocalCopyBuffers(std::vector<BufferInfo> &dst,
                            std::vector<BufferInfo> &src);
};

}  // namespace hermes

#endif  // HERMES_SRC_BUFFER_ORGANIZER_H_
