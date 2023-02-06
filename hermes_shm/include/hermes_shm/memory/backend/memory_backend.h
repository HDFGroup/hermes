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

#ifndef HERMES_SHM_MEMORY_H
#define HERMES_SHM_MEMORY_H

#include <cstdint>
#include <vector>
#include <string>
#include <hermes_shm/memory/memory.h>
#include "hermes_shm/constants/macros.h"
#include <limits>

namespace hermes_shm::ipc {

struct MemoryBackendHeader {
  size_t data_size_;
};

enum class MemoryBackendType {
  kPosixShmMmap,
  kNullBackend,
  kArrayBackend,
  kPosixMmap,
};

#define MEMORY_BACKEND_INITIALIZED 0x1
#define MEMORY_BACKEND_OWNED 0x2

class MemoryBackend {
 public:
  MemoryBackendHeader *header_;
  char *data_;
  size_t data_size_;
  bitfield32_t flags_;

 public:
  MemoryBackend() = default;

  virtual ~MemoryBackend() = default;

  /** Mark data as valid */
  void SetInitialized() {
    flags_.SetBits(MEMORY_BACKEND_INITIALIZED);
  }

  /** Check if data is valid */
  bool IsInitialized() {
    return flags_.OrBits(MEMORY_BACKEND_INITIALIZED);
  }

  /** Mark data as invalid */
  void UnsetInitialized() {
    flags_.UnsetBits(MEMORY_BACKEND_INITIALIZED);
  }

  /** This is the process which destroys the backend */
  void Own() {
    flags_.SetBits(MEMORY_BACKEND_OWNED);
  }

  /** This is owned */
  bool IsOwned() {
    return flags_.OrBits(MEMORY_BACKEND_OWNED);
  }

  /** This is not the process which destroys the backend */
  void Disown() {
    flags_.UnsetBits(MEMORY_BACKEND_OWNED);
  }

  /// Each allocator must define its own shm_init.
  // virtual bool shm_init(size_t size, ...) = 0;
  virtual bool shm_deserialize(std::string url) = 0;
  virtual void shm_detach() = 0;
  virtual void shm_destroy() = 0;
};

}  // namespace hermes_shm::ipc

#endif  // HERMES_SHM_MEMORY_H
