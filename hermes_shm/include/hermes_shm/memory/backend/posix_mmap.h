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

#ifndef HERMES_INCLUDE_MEMORY_BACKEND_POSIX_MMAP_H
#define HERMES_INCLUDE_MEMORY_BACKEND_POSIX_MMAP_H

#include "memory_backend.h"
#include <string>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>

#include <hermes_shm/util/errors.h>
#include <hermes_shm/constants/macros.h>
#include <hermes_shm/introspect/system_info.h>

namespace hermes_shm::ipc {

class PosixMmap : public MemoryBackend {
 private:
  size_t total_size_;

 public:
  /** Constructor */
  PosixMmap() = default;

  /** Destructor */
  ~PosixMmap() override {
    if (IsOwned()) {
      _Destroy();
    } else {
      _Detach();
    }
  }

  /** Initialize backend */
  bool shm_init(size_t size) {
    SetInitialized();
    Own();
    total_size_ = sizeof(MemoryBackendHeader) + size;
    char *ptr = _Map(total_size_);
    header_ = reinterpret_cast<MemoryBackendHeader*>(ptr);
    header_->data_size_ = size;
    data_size_ = size;
    data_ = reinterpret_cast<char*>(header_ + 1);
    return true;
  }

  /** Deserialize the backend */
  bool shm_deserialize(std::string url) override {
    (void) url;
    throw SHMEM_NOT_SUPPORTED.format();
  }

  /** Detach the mapped memory */
  void shm_detach() override {
    _Detach();
  }

  /** Destroy the mapped memory */
  void shm_destroy() override {
    _Destroy();
  }

 protected:
  /** Map shared memory */
  template<typename T=char>
  T* _Map(size_t size) {
    T *ptr = reinterpret_cast<T*>(
      mmap64(nullptr, NextPageSizeMultiple(size), PROT_READ | PROT_WRITE,
             MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    if (ptr == MAP_FAILED) {
      perror("map failed");
      throw SHMEM_CREATE_FAILED.format();
    }
    return ptr;
  }

  /** Unmap shared memory */
  void _Detach() {
    if (!IsInitialized()) { return; }
    munmap(reinterpret_cast<void*>(header_), total_size_);
    UnsetInitialized();
  }

  /** Destroy shared memory */
  void _Destroy() {
    if (!IsInitialized()) { return; }
    _Detach();
    UnsetInitialized();
  }
};

}  // namespace hermes_shm::ipc

#endif  // HERMES_INCLUDE_MEMORY_BACKEND_POSIX_MMAP_H
