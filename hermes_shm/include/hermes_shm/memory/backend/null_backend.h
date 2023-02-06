//
// Created by lukemartinlogan on 1/10/23.
//

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_MEMORY_BACKEND_NULL_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_MEMORY_BACKEND_NULL_H_

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

class NullBackend : public MemoryBackend {
 private:
  size_t total_size_;

 public:
  NullBackend() = default;

  ~NullBackend() override {}

  bool shm_init(size_t size, const std::string &url) {
    (void) url;
    SetInitialized();
    Own();
    total_size_ = sizeof(MemoryBackendHeader) + size;
    char *ptr = (char*)malloc(sizeof(MemoryBackendHeader));
    header_ = reinterpret_cast<MemoryBackendHeader*>(ptr);
    header_->data_size_ = size;
    data_size_ = size;
    data_ = nullptr;
    return true;
  }

  bool shm_deserialize(std::string url) override {
    (void) url;
    throw SHMEM_NOT_SUPPORTED.format();
  }

  void shm_detach() override {
    _Detach();
  }

  void shm_destroy() override {
    _Destroy();
  }

 protected:
  void _Detach() {
    free(header_);
  }

  void _Destroy() {
    free(header_);
  }
};

}  // namespace hermes_shm::ipc

#endif //HERMES_SHM_INCLUDE_HERMES_SHM_MEMORY_BACKEND_NULL_H_
