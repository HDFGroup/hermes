/*
 * Copyright (C) 2022  SCS Lab <scslab@iit.edu>,
 * Luke Logan <llogan@hawk.iit.edu>,
 * Jaime Cernuda Garcia <jcernudagarcia@hawk.iit.edu>
 * Jay Lofstead <gflofst@sandia.gov>,
 * Anthony Kougkas <akougkas@iit.edu>,
 * Xian-He Sun <sun@iit.edu>
 *
 * This file is part of HermesShm
 *
 * HermesShm is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

#ifndef HERMES_SHM_INCLUDE_MEMORY_BACKEND_POSIX_SHM_MMAP_H
#define HERMES_SHM_INCLUDE_MEMORY_BACKEND_POSIX_SHM_MMAP_H

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

class PosixShmMmap : public MemoryBackend {
 private:
  std::string url_;
  int fd_;

 public:
  /** Constructor */
  PosixShmMmap() : fd_(-1) {}

  /** Destructor */
  ~PosixShmMmap() override {
    if (IsOwned()) {
      _Destroy();
    } else {
      _Detach();
    }
  }

  /** Initialize backend */
  bool shm_init(size_t size, std::string url) {
    SetInitialized();
    Own();
    url_ = std::move(url);
    shm_unlink(url_.c_str());
    fd_ = shm_open(url_.c_str(), O_CREAT | O_RDWR, 0666);
    if (fd_ < 0) {
      return false;
    }
    _Reserve(size);
    header_ = _Map<MemoryBackendHeader>(HERMES_SHM_SYSTEM_INFO->page_size_, 0);
    header_->data_size_ = size;
    data_size_ = size;
    data_ = _Map(size, HERMES_SHM_SYSTEM_INFO->page_size_);
    return true;
  }

  /** Deserialize the backend */
  bool shm_deserialize(std::string url) override {
    SetInitialized();
    Disown();
    url_ = std::move(url);
    fd_ = shm_open(url_.c_str(), O_RDWR, 0666);
    if (fd_ < 0) {
      return false;
    }
    header_ = _Map<MemoryBackendHeader>(HERMES_SHM_SYSTEM_INFO->page_size_, 0);
    data_size_ = header_->data_size_;
    data_ = _Map(data_size_, HERMES_SHM_SYSTEM_INFO->page_size_);
    return true;
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
  /** Reserve shared memory */
  void _Reserve(size_t size) {
    int ret = ftruncate64(fd_, static_cast<off64_t>(size));
    if (ret < 0) {
      throw SHMEM_RESERVE_FAILED.format();
    }
  }

  /** Map shared memory */
  template<typename T=char>
  T* _Map(size_t size, off64_t off) {
    T *ptr = reinterpret_cast<T*>(
      mmap64(nullptr, size, PROT_READ | PROT_WRITE,
             MAP_SHARED, fd_, off));
    if (ptr == MAP_FAILED) {
      throw SHMEM_CREATE_FAILED.format();
    }
    return ptr;
  }

  /** Unmap shared memory */
  void _Detach() {
    if (!IsInitialized()) { return; }
    munmap(data_, data_size_);
    munmap(header_, HERMES_SHM_SYSTEM_INFO->page_size_);
    close(fd_);
    UnsetInitialized();
  }

  /** Destroy shared memory */
  void _Destroy() {
    if (!IsInitialized()) { return; }
    _Detach();
    shm_unlink(url_.c_str());
    UnsetInitialized();
  }
};

}  // namespace hermes_shm::ipc

#endif  // HERMES_SHM_INCLUDE_MEMORY_BACKEND_POSIX_SHM_MMAP_H
