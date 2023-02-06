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


#ifndef HERMES_SHM_MEMORY_BACKEND_MEMORY_BACKEND_FACTORY_H_
#define HERMES_SHM_MEMORY_BACKEND_MEMORY_BACKEND_FACTORY_H_

#include "memory_backend.h"
#include "posix_mmap.h"
#include "posix_shm_mmap.h"
#include "null_backend.h"
#include "array_backend.h"

namespace hermes_shm::ipc {

class MemoryBackendFactory {
 public:
  /** Initialize a new backend */
  template<typename BackendT, typename ...Args>
  static std::unique_ptr<MemoryBackend> shm_init(
    size_t size, const std::string &url, Args ...args) {
    if constexpr(std::is_same_v<PosixShmMmap, BackendT>) {
      // PosixShmMmap
      auto backend = std::make_unique<PosixShmMmap>();
      backend->shm_init(size, url, std::forward<args>(args)...);
      return backend;
    } else if constexpr(std::is_same_v<PosixMmap, BackendT>) {
      // PosixMmap
      auto backend = std::make_unique<PosixMmap>();
      backend->shm_init(size, url, std::forward<args>(args)...);
      return backend;
    } else if constexpr(std::is_same_v<NullBackend, BackendT>) {
      // NullBackend
      auto backend = std::make_unique<NullBackend>();
      backend->shm_init(size, url, std::forward<args>(args)...);
      return backend;
    } else if constexpr(std::is_same_v<ArrayBackend, BackendT>) {
      // ArrayBackend
      auto backend = std::make_unique<ArrayBackend>();
      backend->shm_init(size, url, std::forward<args>(args)...);
      return backend;
    } else {
      throw MEMORY_BACKEND_NOT_FOUND.format();
    }
  }

  /** Deserialize an existing backend */
  static std::unique_ptr<MemoryBackend> shm_deserialize(
    MemoryBackendType type, const std::string &url) {
    switch (type) {
      // PosixShmMmap
      case MemoryBackendType::kPosixShmMmap: {
        auto backend = std::make_unique<PosixShmMmap>();
        if (!backend->shm_deserialize(url)) {
          throw MEMORY_BACKEND_NOT_FOUND.format();
        }
        return backend;
      }

      // PosixMmap
      case MemoryBackendType::kPosixMmap: {
        auto backend = std::make_unique<PosixMmap>();
        if (!backend->shm_deserialize(url)) {
          throw MEMORY_BACKEND_NOT_FOUND.format();
        }
        return backend;
      }

      // NullBackend
      case MemoryBackendType::kNullBackend: {
        auto backend = std::make_unique<NullBackend>();
        if (!backend->shm_deserialize(url)) {
          throw MEMORY_BACKEND_NOT_FOUND.format();
        }
        return backend;
      }

      // ArrayBackend
      case MemoryBackendType::kArrayBackend: {
        auto backend = std::make_unique<ArrayBackend>();
        if (!backend->shm_deserialize(url)) {
          throw MEMORY_BACKEND_NOT_FOUND.format();
        }
        return backend;
      }

      // Default
      default: return nullptr;
    }
  }
};

}  // namespace hermes_shm::ipc

#endif  // HERMES_SHM_MEMORY_BACKEND_MEMORY_BACKEND_FACTORY_H_
