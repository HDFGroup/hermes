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

#ifndef HERMES_SHM_ERRORS_H
#define HERMES_SHM_ERRORS_H

#ifdef __cplusplus

#include <hermes_shm/util/error.h>

namespace hermes_shm {
  const Error FILE_NOT_FOUND("File not found at {}");
  const Error INVALID_STORAGE_TYPE("{} is not a valid storage method");
  const Error INVALID_SERIALIZER_TYPE("{} is not a valid serializer type");
  const Error INVALID_TRANSPORT_TYPE("{} is not a valid transport type");
  const Error INVALID_AFFINITY("Could not set CPU affinity of thread: {}");
  const Error MMAP_FAILED("Could not mmap file: {}");
  const Error LAZY_ERROR("Error in function {}");
  const Error PTHREAD_CREATE_FAILED("Failed to create a pthread");
  const Error NOT_IMPLEMENTED("{} not implemented");

  const Error DLSYM_MODULE_NOT_FOUND("Module {} was not loaded; error {}");
  const Error DLSYM_MODULE_NO_CONSTRUCTOR("Module {} has no constructor");

  const Error UNIX_SOCKET_FAILED("Failed to create socket: {}");
  const Error UNIX_BIND_FAILED("Failed to bind socket: {}");
  const Error UNIX_CONNECT_FAILED("Failed to connect over socket: {}");
  const Error UNIX_SENDMSG_FAILED("Failed to send message over socket: {}");
  const Error UNIX_RECVMSG_FAILED("Failed to receive message over socket: {}");
  const Error UNIX_SETSOCKOPT_FAILED("Failed to set socket options: {}");
  const Error UNIX_GETSOCKOPT_FAILED("Failed to acquire user credentials: {}");
  const Error UNIX_LISTEN_FAILED("Failed to listen for connections: {}");
  const Error UNIX_ACCEPT_FAILED("Failed accept connections: {}");

  const Error ARRAY_OUT_OF_BOUNDS("Exceeded the bounds of array in {}");

  const Error SHMEM_CREATE_FAILED("Failed to allocate SHMEM");
  const Error SHMEM_RESERVE_FAILED("Failed to reserve SHMEM");
  const Error SHMEM_NOT_SUPPORTED("Attempting to deserialize a non-shm backend");
  const Error MEMORY_BACKEND_NOT_FOUND("Failed to find the memory backend");
  const Error NOT_ENOUGH_CONCURRENT_SPACE("{}: Failed to divide memory slot {} among {} devices");
  const Error ALIGNED_ALLOC_NOT_SUPPORTED("Allocator does not support aligned allocations");
  const Error PAGE_SIZE_UNSUPPORTED("Allocator does not support size: {}");
  const Error OUT_OF_MEMORY("{}: could not allocate memory of size {}");
  const Error INVALID_FREE("{}: could not free memory of size {}");
  const Error DOUBLE_FREE("Freeing the same memory twice!");

  const Error UNORDERED_MAP_CANT_FIND("Could not find key in unordered_map");
}  // namespace hermes_shm

#endif

#endif  // HERMES_SHM_ERRORS_H
