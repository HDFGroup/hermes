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

#ifndef HERMES_SHM_TEST_UNIT_DATA_STRUCTURES_SERIALIZE_THALLIUM_TEST_INIT_H_
#define HERMES_SHM_TEST_UNIT_DATA_STRUCTURES_SERIALIZE_THALLIUM_TEST_INIT_H_

#include <thallium.hpp>
#include "hermes_shm/thread/thread_model_manager.h"
#include "hermes_shm/thread/thread_model/thread_model_factory.h"
#include "hermes_shm/data_structures/data_structure.h"
#include "hermes_shm/data_structures/serialization/thallium.h"

using hshm::ipc::PosixShmMmap;
using hshm::ipc::MemoryBackendType;
using hshm::ipc::MemoryBackend;
using hshm::ipc::allocator_id_t;
using hshm::ipc::AllocatorType;
using hshm::ipc::Allocator;
using hshm::ipc::Pointer;

using hshm::ipc::MemoryBackendType;
using hshm::ipc::MemoryBackend;
using hshm::ipc::allocator_id_t;
using hshm::ipc::AllocatorType;
using hshm::ipc::Allocator;
using hshm::ipc::MemoryManager;
using hshm::ipc::Pointer;

namespace thallium {
class Constants {
 public:
  static inline const char *kServerName = "ofi+sockets://127.0.0.1:8080";
  static inline const char *kTestString = "012344823723642364723874623";

  /** Test cases */
  static inline const char *kStringTest0 = "kStringTest0";
  static inline const char *kStringTestLarge = "kStringTestLarge";
  static inline const char *kCharbufTest0 = "kCharbufTest0";
  static inline const char *kCharbufTestLarge = "kCharbufTestLarge";
  static inline const char *kVecOfInt0Test = "kVecOfInt0Test";
  static inline const char *kVecOfIntLargeTest = "kVecOfIntLargeTest";
  static inline const char *kVecOfString0Test = "kVecOfString0Test";
  static inline const char *kVecOfStringLargeTest = "kVecOfStringLargeTest";
  static inline const char *kBitfieldTest = "kBitfieldTest";
};
}  // namespace thallium
using tcnst = thallium::Constants;

namespace tl = thallium;
using thallium::request;

/** Test init */
template<typename AllocT>
void ServerPretest() {
  std::string shm_url = "test_serializers";
  allocator_id_t alloc_id(0, 1);
  auto mem_mngr = HERMES_MEMORY_MANAGER;
  mem_mngr->CreateBackend<PosixShmMmap>(
    MemoryManager::kDefaultBackendSize, shm_url);
  mem_mngr->CreateAllocator<AllocT>(shm_url, alloc_id, 0);
}

template<typename AllocT>
void ClientPretest() {
  std::string shm_url = "test_serializers";
  allocator_id_t alloc_id(0, 1);
  auto mem_mngr = HERMES_MEMORY_MANAGER;
  mem_mngr->AttachBackend(MemoryBackendType::kPosixShmMmap, shm_url);
}

extern std::unique_ptr<tl::engine> client_;
extern std::unique_ptr<tl::engine> server_;

#endif  // HERMES_SHM_TEST_UNIT_DATA_STRUCTURES_SERIALIZE_THALLIUM_TEST_INIT_H_
