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


#include <hermes_shm/memory/memory_manager.h>
#include "hermes_shm/memory/backend/memory_backend_factory.h"
#include "hermes_shm/memory/allocator/allocator_factory.h"
#include <hermes_shm/introspect/system_info.h>
#include "hermes_shm/constants/data_structure_singleton_macros.h"

namespace hshm::ipc {

void MemoryManager::ScanBackends() {
  for (auto &[url, backend] : HERMES_MEMORY_REGISTRY->backends_) {
    auto alloc = AllocatorFactory::shm_deserialize(backend.get());
    RegisterAllocator(alloc);
  }
}

}  // namespace hshm::ipc
