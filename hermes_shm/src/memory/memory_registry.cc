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

#include "hermes_shm/memory/memory_registry.h"

namespace hshm::ipc {

MemoryRegistry::MemoryRegistry() {
  root_allocator_id_.bits_.major_ = 3;
  root_allocator_id_.bits_.minor_ = 3;
  root_backend_.shm_init(MEGABYTES(128));
  root_backend_.Own();
  root_allocator_.shm_init(root_allocator_id_, 0,
                           root_backend_.data_,
                           root_backend_.data_size_);
  default_allocator_ = &root_allocator_;
  RegisterAllocator(&root_allocator_);
}

}  // namespace hshm::ipc
