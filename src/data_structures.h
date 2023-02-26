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

#ifndef HERMES_SRC_DATA_STRUCTURES_H_
#define HERMES_SRC_DATA_STRUCTURES_H_

#include <hermes_shm/data_structures/thread_unsafe/unordered_map.h>
#include <hermes_shm/data_structures/thread_unsafe/vector.h>
#include <hermes_shm/data_structures/thread_unsafe/list.h>
#include <hermes_shm/data_structures/data_structure.h>
#include <hermes_shm/data_structures/string.h>
#include <hermes_shm/data_structures/smart_ptr/manual_ptr.h>
#include <hermes_shm/thread/lock.h>
#include <hermes_shm/types/charbuf.h>
#include <hermes_shm/types/atomic.h>

namespace hipc = hermes_shm::ipc;

using hermes_shm::RwLock;
using hermes_shm::Mutex;
using hermes_shm::bitfield32_t;
using hermes_shm::ScopedRwReadLock;
using hermes_shm::ScopedRwWriteLock;

#include <unordered_map>
#include <unordered_set>
#include <set>
#include <vector>
#include <list>
#include <queue>

namespace hermes {
template<typename T>
struct ShmHeader;
}  // namespace hermes

namespace hermes::api {
template<typename T>
struct ShmHeader;
}  // namespace hermes::api

namespace hermes::config {
template<typename T>
struct ShmHeader;
}  // namespace hermes::config

#endif  // HERMES_SRC_DATA_STRUCTURES_H_
