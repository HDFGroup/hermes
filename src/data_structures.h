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

#include <hermes_shm/data_structures/ipc/unordered_map.h>
#include <hermes_shm/data_structures/ipc/vector.h>
#include <hermes_shm/data_structures/ipc/list.h>
#include <hermes_shm/data_structures/ipc/slist.h>
#include <hermes_shm/data_structures/data_structure.h>
#include <hermes_shm/data_structures/ipc/string.h>
#include <hermes_shm/data_structures/containers/charbuf.h>
#include <hermes_shm/data_structures/containers/converters.h>
#include <hermes_shm/thread/lock.h>
#include <hermes_shm/thread/thread_manager.h>
#include <hermes_shm/types/atomic.h>

using hshm::RwLock;
using hshm::Mutex;
using hshm::bitfield32_t;
using hshm::ScopedRwReadLock;
using hshm::ScopedRwWriteLock;

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
