//
// Created by lukemartinlogan on 12/2/22.
//

#ifndef HERMES_SRC_DATA_STRUCTURES_H_
#define HERMES_SRC_DATA_STRUCTURES_H_

#include <labstor/data_structures/thread_unsafe/unordered_map.h>
#include <labstor/data_structures/thread_unsafe/vector.h>
#include <labstor/data_structures/thread_unsafe/list.h>
#include <labstor/data_structures/string.h>
#include <labstor/data_structures/smart_ptr/manual_ptr.h>
#include <labstor/types/atomic.h>

namespace lipc = labstor::ipc;

using labstor::RwLock;
using labstor::Mutex;

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
