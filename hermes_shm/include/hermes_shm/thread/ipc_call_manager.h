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

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_THREAD_IPCALL_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_THREAD_IPCALL_H_

#include <vector>
#include <hermes_shm/types/real_number.h>
#include <functional>
#include <hermes_shm/data_structures/ipc/vector.h>
#include <hermes_shm/data_structures/ipc/mpsc_queue.h>

namespace hshm::ipc {

/** Unique id of an IPC function to call */
typedef uint32_t ipc_call_id_t;

/** Used for asynchronous polling of IPC call */
struct IpcFuture {
  bitfield32_t *completion_;

  /** Default constructor */
  IpcFuture() = default;

  /** Emplace constructor */
  IpcFuture(char *ptr) {
    completion_ = reinterpret_cast<bitfield32_t*>(ptr);
  }

  bool IsComplete() {
    return (*completion_).Any(1);
  }
};

/**
 * Used for single-node, inter-process procedure calls.
 * Calls are divided into two groups: readers and writers.
 *
 * Read calls will occur in the thread that called them since reads can
 * progress in parallel.
 *
 * Write calls will occur in an asynchronous thread.
 *
 * This implementation requires that all arguments to the IPC function
 * can be stored in shared memory.
 * */
class IpcCallManager {
 private:
  std::vector<std::pair<ipc_call_id_t, std::function<void()>>> ipcs_;
  hipc::Allocator *alloc_;
  hipc::Ref<hipc::mpsc_queue<hipc::OffsetPointer>> queue_;

 public:
  /** Register an IPC to be called server-side */
  template<typename FUNC>
  void RegisterIpc(ipc_call_id_t id, FUNC &&lambda) {
    if (id >= ipcs_.size()) {
      size_t new_size = (RealNumber(5, 4) * (id + 16)).as_int();
      ipcs_.resize(new_size);
    }
    ipcs_[id] = lambda;
  }

  /** Call a function asynchronously */
  template<typename ...Args>
  IpcFuture AsyncCall(ipc_call_id_t id, Args&& ...args) {
    bitfield32_t completion;

    // Get size of serialized message
    size_t size = 0;
    auto lambda1 = [&size](auto i, auto &&arg) {
      if constexpr(std::is_pod<decltype(arg)>()) {
        size += sizeof(arg);
      } else if constexpr (IS_SHM_ARCHIVEABLE(decltype(arg))) {
        size += sizeof(hipc::OffsetPointer);
      } else {
        throw IPC_ARGS_NOT_SHM_COMPATIBLE.format();
      }
    };
    hshm::ForwardIterateArgpack::Apply(
      make_argpack(completion, id, std::forward<Args>(args)...),
      lambda1);

    // Allocate SHM message
    hipc::OffsetPointer p;
    char *ptr = alloc_->AllocatePtr<char>(size, p);

    // Serialize id + args into a buffer
    auto lambda2 = [&ptr](auto i, auto &&arg) {
      if constexpr(std::is_pod<decltype(arg)>()) {
        *reinterpret_cast<decltype(arg)*>(ptr) = arg;
        ptr += sizeof(arg);
      } else if constexpr (IS_SHM_ARCHIVEABLE(decltype(arg))) {
        arg >> *reinterpret_cast<hipc::OffsetPointer*>(ptr);
        ptr += sizeof(hipc::OffsetPointer);
      } else {
        throw IPC_ARGS_NOT_SHM_COMPATIBLE.format();
      }
    };
    hshm::ForwardIterateArgpack::Apply(
      make_argpack(completion, id, std::forward<Args>(args)...),
      lambda2);

    // Submit request into a queue
    queue_->emplace(ptr);

    // Send completion event
    IpcFuture future(ptr);
    return future;
  }

  /** Process this read event if possible */
  template<typename RetT>
  void ProcessRead() {
  }

  /** Process an event in the queue */
  void ProcessWrite() {
  }
};

}  // namespace hshm::ipc

#endif  // HERMES_SHM_INCLUDE_HERMES_SHM_THREAD_IPCALL_H_
