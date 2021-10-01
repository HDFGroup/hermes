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

#ifndef HERMES_BUFFER_ORGANIZER_H_
#define HERMES_BUFFER_ORGANIZER_H_

#include "thread_pool.h"

namespace hermes {

enum class BoOperation {
  kMove,
  kCopy,
  kDelete,

  kCount
};

enum class BoPriority {
  kLow,
  kHigh,

  kCount
};

union BoArgs {
  struct {
    BufferID src;
    TargetID dest;
  } move_args;
  struct {
    BufferID src;
    TargetID dest;
  } copy_args;
  struct {
    BufferID src;
  } delete_args;
};

struct BoTask {
  BoOperation op;
  BoArgs args;
};

struct BufferOrganizer {
  ThreadPool pool;

  explicit BufferOrganizer(int num_threads);
};

bool LocalEnqueueBoTask(SharedMemoryContext *context, BoTask task,
                        BoPriority priority = BoPriority::kLow);
bool LocalEnqueueFlushingTask(SharedMemoryContext *context, RpcContext *rpc,
                              BlobID blob_id, const std::string &filename,
                              u64 offset);
bool EnqueueFlushingTask(RpcContext *rpc, BlobID blob_id,
                         const std::string &filename, u64 offset);

void BoMove(SharedMemoryContext *context, BufferID src, TargetID dest);
void BoCopy(SharedMemoryContext *context, BufferID src, TargetID dest);
void BoDelete(SharedMemoryContext *context, BufferID src);

void FlushBlob(SharedMemoryContext *context, RpcContext *rpc, BlobID blob_id,
               const std::string &filename, u64 offset, bool async = false);
void LocalShutdownBufferOrganizer(SharedMemoryContext *context);
void IncrementFlushCount(SharedMemoryContext *context, RpcContext *rpc,
                         const std::string &vbkt_name);
void DecrementFlushCount(SharedMemoryContext *context, RpcContext *rpc,
                         const std::string &vbkt_name);
void LocalIncrementFlushCount(SharedMemoryContext *context,
                              const std::string &vbkt_name);
void LocalDecrementFlushCount(SharedMemoryContext *context,
                              const std::string &vbkt_name);
void AwaitAsyncFlushingTasks(SharedMemoryContext *context, RpcContext *rpc,
                             VBucketID id);
}  // namespace hermes

#endif  // HERMES_BUFFER_ORGANIZER_H_
