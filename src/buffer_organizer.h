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

using BoMoveList = std::vector<std::pair<BufferID, std::vector<BufferID>>>;

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

struct BufferInfo {
  BufferID id;
  f32 bandwidth_mbps;
  size_t size;
};

bool operator==(const BufferInfo &lhs, const BufferInfo &rhs);

struct BufferOrganizer {
  ThreadPool pool;

  explicit BufferOrganizer(int num_threads);
};

bool LocalEnqueueFlushingTask(SharedMemoryContext *context, RpcContext *rpc,
                              BlobID blob_id, const std::string &filename,
                              u64 offset);
bool EnqueueFlushingTask(RpcContext *rpc, BlobID blob_id,
                         const std::string &filename, u64 offset);

void BoMove(SharedMemoryContext *context, RpcContext *rpc,
            const BoMoveList &moves, BlobID blob_id, BucketID bucket_id,
            const std::string &internal_blob_name);
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

void LocalOrganizeBlob(SharedMemoryContext *context, RpcContext *rpc,
                       const std::string &internal_blob_name,
                       BucketID bucket_id, f32 epsilon,
                       f32 explicit_importance_score);

void OrganizeBlob(SharedMemoryContext *context, RpcContext *rpc,
                  BucketID bucket_id, const std::string &blob_name,
                  f32 epsilon, f32 importance_score = -1);
void OrganizeDevice(SharedMemoryContext *context, RpcContext *rpc,
                    DeviceID devices_id);
std::vector<BufferInfo> GetBufferInfo(SharedMemoryContext *context,
                                      RpcContext *rpc,
                                      const std::vector<BufferID> &buffer_ids);
f32 ComputeBlobAccessScore(SharedMemoryContext *context,
                           const std::vector<BufferInfo> &buffer_info);
void LocalEnqueueBoMove(SharedMemoryContext *context, RpcContext *rpc,
                        const BoMoveList &moves, BlobID blob_id,
                        BucketID bucket_id,
                        const std::string &internal_blob_name,
                        BoPriority priority);
void EnqueueBoMove(RpcContext *rpc, const BoMoveList &moves, BlobID blob_id,
                   BucketID bucket_id, const std::string &internal_name,
                   BoPriority priority);
void EnforceCapacityThresholds(SharedMemoryContext *context, RpcContext *rpc,
                               DeviceID devices_id);
}  // namespace hermes

#endif  // HERMES_BUFFER_ORGANIZER_H_
