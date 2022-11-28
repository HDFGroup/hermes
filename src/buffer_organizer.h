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

/** move list for buffer organizer */
using BoMoveList = std::vector<std::pair<BufferID, std::vector<BufferID>>>;

/** buffer organizer operations */
enum class BoOperation {
  kMove,
  kCopy,
  kDelete,

  kCount
};

/** buffer organizer priorities */
enum class BoPriority {
  kLow,
  kHigh,

  kCount
};

/**
 A union of structures to represent buffer organizer arguments
*/
union BoArgs {
  /** A structure to represent move arguments */
  struct {
    BufferID src;  /**< source buffer ID */
    TargetID dest; /**< destination target ID */
  } move_args;
  /** A structure to represent copy arguments */
  struct {
    BufferID src;  /**< source buffer ID */
    TargetID dest; /**< destination target ID */
  } copy_args;
  /** A structure to represent delete arguments */
  struct {
    BufferID src; /**< source buffer ID */
  } delete_args;
};

/**
   A structure to represent buffer organizer task
*/
struct BoTask {
  BoOperation op; /**< buffer organizer operation */
  BoArgs args;    /**< buffer organizer arguments */
};

/**
   A structure to represent buffer information
*/
struct BufferInfo {
  BufferID id;        /**< buffer ID */
  f32 bandwidth_mbps; /**< bandwidth in Megabits per second */
  size_t size;        /**< buffer size */
};

/** comparison operator */
bool operator==(const BufferInfo &lhs, const BufferInfo &rhs);

/**
   A structure to represent buffer organizer
*/
struct BufferOrganizer {
  ThreadPool pool; /**< a pool of threads */
  /** initialize buffer organizer with \a num_threads number of threads.  */
  explicit BufferOrganizer(int num_threads);
};

/** enqueue flushing task locally */
bool LocalEnqueueFlushingTask(SharedMemoryContext *context, RpcContext *rpc,
                              BlobID blob_id, const std::string &filename,
                              u64 offset);
/** enqueue flushing task */
bool EnqueueFlushingTask(RpcContext *rpc, BlobID blob_id,
                         const std::string &filename, u64 offset);

void BoMove(SharedMemoryContext *context, RpcContext *rpc,
            const BoMoveList &moves, BlobID blob_id, BucketID bucket_id,
            const std::string &internal_blob_name);
void FlushBlob(SharedMemoryContext *context, RpcContext *rpc, BlobID blob_id,
               const std::string &filename, u64 offset, bool async = false);
/** shut down buffer organizer locally */
void LocalShutdownBufferOrganizer(SharedMemoryContext *context);
/** increment flush count */
void IncrementFlushCount(SharedMemoryContext *context, RpcContext *rpc,
                         const std::string &vbkt_name);
/** decrement flush count */
void DecrementFlushCount(SharedMemoryContext *context, RpcContext *rpc,
                         const std::string &vbkt_name);
/** increment flush count locally */
void LocalIncrementFlushCount(SharedMemoryContext *context,
                              const std::string &vbkt_name);
/** decrement flush count locally */
void LocalDecrementFlushCount(SharedMemoryContext *context,
                              const std::string &vbkt_name);
/** await asynchronous flushing tasks */
void AwaitAsyncFlushingTasks(SharedMemoryContext *context, RpcContext *rpc,
                             VBucketID id);
/** organize BLOB locally */
void LocalOrganizeBlob(SharedMemoryContext *context, RpcContext *rpc,
                       const std::string &internal_blob_name,
                       BucketID bucket_id, f32 epsilon,
                       f32 explicit_importance_score);
/** organize BLOB */
void OrganizeBlob(SharedMemoryContext *context, RpcContext *rpc,
                  BucketID bucket_id, const std::string &blob_name, f32 epsilon,
                  f32 importance_score = -1);
/** organize device */
void OrganizeDevice(SharedMemoryContext *context, RpcContext *rpc,
                    DeviceID devices_id);

/** get buffer information */
std::vector<BufferInfo> GetBufferInfo(SharedMemoryContext *context,
                                      RpcContext *rpc,
                                      const std::vector<BufferID> &buffer_ids);
/** compute BLOB access score */
f32 ComputeBlobAccessScore(SharedMemoryContext *context,
                           const std::vector<BufferInfo> &buffer_info);
/**  enqueue buffer organizer move list locally */
void LocalEnqueueBoMove(SharedMemoryContext *context, RpcContext *rpc,
                        const BoMoveList &moves, BlobID blob_id,
                        BucketID bucket_id,
                        const std::string &internal_blob_name,
                        BoPriority priority);
/**  enqueue buffer organizer move list */
void EnqueueBoMove(RpcContext *rpc, const BoMoveList &moves, BlobID blob_id,
                   BucketID bucket_id, const std::string &internal_name,
                   BoPriority priority);
/** enforce capacity threholds */
void EnforceCapacityThresholds(SharedMemoryContext *context, RpcContext *rpc,
                               ViolationInfo info);
/** enforce capacity threholds locally */
void LocalEnforceCapacityThresholds(SharedMemoryContext *context,
                                    RpcContext *rpc, ViolationInfo info);
}  // namespace hermes

#endif  // HERMES_BUFFER_ORGANIZER_H_
