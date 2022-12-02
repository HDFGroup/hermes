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
#include "hermes_status.h"
#include "rpc_decorator.h"

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
 A structure to represent Target information
*/
struct TargetInfo {
  TargetID id;                  /**< unique ID */
  f32 bandwidth_mbps;           /**< bandwidth in Megabits per second */
  u64 capacity;                 /**< capacity */
};

/**
   A structure to represent buffer organizer
*/
class BufferOrganizer {
 public:
  SharedMemoryContext *context_;
  RpcContext *rpc_;
  ThreadPool pool; /**< a pool of threads */

 public:
  /** initialize buffer organizer with \a num_threads number of threads.  */
  explicit BufferOrganizer(SharedMemoryContext *context,
                           RpcContext *rpc, int num_threads);

  /** get buffer information locally */
  RPC BufferInfo LocalGetBufferInfo(BufferID buffer_id);

  /** get buffer information */
  BufferInfo GetBufferInfo(BufferID buffer_id);

  /** get information for multiple buffers */
  std::vector<BufferInfo>
  GetBufferInfo(const std::vector<BufferID> &buffer_ids);

  /** normalize access score from \a raw-score using \a size_mb */
  f32 NormalizeAccessScore(f32 raw_score, f32 size_mb);

  /** compute blob access score */
  f32 ComputeBlobAccessScore(const std::vector<BufferInfo> &buffer_info);

  /** sort buffer information */
  void SortBufferInfo(std::vector<BufferInfo> &buffer_info, bool increasing);

  /** sort target information */
  void SortTargetInfo(std::vector<TargetInfo> &target_info, bool increasing);

  /** Local enqueue of buffer information */
  RPC void LocalEnqueueBoMove(const BoMoveList &moves, BlobID blob_id,
                              BucketID bucket_id,
                              const std::string &internal_blob_name,
                              BoPriority priority);

  /** enqueue a move operation */
  void EnqueueBoMove(const BoMoveList &moves, BlobID blob_id,
                     BucketID bucket_id, const std::string &internal_name,
                     BoPriority priority);

  /**
   * copy a set of buffers into a set of new buffers.
   *
   * assumes all BufferIDs in destinations are local
   * */
  void BoMove(const BoMoveList &moves, BlobID blob_id, BucketID bucket_id,
              const std::string &internal_blob_name);

  /** change the composition of a blob based on importance locally */
  RPC void LocalOrganizeBlob(const std::string &internal_blob_name,
                             BucketID bucket_id, f32 epsilon,
                             f32 explicit_importance_score);

  /** change the composition of a blob  */
  void OrganizeBlob(BucketID bucket_id, const std::string &blob_name,
                    f32 epsilon, f32 importance_score);


  void EnforceCapacityThresholds(ViolationInfo info);
  RPC void LocalEnforceCapacityThresholds(ViolationInfo info);
  void LocalShutdownBufferOrganizer();
  void FlushBlob(BlobID blob_id, const std::string &filename,
                 u64 offset, bool async);
  bool EnqueueFlushingTask(BlobID blob_id,
                           const std::string &filename, u64 offset);
  RPC bool LocalEnqueueFlushingTask(BlobID blob_id, const std::string &filename,
                                    u64 offset);
  api::Status PlaceInHierarchy(SwapBlob swap_blob, const std::string &name,
                               const api::Context &ctx);
  void LocalAdjustFlushCount(const std::string &vbkt_name, int adjustment);
  RPC void LocalIncrementFlushCount(const std::string &vbkt_name);
  RPC void LocalDecrementFlushCount(const std::string &vbkt_name);
  void IncrementFlushCount(const std::string &vbkt_name);
  void DecrementFlushCount(const std::string &vbkt_name);
  void AwaitAsyncFlushingTasks(VBucketID id);

  /** Automatically Generate RPCs */
  RPC_AUTOGEN_START
  //hello
  //hello
  RPC_AUTOGEN_END
};

static inline f32 BytesToMegabytes(size_t bytes) {
  f32 result = (f32)bytes / (f32)MEGABYTES(1);

  return result;
}

}  // namespace hermes

#endif  // HERMES_BUFFER_ORGANIZER_H_
