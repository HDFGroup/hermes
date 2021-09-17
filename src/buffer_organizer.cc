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

#include <sys/file.h>
#include <algorithm>

#include "hermes.h"
#include "buffer_organizer.h"
#include "data_placement_engine.h"

namespace hermes {

BufferOrganizer::BufferOrganizer(int num_threads) : pool(num_threads) {
}

BufferInfo LocalGetBufferInfo(SharedMemoryContext *context,
                              BufferID buffer_id) {
  BufferInfo result = {};

  BufferHeader *header = GetHeaderByBufferId(context, buffer_id);
  // TODO(chogan): Should probably use Targets to factor in remote devices.
  // However, we currently don't distinguish between the bandwidth of a device
  // and the same device accessed from a remote node.
  Device *device = GetDeviceFromHeader(context, header);

  result.id = buffer_id;
  result.bandwidth_mbps = device->bandwidth_mbps;
  result.size = header->used;

  return result;
}

BufferInfo GetBufferInfo(SharedMemoryContext *context, RpcContext *rpc,
                         BufferID buffer_id) {
  BufferInfo result = {};
  u32 target_node = buffer_id.bits.node_id;

  if (target_node == rpc->node_id) {
    result = LocalGetBufferInfo(context, buffer_id);
  } else {
    result = RpcCall<BufferInfo>(rpc, target_node, "RemoteGetBufferInfo",
                                 buffer_id);
  }

  return result;
}

f32 NormalizeAccessScore(SharedMemoryContext *context, f32 raw_score,
                         f32 size_mb) {
  BufferPool *pool = GetBufferPoolFromContext(context);

  f32 min_seconds = size_mb * pool->min_device_bw_mbps;
  f32 max_seconds = size_mb * pool->max_device_bw_mbps;
  f32 range = max_seconds - min_seconds;
  f32 adjusted_score = raw_score - min_seconds;
  f32 result = adjusted_score / range;

  return result;
}

static inline f32 BytesToMegabytes(size_t bytes) {
  f32 result = (f32)bytes / (f32)MEGABYTES(1);

  return result;
}

f32 ComputeBlobAccessScore(SharedMemoryContext *context, RpcContext *rpc,
                           const std::vector<BufferID> &buffer_ids,
                           std::vector<BufferInfo> &buffer_info,
                           f32 &total_blob_size_mb) {
  f32 result = 0;
  total_blob_size_mb = 0;
  size_t num_buffers = buffer_ids.size();

  if (buffer_info.size() != num_buffers) {
    LOG(ERROR) << "Couldn't compute Blob access score. "
               << "Expected buffer_ids.size() to equal buffer_info.size(), but "
               << num_buffers << " != " << buffer_info.size() << "\n";
  } else {
    f32 raw_score = 0;
    for (size_t i = 0; i < num_buffers; ++i) {
      BufferID id = buffer_ids[i];
      buffer_info[i] = GetBufferInfo(context, rpc, id);

      f32 size_in_mb = BytesToMegabytes(buffer_info[i].size);
      f32 seconds_per_mb = 1.0f / buffer_info[i].bandwidth_mbps;
      f32 total_seconds =  size_in_mb * seconds_per_mb;

      total_blob_size_mb += size_in_mb;
      raw_score += total_seconds;
    }
    result = NormalizeAccessScore(context, raw_score, total_blob_size_mb);
  }

  return result;
}

void LocalOrganizeBlob(SharedMemoryContext *context, RpcContext *rpc,
                       const std::string &internal_blob_name, double epsilon) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BlobID blob_id = {};
  blob_id.as_int = LocalGet(mdm, internal_blob_name.c_str(), kMapType_BlobId);

  f32 importance_score = LocalGetBlobImportanceScore(context, blob_id);

  std::vector<BufferID> buffer_ids = LocalGetBufferIdList(mdm, blob_id);

  std::vector<BufferInfo> buffer_info(buffer_ids.size());
  f32 total_blob_size_mb = 0;
  f32 access_score = ComputeBlobAccessScore(context, rpc, buffer_ids,
                                            buffer_info, total_blob_size_mb);

  bool increasing_access_score = importance_score > access_score;
  // f32 score_difference = std::abs(importance_score - access_score);

  auto buffer_info_comparator =
    [](const BufferInfo &lhs, const BufferInfo &rhs) {
      return (std::tie(lhs.bandwidth_mbps, lhs.size) <
              std::tie(rhs.bandwidth_mbps, rhs.size));
  };

  if (increasing_access_score) {
    std::sort(buffer_info.begin(), buffer_info.end(), buffer_info_comparator);
  } else {
    std::sort(buffer_info.rbegin(), buffer_info.rend(), buffer_info_comparator);
  }

  for (size_t i = 0; i < buffer_info.size(); ++i) {
    // TODO(chogan): Get systemviewstate
    // TODO(chogan): GetBuffers
    // TODO(chogan): Calculate score based on new buffers, not existing
    f32 buffer_size_mb = BytesToMegabytes(buffer_info[i].size);
    f32 buffer_access_seconds = buffer_size_mb * buffer_info[i].bandwidth_mbps;
    f32 buffer_contribution = buffer_size_mb / total_blob_size_mb;
    f32 adjusted_access_seconds = buffer_access_seconds * buffer_contribution;
    f32 buffer_access_score =
      NormalizeAccessScore(context, adjusted_access_seconds, buffer_size_mb);

    if (!increasing_access_score) {
      buffer_access_score *= -1;
    }

    f32 new_access_score = access_score + buffer_access_score;
    bool move_is_valid = true;
    // Make sure we didn't move too far past the target
    if (increasing_access_score) {
      if (new_access_score > importance_score &&
          new_access_score - importance_score > epsilon) {
        move_is_valid = false;
      }
    } else {
      if (new_access_score < importance_score &&
          importance_score - new_access_score > epsilon) {
        move_is_valid = false;
      }
    }

    if (move_is_valid) {
      // TODO(chogan): EnqueueBOTask();
    }

    if (std::abs(importance_score - access_score) < epsilon) {
      break;
    }
  }
}

void OrganizeBlob(SharedMemoryContext *context, RpcContext *rpc,
                  BucketID bucket_id, const std::string &blob_name,
                  double epsilon) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  std::string internal_name = MakeInternalBlobName(blob_name, bucket_id);
  u32 target_node = HashString(mdm, rpc, internal_name.c_str());

  if (target_node == rpc->node_id) {
    LocalOrganizeBlob(context, rpc, internal_name, epsilon);
  } else {
    RpcCall<void>(rpc, target_node, "RemoteOrganizeBlob", internal_name,
                  epsilon);
  }
}

void LocalShutdownBufferOrganizer(SharedMemoryContext *context) {
  // NOTE(chogan): ThreadPool destructor needs to be called manually since we
  // allocated the BO instance with placement new.
  context->bo->pool.~ThreadPool();
}

void BoMove(SharedMemoryContext *context, BufferID src, TargetID dest) {
  (void)context;
  printf("%s(%d, %d)\n", __func__, (int)src.as_int, (int)dest.as_int);
}

void BoCopy(SharedMemoryContext *context, BufferID src, TargetID dest) {
  (void)context;
  printf("%s(%d, %d)\n", __func__, (int)src.as_int, (int)dest.as_int);
}

void BoDelete(SharedMemoryContext *context, BufferID src) {
  (void)context;
  printf("%s(%d)\n", __func__, (int)src.as_int);
}

bool LocalEnqueueBoTask(SharedMemoryContext *context, BoTask task,
                        BoPriority priority) {
  // TODO(chogan): Limit queue size and return false when full
  bool result = true;
  bool is_high_priority = priority == BoPriority::kHigh;

  ThreadPool *pool = &context->bo->pool;
  switch (task.op) {
    case BoOperation::kMove: {
      pool->run(std::bind(BoMove, context, task.args.move_args.src,
                          task.args.move_args.dest), is_high_priority);
      break;
    }
    case BoOperation::kCopy: {
      pool->run(std::bind(BoCopy, context, task.args.copy_args.src,
                          task.args.copy_args.dest), is_high_priority);
      break;
    }
    case BoOperation::kDelete: {
      pool->run(std::bind(BoDelete, context, task.args.delete_args.src),
                is_high_priority);
      break;
    }
    default: {
      HERMES_INVALID_CODE_PATH;
    }
  }

  return result;
}

void FlushBlob(SharedMemoryContext *context, RpcContext *rpc, BlobID blob_id,
               const std::string &filename, u64 offset, bool async) {
  if (LockBlob(context, rpc, blob_id)) {
    int open_flags = 0;
    mode_t open_mode = 0;
    if (access(filename.c_str(), F_OK) == 0) {
      open_flags = O_WRONLY;
    } else {
      open_flags = O_WRONLY | O_CREAT | O_TRUNC;
      open_mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    }

    int fd = open(filename.c_str(), open_flags, open_mode);
    if (fd != -1) {
      VLOG(1) << "Flushing BlobID " << blob_id.as_int << " to file "
              << filename << " at offset " << offset << "\n";

      const int kFlushBufferSize = KILOBYTES(4);
      u8 flush_buffer[kFlushBufferSize];
      Arena local_arena = {};
      InitArena(&local_arena, kFlushBufferSize, flush_buffer);

      if (flock(fd, LOCK_EX) != 0) {
        FailedLibraryCall("flock");
      }

      StdIoPersistBlob(context, rpc, &local_arena, blob_id, fd, offset);

      if (flock(fd, LOCK_UN) != 0) {
        FailedLibraryCall("flock");
      }

      if (close(fd) != 0) {
        FailedLibraryCall("close");
      }
    } else {
      FailedLibraryCall("open");
    }
    UnlockBlob(context, rpc, blob_id);
  }

  if (async) {
    DecrementFlushCount(context, rpc, filename);
  }

  // TODO(chogan):
  // if (DONTNEED) {
  //   DestroyBlobById();
  // } else {
  //   ReplaceBlobWithSwapBlob();
  // }
}

bool EnqueueFlushingTask(RpcContext *rpc, BlobID blob_id,
                         const std::string &filename, u64 offset) {
  bool result = RpcCall<bool>(rpc, rpc->node_id, "BO::EnqueueFlushingTask",
                              blob_id, filename, offset);

  return result;
}

bool LocalEnqueueFlushingTask(SharedMemoryContext *context, RpcContext *rpc,
                              BlobID blob_id, const std::string &filename,
                              u64 offset) {
  bool result = false;

  // TODO(chogan): Handle Swap Blobs (should work, just needs testing)
  if (!BlobIsInSwap(blob_id)) {
    ThreadPool *pool = &context->bo->pool;
    IncrementFlushCount(context, rpc, filename);
    bool async = true;
    pool->run(std::bind(FlushBlob, context, rpc, blob_id, filename, offset,
                        async));
    result = true;
  }

  return result;
}

Status PlaceInHierarchy(SharedMemoryContext *context, RpcContext *rpc,
                        SwapBlob swap_blob, const std::string &name,
                        const api::Context &ctx) {
  std::vector<PlacementSchema> schemas;
  std::vector<size_t> sizes(1, swap_blob.size);
  Status result = CalculatePlacement(context, rpc, sizes, schemas, ctx);

  if (result.Succeeded()) {
    std::vector<u8> blob_mem(swap_blob.size);
    Blob blob = {};
    blob.data = blob_mem.data();
    blob.size = blob_mem.size();
    ReadFromSwap(context, blob, swap_blob);
    result = PlaceBlob(context, rpc, schemas[0], blob, name,
                       swap_blob.bucket_id, ctx, true);
  } else {
    LOG(ERROR) << result.Msg();
  }

  return result;
}

int MoveToTarget(SharedMemoryContext *context, RpcContext *rpc, BlobID blob_id,
                 TargetID dest) {
  (void)(context);
  (void)(rpc);
  (void)(blob_id);
  (void)(dest);
// TODO(chogan): Move blob from current location to Target dest
  HERMES_NOT_IMPLEMENTED_YET;
  int result = 0;
  return result;
}

void LocalAdjustFlushCount(SharedMemoryContext *context,
                           const std::string &vbkt_name, int adjustment) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  VBucketID id = LocalGetVBucketId(context, vbkt_name.c_str());
  VBucketInfo *info = LocalGetVBucketInfoById(mdm, id);
  if (info) {
    int flush_count = info->async_flush_count.fetch_add(adjustment);
    VLOG(1) << "Flush count on VBucket " << vbkt_name
            << (adjustment > 0 ? "incremented" : "decremented") << " to "
            << flush_count + adjustment << "\n";
  }
}

void LocalIncrementFlushCount(SharedMemoryContext *context,
                              const std::string &vbkt_name) {
  LocalAdjustFlushCount(context, vbkt_name, 1);
}

void LocalDecrementFlushCount(SharedMemoryContext *context,
                         const std::string &vbkt_name) {
  LocalAdjustFlushCount(context, vbkt_name, -1);
}

void IncrementFlushCount(SharedMemoryContext *context, RpcContext *rpc,
                         const std::string &vbkt_name) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  u32 target_node = HashString(mdm, rpc, vbkt_name.c_str());

  if (target_node == rpc->node_id) {
    LocalIncrementFlushCount(context, vbkt_name);
  } else {
    RpcCall<bool>(rpc, target_node, "RemoteIncrementFlushCount",
                  vbkt_name);
  }
}

void DecrementFlushCount(SharedMemoryContext *context, RpcContext *rpc,
                         const std::string &vbkt_name) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  u32 target_node = HashString(mdm, rpc, vbkt_name.c_str());

  if (target_node == rpc->node_id) {
    LocalDecrementFlushCount(context, vbkt_name);
  } else {
    RpcCall<bool>(rpc, target_node, "RemoteDecrementFlushCount",
                  vbkt_name);
  }
}

void AwaitAsyncFlushingTasks(SharedMemoryContext *context, RpcContext *rpc,
                             VBucketID id) {
  auto sleep_time = std::chrono::milliseconds(500);
  int outstanding_flushes = 0;
  int log_every = 10;
  int counter = 0;

  while ((outstanding_flushes =
          GetNumOutstandingFlushingTasks(context, rpc, id)) != 0) {
    if (++counter == log_every) {
      LOG(INFO) << "Waiting for " << outstanding_flushes
                << " outstanding flushes" << std::endl;
      counter = 0;
    }
    std::this_thread::sleep_for(sleep_time);
  }
}

}  // namespace hermes
