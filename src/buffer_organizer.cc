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

#include "sys/file.h"

#include "hermes.h"
#include "buffer_organizer.h"
#include "data_placement_engine.h"

namespace hermes {

BufferOrganizer::BufferOrganizer(int num_threads) : pool(num_threads) {
}

void LocalShutdownBufferOrganizer(SharedMemoryContext *context) {
  // NOTE(chogan): ThreadPool destructor needs to be called manually since we
  // allocated the BO instance with placement new.
  context->bo->pool.~ThreadPool();
}

void ShutdownBufferOrganizer(RpcContext *rpc) {
  RpcCall<bool>(rpc, rpc->node_id, "BO::ShutdownBufferOrganizer");
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
               const std::string &filename, u64 offset) {
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
  } else {
    FailedLibraryCall("open");
  }
  DecrementFlushCount(context, rpc, filename);

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
    pool->run(std::bind(FlushBlob, context, rpc, blob_id, filename, offset));
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

  while ((outstanding_flushes =
          GetNumOutstandingFlushingTasks(context, rpc, id)) != 0) {
    LOG(INFO) << "Waiting for " << outstanding_flushes
              << " outstanding flushes" << std::endl;
    std::this_thread::sleep_for(sleep_time);
  }
}

}  // namespace hermes
