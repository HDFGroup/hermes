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
#include <unordered_set>

#include "hermes.h"
#include "buffer_organizer.h"
#include "data_placement_engine.h"

namespace hermes {

BufferOrganizer::BufferOrganizer(int num_threads) : pool(num_threads) {
}

bool operator==(const BufferInfo &lhs, const BufferInfo &rhs) {
  return (lhs.id == rhs.id && lhs.size == rhs.size &&
          lhs.bandwidth_mbps == rhs.bandwidth_mbps);
}

struct TargetInfo {
  TargetID id;
  f32 bandwidth_mbps;
  u64 capacity;
};

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

  f32 min_seconds = size_mb * (1.0 / pool->max_device_bw_mbps);
  f32 max_seconds = size_mb * (1.0 / pool->min_device_bw_mbps);
  f32 range = max_seconds - min_seconds;
  f32 adjusted_score = raw_score - min_seconds;
  f32 result = 1.0 - (adjusted_score / range);
  assert(result >= 0.0 && result <= 1.0);

  return result;
}

static inline f32 BytesToMegabytes(size_t bytes) {
  f32 result = (f32)bytes / (f32)MEGABYTES(1);

  return result;
}

std::vector<BufferInfo> GetBufferInfo(SharedMemoryContext *context,
                                      RpcContext *rpc,
                                      const std::vector<BufferID> &buffer_ids) {
  std::vector<BufferInfo> result(buffer_ids.size());

  for (size_t i = 0; i < buffer_ids.size(); ++i) {
    result[i] = GetBufferInfo(context, rpc, buffer_ids[i]);
  }

  return result;
}

f32 ComputeBlobAccessScore(SharedMemoryContext *context,
                           const std::vector<BufferInfo> &buffer_info) {
  f32 result = 0;
  f32 raw_score = 0;
  f32 total_blob_size_mb = 0;

  for (size_t i = 0; i < buffer_info.size(); ++i) {
    f32 size_in_mb = BytesToMegabytes(buffer_info[i].size);
    f32 seconds_per_mb = 1.0f / buffer_info[i].bandwidth_mbps;
    f32 total_seconds =  size_in_mb * seconds_per_mb;

    total_blob_size_mb += size_in_mb;
    raw_score += total_seconds;
  }
  result = NormalizeAccessScore(context, raw_score, total_blob_size_mb);

  return result;
}
void SortBufferInfo(std::vector<BufferInfo> &buffer_info, bool increasing) {
#define HERMES_BUFFER_INFO_COMPARATOR(direction, comp)    \
  auto direction##_buffer_info_comparator =               \
    [](const BufferInfo &lhs, const BufferInfo &rhs) {    \
      if (lhs.bandwidth_mbps == rhs.bandwidth_mbps) {     \
        return lhs.size > rhs.size;                       \
      }                                                   \
      return lhs.bandwidth_mbps comp rhs.bandwidth_mbps;  \
  };

  if (increasing) {
    // Sort first by bandwidth (descending), then by size (descending)
    HERMES_BUFFER_INFO_COMPARATOR(increasing, >);
    std::sort(buffer_info.begin(), buffer_info.end(),
              increasing_buffer_info_comparator);
  } else {
    // Sort first by bandwidth (ascending), then by size (descending)
    HERMES_BUFFER_INFO_COMPARATOR(decreasing, <);
    std::sort(buffer_info.begin(), buffer_info.end(),
              decreasing_buffer_info_comparator);
  }

#undef HERMES_BUFFER_INFO_COMPARATOR
}

void SortTargetInfo(std::vector<TargetInfo> &target_info, bool increasing) {
  auto increasing_target_info_comparator = [](const TargetInfo &lhs,
                                              const TargetInfo &rhs) {
    return lhs.bandwidth_mbps > rhs.bandwidth_mbps;
  };
  auto decreasing_target_info_comparator = [](const TargetInfo &lhs,
                                              const TargetInfo &rhs) {
    return lhs.bandwidth_mbps < rhs.bandwidth_mbps;
  };

  if (increasing) {
    std::sort(target_info.begin(), target_info.end(),
              increasing_target_info_comparator);
  } else {
    std::sort(target_info.begin(), target_info.end(),
              decreasing_target_info_comparator);
  }
}

void EnqueueBoMove(RpcContext *rpc, const BoMoveList &moves, BlobID blob_id,
                   BucketID bucket_id, const std::string &internal_name,
                   BoPriority priority) {
  RpcCall<bool>(rpc, rpc->node_id, "BO::EnqueueBoMove", moves, blob_id,
                bucket_id, internal_name, priority);
}

void LocalEnqueueBoMove(SharedMemoryContext *context, RpcContext *rpc,
                        const BoMoveList &moves, BlobID blob_id,
                        BucketID bucket_id,
                        const std::string &internal_blob_name,
                        BoPriority priority) {
  ThreadPool *pool = &context->bo->pool;
  bool is_high_priority = priority == BoPriority::kHigh;
  VLOG(1) << "BufferOrganizer moving Blob " << blob_id.as_int;
  pool->run(std::bind(BoMove, context, rpc, moves, blob_id, bucket_id,
                      internal_blob_name),
            is_high_priority);
  VLOG(1) << "BufferOrganizer " << blob_id.as_int << " done\n";
}

/**
 * Assumes all BufferIDs in destinations are local
 */
void BoMove(SharedMemoryContext *context, RpcContext *rpc,
            const BoMoveList &moves, BlobID blob_id, BucketID bucket_id,
            const std::string &internal_blob_name) {
  VLOG(1) << "Moving blob " << blob_id.bits.buffer_ids_offset << std::endl;
  MetadataManager *mdm = GetMetadataManagerFromContext(context);

  if (LocalLockBlob(context, blob_id)) {
    auto warning_string = [](BufferID id) {
      std::ostringstream ss;
      ss << "BufferID" << id.as_int << " not found on this node\n";

      return ss.str();
    };

    std::vector<BufferID> replacement_ids;
    std::vector<BufferID> replaced_ids;

    for (size_t move_index = 0; move_index < moves.size(); ++move_index) {
      BufferID src = moves[move_index].first;
      BufferHeader *src_header = GetHeaderByBufferId(context, src);
      if (src_header) {
        std::vector<u8> src_data(src_header->used);
        Blob blob = {};
        blob.data = src_data.data();
        blob.size = src_header->used;
        LocalReadBufferById(context, src, &blob, 0);
        size_t offset = 0;
        i64 remaining_src_size = (i64)blob.size;
        replaced_ids.push_back(src);

        for (size_t i = 0; i < moves[move_index].second.size(); ++i) {
          BufferID dest = moves[move_index].second[i];
          BufferHeader *dest_header = GetHeaderByBufferId(context, dest);

          if (dest_header) {
            u32 dest_capacity = dest_header->capacity;
            size_t portion_size = std::min((i64)dest_capacity,
                                           remaining_src_size);
            Blob blob_portion = {};
            blob_portion.data = blob.data + offset;
            blob_portion.size = portion_size;
            LocalWriteBufferById(context, dest, blob_portion, offset);
            offset += portion_size;
            remaining_src_size -= portion_size;
            replacement_ids.push_back(dest);
          } else {
            LOG(WARNING) << warning_string(dest);
          }
        }
      } else {
        LOG(WARNING) << warning_string(src);
      }
    }

    if (replacement_ids.size() > 0) {
      std::vector<BufferID> buffer_ids = LocalGetBufferIdList(mdm, blob_id);
      using BufferIdSet = std::unordered_set<BufferID, BufferIdHash>;
      BufferIdSet new_buffer_ids(buffer_ids.begin(), buffer_ids.end());

      // Remove all replaced BufferIDs from the new IDs.
      for (size_t i = 0; i < replaced_ids.size(); ++i) {
        new_buffer_ids.erase(replaced_ids[i]);
      }

      // Add all the replacement IDs
      for (size_t i = 0; i < replacement_ids.size(); ++i) {
        new_buffer_ids.insert(replacement_ids[i]);
      }

      std::vector<BufferID> ids_vec(new_buffer_ids.begin(),
                                    new_buffer_ids.end());
      BlobID new_blob_id = {};
      new_blob_id.bits.node_id = blob_id.bits.node_id;
      new_blob_id.bits.buffer_ids_offset =
        LocalAllocateBufferIdList(mdm, ids_vec);

      BlobInfo new_info = {};
      BlobInfo *old_info = GetBlobInfoPtr(mdm, blob_id);
      new_info.stats = old_info->stats;
      // Invalidate the old Blob. It will get deleted when its TicketMutex
      // reaches old_info->last
      old_info->stop = true;
      ReleaseBlobInfoPtr(mdm);
      LocalPut(mdm, new_blob_id, new_info);

      // update blob_id in bucket's blob list
      ReplaceBlobIdInBucket(context, rpc, bucket_id, blob_id, new_blob_id);
      // update BlobID map
      LocalPut(mdm, internal_blob_name.c_str(), new_blob_id.as_int,
               kMapType_BlobId);

      if (!BlobIsInSwap(blob_id)) {
        LocalReleaseBuffers(context, replaced_ids);
      }
      // NOTE(chogan): We don't free the Blob's BufferIdList here because that
      // would make the buffer_id_list_offset available for new incoming Blobs,
      // and we can't reuse the buffer_id_list_offset until the old BlobInfo is
      // deleted. We take care of both in LocalLockBlob when the final
      // outstanding operation on this BlobID is complete (which is tracked by
      // BlobInfo::last).
    }
    LocalUnlockBlob(context, blob_id);
    VLOG(1) << "Done moving blob " << blob_id.bits.buffer_ids_offset;
  } else {
    LOG(WARNING) << "Couldn't lock BlobID " << blob_id.as_int << "\n";
  }
}

void LocalOrganizeBlob(SharedMemoryContext *context, RpcContext *rpc,
                       const std::string &internal_blob_name,
                       BucketID bucket_id, f32 epsilon,
                       f32 explicit_importance_score) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BlobID blob_id = {};
  blob_id.as_int = LocalGet(mdm, internal_blob_name.c_str(), kMapType_BlobId);

  f32 importance_score = explicit_importance_score;
  if (explicit_importance_score == -1) {
    importance_score = LocalGetBlobImportanceScore(context, blob_id);
  }

  std::vector<BufferID> buffer_ids = LocalGetBufferIdList(mdm, blob_id);
  std::vector<BufferInfo> buffer_info = GetBufferInfo(context, rpc, buffer_ids);
  f32 access_score = ComputeBlobAccessScore(context, buffer_info);
  bool increasing_access_score = importance_score > access_score;
  SortBufferInfo(buffer_info, increasing_access_score);
  std::vector<BufferInfo> new_buffer_info(buffer_info);

  BoMoveList src_dest;

  for (size_t i = 0; i < buffer_info.size(); ++i) {
    std::vector<TargetID> targets = LocalGetNodeTargets(context);
    std::vector<f32> target_bandwidths_mbps = GetBandwidths(context, targets);
    std::vector<u64> capacities = GetRemainingTargetCapacities(context, rpc,
                                                               targets);

    std::vector<TargetInfo> target_info(targets.size());
    for (size_t j = 0; j < target_info.size(); ++j) {
      target_info[j].id = targets[j];
      target_info[j].bandwidth_mbps = target_bandwidths_mbps[j];
      target_info[j].capacity = capacities[j];
    }
    SortTargetInfo(target_info, increasing_access_score);

    BufferID src_buffer_id = {};
    PlacementSchema schema;
    f32 new_bandwidth_mbps = 0;
    for (size_t j = 0; j < target_info.size(); ++j) {
      if (target_info[j].capacity > buffer_info[i].size) {
        src_buffer_id = buffer_info[i].id;
        if (src_buffer_id.bits.node_id == rpc->node_id) {
          // Only consider local BufferIDs
          schema.push_back(std::pair(buffer_info[i].size, target_info[j].id));
          new_bandwidth_mbps = target_info[j].bandwidth_mbps;
          break;
        }
      }
    }

    // TODO(chogan): Possibly merge multiple smaller buffers into one large

    std::vector<BufferID> dest = GetBuffers(context, schema);
    if (dest.size() == 0) {
      continue;
    }

    // Replace old BufferInfo with new so we can calculate the updated access
    // score
    for (size_t j = 0; j < new_buffer_info.size(); ++j) {
      if (new_buffer_info[j].id.as_int == src_buffer_id.as_int) {
        new_buffer_info[j].id = dest[0];
        new_buffer_info[j].bandwidth_mbps = new_bandwidth_mbps;
        BufferHeader *new_header = GetHeaderByBufferId(context, dest[0]);
        // Assume we're using the full capacity
        new_buffer_info[j].size = new_header->capacity;
        break;
      }
    }
    for (size_t j = 1; j < dest.size(); ++j) {
      BufferInfo new_info = {};
      new_info.id = dest[j];
      new_info.bandwidth_mbps = new_bandwidth_mbps;
      BufferHeader *new_header = GetHeaderByBufferId(context, dest[0]);
      // Assume we're using the full capacity
      new_info.size = new_header->capacity;
      new_buffer_info.push_back(new_info);
    }

    f32 new_access_score = ComputeBlobAccessScore(context, new_buffer_info);

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
      src_dest.push_back(std::pair(src_buffer_id, dest));
    } else {
      ReleaseBuffers(context, rpc, dest);
    }

    if (std::abs(importance_score - new_access_score) < epsilon) {
      break;
    }
  }
  EnqueueBoMove(rpc, src_dest, blob_id, bucket_id, internal_blob_name,
                BoPriority::kLow);
}

void OrganizeBlob(SharedMemoryContext *context, RpcContext *rpc,
                  BucketID bucket_id, const std::string &blob_name,
                  f32 epsilon, f32 importance_score) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  std::string internal_name = MakeInternalBlobName(blob_name, bucket_id);
  u32 target_node = HashString(mdm, rpc, internal_name.c_str());

  if (target_node == rpc->node_id) {
    LocalOrganizeBlob(context, rpc, internal_name, bucket_id, epsilon,
                      importance_score);
  } else {
    RpcCall<void>(rpc, target_node, "BO::OrganizeBlob", internal_name,
                  bucket_id, epsilon);
  }
}

void EnforceCapacityThresholds(SharedMemoryContext *context, RpcContext *rpc,
                               DeviceID devices_id) {
  (void)context;
  (void)rpc;
  (void)devices_id;

  // TODO(chogan): Pass in whether it's min or max that's violated
  // TODO(chogan): Pass in how much space needs to be filled/freed

  // while (max is violated)
  // Choose largest buffer from least important Blob
  // Move to lower tier

  // while (min is violated)
  // Choose largest buffer from most important Blob
  // Move to higher tier
}

void LocalShutdownBufferOrganizer(SharedMemoryContext *context) {
  // NOTE(chogan): ThreadPool destructor needs to be called manually since we
  // allocated the BO instance with placement new.
  context->bo->pool.~ThreadPool();
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
      LOG(INFO) << "Flushing BlobID " << blob_id.as_int << " to file "
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
  } else {
    HERMES_NOT_IMPLEMENTED_YET;
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

void LocalAdjustFlushCount(SharedMemoryContext *context,
                           const std::string &vbkt_name, int adjustment) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  VBucketID id = LocalGetVBucketId(context, vbkt_name.c_str());
  BeginTicketMutex(&mdm->vbucket_mutex);
  VBucketInfo *info = LocalGetVBucketInfoById(mdm, id);
  if (info) {
    int flush_count = info->async_flush_count.fetch_add(adjustment);
    VLOG(1) << "Flush count on VBucket " << vbkt_name
            << (adjustment > 0 ? "incremented" : "decremented") << " to "
            << flush_count + adjustment << "\n";
  }
  EndTicketMutex(&mdm->vbucket_mutex);
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
