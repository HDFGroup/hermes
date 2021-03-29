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

#ifndef HERMES_METADATA_MANAGEMENT_H_
#define HERMES_METADATA_MANAGEMENT_H_

#include <string.h>

#include <atomic>
#include <string>

#include "memory_management.h"
#include "buffer_pool.h"

namespace hermes {

static const u32 kGlobalMutexNodeId = 1;

struct RpcContext;

enum MapType {
  kMapType_Bucket,
  kMapType_VBucket,
  kMapType_Blob,

  kMapType_Count
};

struct Stats {
};

const int kIdListChunkSize = 10;

struct ChunkedIdList {
  u32 head_offset;
  u32 length;
  u32 capacity;
};

struct IdList {
  u32 head_offset;
  u32 length;
};

struct BufferIdArray {
  BufferID *ids;
  u32 length;
};

struct BucketInfo {
  BucketID next_free;
  ChunkedIdList blobs;
  std::atomic<int> ref_count;
  bool active;
  Stats stats;
};

static constexpr int kMaxTraitsPerVBucket = 8;

struct VBucketInfo {
  VBucketID next_free;
  ChunkedIdList blobs;
  std::atomic<int> ref_count;
  TraitID traits[kMaxTraitsPerVBucket];
  bool active;
  Stats stats;
};

struct SystemViewState {
  std::atomic<u64> bytes_available[kMaxDevices];
  int num_devices;
};

struct MetadataManager {
  // All offsets are relative to the beginning of the MDM
  ptrdiff_t bucket_info_offset;
  BucketID first_free_bucket;

  ptrdiff_t vbucket_info_offset;
  VBucketID first_free_vbucket;

  ptrdiff_t rpc_state_offset;
  ptrdiff_t system_view_state_offset;
  ptrdiff_t global_system_view_state_offset;

  ptrdiff_t id_heap_offset;
  ptrdiff_t map_heap_offset;

  ptrdiff_t bucket_map_offset;
  ptrdiff_t vbucket_map_offset;
  ptrdiff_t blob_map_offset;

  ptrdiff_t swap_filename_prefix_offset;
  ptrdiff_t swap_filename_suffix_offset;

  // TODO(chogan): @optimization Should the TicketMutexes here be reader/writer
  // locks?

  /** Lock for accessing `BucketInfo` structures located at
   * `bucket_info_offset` */
  TicketMutex bucket_mutex;
  /** Lock for accessing `VBucketInfo` structures located at
   * `vbucket_info_offset` */
  TicketMutex vbucket_mutex;

  /** Lock for accessing the `IdMap` located at `bucket_map_offset` */
  TicketMutex bucket_map_mutex;
  /** Lock for accessing the `IdMap` located at `vbucket_map_offset` */
  TicketMutex vbucket_map_mutex;
  /** Lock for accessing the `IdMap` located at `blob_map_offset` */
  TicketMutex blob_map_mutex;
  /** Lock for accessing `IdList`s and `ChunkedIdList`s */
  TicketMutex id_mutex;

  size_t map_seed;

  IdList node_targets;
  IdList neighborhood_targets;

  u32 system_view_state_update_interval_ms;
  u32 global_system_view_state_node_id;
  u32 num_buckets;
  u32 max_buckets;
  u32 num_vbuckets;
  u32 max_vbuckets;
};

struct RpcContext;

/**
 *
 */
void InitMetadataManager(MetadataManager *mdm, Arena *arena, Config *config,
                         int node_id);

/**
 *
 */
void InitNeighborhoodTargets(SharedMemoryContext *context, RpcContext *rpc);

/**
 *
 */
bool DestroyBucket(SharedMemoryContext *context, RpcContext *rpc,
                   const char *name, BucketID bucket_id);

/**
 *
 */
void DestroyBlobByName(SharedMemoryContext *context, RpcContext *rpc,
                       BucketID bucket_id, const std::string &blob_name);

/**
 *
 */
void RenameBlob(SharedMemoryContext *context, RpcContext *rpc,
                const std::string &old_name, const std::string &new_name,
                BucketID bucket_id);

/**
 *
 */
void RenameBucket(SharedMemoryContext *context, RpcContext *rpc, BucketID id,
                  const std::string &old_name, const std::string &new_name);

/**
 *
 */
bool ContainsBlob(SharedMemoryContext *context, RpcContext *rpc,
                  BucketID bucket_id, const std::string &blob_name);

/**
 *
 */
BufferIdArray GetBufferIdsFromBlobId(Arena *arena,
                                     SharedMemoryContext *context,
                                     RpcContext *rpc, BlobID blob_id,
                                     u32 **sizes);

/**
 *
 */
BlobID GetBlobId(SharedMemoryContext *context, RpcContext *rpc,
                       const std::string &name, BucketID bucket_id);



/**
 *
 */
std::string GetBlobNameFromId(MetadataManager *mdm, RpcContext *rpc,
                              BlobID blob_id);

/**
 *
 */
bool BlobIsInSwap(BlobID id);

/**
 *
 */
BucketID GetOrCreateBucketId(SharedMemoryContext *context, RpcContext *rpc,
                             const std::string &name);

/**
 *
 */
VBucketID GetOrCreateVBucketId(SharedMemoryContext *context, RpcContext *rpc,
                               const std::string &name);

/**
 *
 */
void AttachBlobToBucket(SharedMemoryContext *context, RpcContext *rpc,
                        const char *blob_name, BucketID bucket_id,
                        const std::vector<BufferID> &buffer_ids,
                        bool is_swap_blob = false);

/**
 *
 */
void DecrementRefcount(SharedMemoryContext *context, RpcContext *rpc,
                       BucketID id);

/**
 *
 */
std::vector<BufferID> SwapBlobToVec(SwapBlob swap_blob);

/**
 *
 */
SwapBlob VecToSwapBlob(std::vector<BufferID> &vec);

/**
 *
 */
SwapBlob IdArrayToSwapBlob(BufferIdArray ids);

/**
 *
 */
bool IsBlobNameTooLong(const std::string &name);

/**
 *
 */
bool IsBucketNameTooLong(const std::string &name);

/**
 *
 */
bool IsVBucketNameTooLong(const std::string &name);

/**
 *
 */
TargetID FindTargetIdFromDeviceId(const std::vector<TargetID> &targets,
                                  DeviceID device_id);

/**
 *
 */
std::vector<TargetID> GetNeighborhoodTargets(SharedMemoryContext *context,
                                             RpcContext *rpc);
/**
 *
 */
std::vector<u64>
GetRemainingTargetCapacities(SharedMemoryContext *context, RpcContext *rpc,
                             const std::vector<TargetID> &targets);
/**
 *
 */
std::vector<BlobID> GetBlobIds(SharedMemoryContext *context, RpcContext *rpc,
                               BucketID bucket_id);

/**
 *
 */
BucketID GetBucketId(SharedMemoryContext *context, RpcContext *rpc,
                           const char *name);

/**
 *
 */
BucketID GetBucketIdFromBlobId(SharedMemoryContext *context, RpcContext *rpc,
                               BlobID blob_id);

/**
 *
 */
void DecrementRefcount(SharedMemoryContext *context, RpcContext *rpc,
                       VBucketID id);
/**
 *
 */
bool IsNullBucketId(BucketID id);

/**
 *
 */
bool IsNullVBucketId(VBucketID id);

/**
 *
 */
bool IsNullBlobId(BlobID id);

/**
 *
 */
void BeginGlobalTicketMutex(SharedMemoryContext *context, RpcContext *rpc);

/**
 *
 */
void EndGlobalTicketMutex(SharedMemoryContext *context, RpcContext *rpc);

/**
 *
 */
void LocalBeginGlobalTicketMutex(MetadataManager *mdm);

/**
 *
 */
void LocalEndGlobalTicketMutex(MetadataManager *mdm);

/**
 *
 */
void AttachBlobToVBucket(SharedMemoryContext *context, RpcContext *rpc,
                         const char *blob_name, const char *bucket_name,
                         VBucketID vbucket_id);

/**
 *
 */
void RemoveBlobFromVBucketInfo(SharedMemoryContext *context, RpcContext *rpc,
                               VBucketID vbucket_id, const char *blob_name,
                               const char *bucket_name);

/**
 *
 */
std::vector<BlobID> GetBlobsFromVBucketInfo(SharedMemoryContext *context,
                                            RpcContext *rpc,
                                            VBucketID vbucket_id);

/**
 *
 */
std::string GetBlobNameById(SharedMemoryContext *context, RpcContext *rpc,
                            BlobID id);
/**
 *
 */
std::string GetBucketNameById(SharedMemoryContext *context, RpcContext *rpc,
                              BucketID id);
}  // namespace hermes

#endif  // HERMES_METADATA_MANAGEMENT_H_
