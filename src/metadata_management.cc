#include "metadata_management.h"

#include <string.h>

#include <string>

#include "memory_management.h"
#include "buffer_pool.h"
#include "buffer_pool_internal.h"
#include "rpc.h"
#include "metadata_storage.h"

namespace hermes {

bool IsNullBucketId(BucketID id) {
  bool result = id.as_int == 0;

  return result;
}

bool IsNullVBucketId(VBucketID id) {
  bool result = id.as_int == 0;

  return result;
}

bool IsNullBlobId(BlobID id) {
  bool result = id.as_int == 0;

  return result;
}

TicketMutex *GetMapMutex(MetadataManager *mdm, MapType map_type) {
  TicketMutex *mutex = 0;
  switch (map_type) {
    case kMapType_Bucket: {
      mutex = &mdm->bucket_map_mutex;
      break;
    }
    case kMapType_VBucket: {
      mutex = &mdm->vbucket_map_mutex;
      break;
    }
    case kMapType_Blob: {
      mutex = &mdm->blob_map_mutex;
      break;
    }
    default: {
      assert(!"Invalid code path\n");
    }
  }

  return mutex;
}

void LocalPut(MetadataManager *mdm, const char *key, u64 val,
              MapType map_type) {
  PutToStorage(mdm, key, val, map_type);
}

u64 LocalGet(MetadataManager *mdm, const char *key, MapType map_type) {
  u64 result = GetFromStorage(mdm, key, map_type);

  return result;
}

void LocalDelete(MetadataManager *mdm, const char *key, MapType map_type) {
  DeleteFromStorage(mdm, key, map_type);
}

MetadataManager *GetMetadataManagerFromContext(SharedMemoryContext *context) {
  MetadataManager *result =
    (MetadataManager *)(context->shm_base + context->metadata_manager_offset);

  return result;
}

static void MetadataArenaErrorHandler() {
  LOG(FATAL) << "Metadata arena capacity exceeded. Consider increasing the "
             << "value of metadata_arena_percentage in the Hermes configuration"
             << std::endl;
}

u32 HashString(MetadataManager *mdm, RpcContext *rpc, const char *str) {
  u32 result = HashStringForStorage(mdm, rpc, str);

  return result;
}

u64 GetIdByName(SharedMemoryContext *context, RpcContext *rpc, const char *name,
                MapType map_type) {
  u64 result = 0;

  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  u32 target_node = HashString(mdm, rpc, name);

  if (target_node == rpc->node_id) {
    result = LocalGet(mdm, name, map_type);
  } else {
    result = RpcCall<u64>(rpc, target_node, "RemoteGet", std::string(name),
                          map_type);
  }

  return result;
}

BucketID GetBucketIdByName(SharedMemoryContext *context, RpcContext *rpc,
                           const char *name) {
  BucketID result = {};
  result.as_int = GetIdByName(context, rpc, name, kMapType_Bucket);

  return result;
}

VBucketID GetVBucketIdByName(SharedMemoryContext *context, RpcContext *rpc,
                             const char *name) {
  VBucketID result = {};
  result.as_int = GetIdByName(context, rpc, name, kMapType_VBucket);

  return result;
}

BlobID GetBlobIdByName(SharedMemoryContext *context, RpcContext *rpc,
                       const char *name) {
  BlobID result = {};
  result.as_int = GetIdByName(context, rpc, name, kMapType_Blob);

  return result;
}

void PutId(MetadataManager *mdm, RpcContext *rpc, const std::string &name,
           u64 id, MapType map_type) {
  u32 target_node = HashString(mdm, rpc, name.c_str());
  if (target_node == rpc->node_id) {
    LocalPut(mdm, name.c_str(), id, map_type);
  } else {
    RpcCall<void>(rpc, target_node, "RemotePut", name, id, map_type);
  }
}

void DeleteId(MetadataManager *mdm, RpcContext *rpc, const std::string &name,
              MapType map_type) {
  u32 target_node = HashString(mdm, rpc, name.c_str());

  if (target_node == rpc->node_id) {
    LocalDelete(mdm, name.c_str(), map_type);
  } else {
    RpcCall<void>(rpc, target_node, "RemoteDelete", name, map_type);
  }
}

void PutBucketId(MetadataManager *mdm, RpcContext *rpc, const std::string &name,
                 BucketID id) {
  PutId(mdm, rpc, name, id.as_int, kMapType_Bucket);
}

void PutVBucketId(MetadataManager *mdm, RpcContext *rpc,
                  const std::string &name, VBucketID id) {
  PutId(mdm, rpc, name, id.as_int, kMapType_VBucket);
}

void PutBlobId(MetadataManager *mdm, RpcContext *rpc, const std::string &name,
               BlobID id) {
  PutId(mdm, rpc, name, id.as_int, kMapType_Blob);
}

BucketInfo *LocalGetBucketInfoByIndex(MetadataManager *mdm, u32 index) {
  BucketInfo *info_array = (BucketInfo *)((u8 *)mdm + mdm->bucket_info_offset);
  BucketInfo *result = info_array + index;

  return result;
}

BucketInfo *LocalGetBucketInfoById(MetadataManager *mdm, BucketID id) {
  BucketInfo *result = LocalGetBucketInfoByIndex(mdm, id.bits.index);

  return result;
}

VBucketInfo *GetVBucketInfoByIndex(MetadataManager *mdm, u32 index) {
  VBucketInfo *info_array =
    (VBucketInfo *)((u8 *)mdm + mdm->vbucket_info_offset);
  VBucketInfo *result = info_array + index;

  return result;
}

BucketID LocalGetNextFreeBucketId(SharedMemoryContext *context, RpcContext *rpc,
                             const std::string &name) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BucketID result = {};

  if (mdm->num_buckets < mdm->max_buckets) {
    result = mdm->first_free_bucket;
    assert(result.bits.node_id == rpc->node_id);

    if (!IsNullBucketId(result)) {
      BucketInfo *info = LocalGetBucketInfoByIndex(mdm, result.bits.index);
      info->blobs = {};
      info->stats = {};
      info->ref_count.store(1);
      info->active = true;
      mdm->first_free_bucket = info->next_free;
      mdm->num_buckets++;
    }
  } else {
    // TODO(chogan): @errorhandling
    LOG(INFO) << "Exceeded max allowed buckets. "
              << "Increase max_buckets_per_node in the Hermes configuration."
              << std::endl;
  }

  if (!IsNullBucketId(result)) {
    // NOTE(chogan): Add metadata entry
    PutBucketId(mdm, rpc, name, result);
  }

  return result;
}

BucketID GetNextFreeBucketId(SharedMemoryContext *context, RpcContext *rpc,
                             const std::string &name) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  u32 target_node = HashString(mdm, rpc, name.c_str());
  BucketID result = {};

  if (target_node == rpc->node_id) {
    result = LocalGetNextFreeBucketId(context, rpc, name);
  } else {
    result = RpcCall<BucketID>(rpc, target_node, "RemoteGetNextFreeBucketId",
                               name);
  }

  return result;
}

BucketID GetOrCreateBucketId(SharedMemoryContext *context, RpcContext *rpc,
                             const std::string &name) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);

  BeginTicketMutex(&mdm->bucket_mutex);
  BucketID result = GetBucketIdByName(context, rpc, name.c_str());

  if (result.as_int != 0) {
    LOG(INFO) << "Opening Bucket '" << name << "'" << std::endl;
    IncrementRefcount(context, rpc, result);
  } else {
    LOG(INFO) << "Creating Bucket '" << name << "'" << std::endl;
    result = GetNextFreeBucketId(context, rpc, name);
  }
  EndTicketMutex(&mdm->bucket_mutex);

  return result;
}

VBucketID GetNextFreeVBucketId(SharedMemoryContext *context, RpcContext *rpc,
                               const std::string &name) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  VBucketID result = {};

  // TODO(chogan): Could replace this with lock-free version if/when it matters
  BeginTicketMutex(&mdm->vbucket_mutex);
  if (mdm->num_vbuckets < mdm->max_vbuckets) {
    result = mdm->first_free_vbucket;
    if (!IsNullVBucketId(result)) {
      VBucketInfo *info = GetVBucketInfoByIndex(mdm, result.bits.index);
      info->blobs = {};
      info->stats = {};
      memset(info->traits, 0, sizeof(TraitID) * kMaxTraitsPerVBucket);
      info->ref_count.store(1);
      info->active = true;
      mdm->first_free_vbucket = info->next_free;
      mdm->num_vbuckets++;
    }
  } else {
    // TODO(chogan): @errorhandling
    LOG(INFO) << "Exceeded max allowed vbuckets. "
              << "Increase max_vbuckets_per_node in the Hermes configuration."
              << std::endl;
  }
  EndTicketMutex(&mdm->vbucket_mutex);

  if (!IsNullVBucketId(result)) {
    PutVBucketId(mdm, rpc, name, result);
  }

  return result;
}

void CopyIds(u64 *dest, u64 *src, u32 count) {
  static_assert(sizeof(BlobID) == sizeof(BufferID));
  for (u32 i = 0; i < count; ++i) {
    dest[i] = src[i];
  }
}

void AddBlobIdToBucket(MetadataManager *mdm, RpcContext *rpc, BlobID blob_id,
                       BucketID bucket_id) {
  u32 target_node = bucket_id.bits.node_id;

  if (target_node == rpc->node_id) {
    LocalAddBlobIdToBucket(mdm, bucket_id, blob_id);
  } else {
    RpcCall<void>(rpc, target_node, "RemoteAddBlobIdToBucket", bucket_id,
                  blob_id);
  }
}

u32 AllocateBufferIdList(SharedMemoryContext *context, RpcContext *rpc,
                         u32 target_node,
                         const std::vector<BufferID> &buffer_ids) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  u32 result = 0;

  if (target_node == rpc->node_id) {
    result = LocalAllocateBufferIdList(mdm, buffer_ids);
  } else {
    result = RpcCall<u32>(rpc, target_node, "RemoteAllocateBufferIdList",
                          buffer_ids);
  }

  return result;
}

u32 GetBlobNodeId(BlobID id) {
  u32 result = (u32)abs(id.bits.node_id);

  return result;
}

bool BlobIsInSwap(BlobID id) {
  bool result = id.bits.node_id < 0;

  return result;
}

void GetBufferIdList(Arena *arena, SharedMemoryContext *context,
                     RpcContext *rpc, BlobID blob_id,
                     BufferIdArray *buffer_ids) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  u32 target_node = GetBlobNodeId(blob_id);

  if (target_node == rpc->node_id) {
    LocalGetBufferIdList(arena, mdm, blob_id, buffer_ids);
  } else {
    std::vector<BufferID> result =
      RpcCall<std::vector<BufferID>>(rpc, target_node, "RemoteGetBufferIdList",
                                     blob_id);
    buffer_ids->ids = PushArray<BufferID>(arena, result.size());
    buffer_ids->length = (u32)result.size();
    CopyIds((u64 *)buffer_ids->ids, (u64 *)result.data(), result.size());
  }
}

std::vector<BufferID> GetBufferIdList(SharedMemoryContext *context,
                                      RpcContext *rpc, BlobID blob_id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  u32 target_node = GetBlobNodeId(blob_id);

  std::vector<BufferID> result;

  if (target_node == rpc->node_id) {
    result = LocalGetBufferIdList(mdm, blob_id);
  } else {
    result = RpcCall<std::vector<BufferID>>(rpc, target_node,
                                            "RemoteGetBufferIdList", blob_id);
  }

  return result;
}

BufferIdArray GetBufferIdsFromBlobName(Arena *arena,
                                       SharedMemoryContext *context,
                                       RpcContext *rpc,
                                       const char *blob_name,
                                       u32 **sizes) {
  BufferIdArray result = {};
  BlobID blob_id = GetBlobIdByName(context, rpc, blob_name);
  GetBufferIdList(arena, context, rpc, blob_id, &result);

  if (sizes) {
    u32 *buffer_sizes = PushArray<u32>(arena, result.length);
    for (u32 i = 0; i < result.length; ++i) {
      buffer_sizes[i] = GetBufferSize(context, rpc, result.ids[i]);
    }
    *sizes = buffer_sizes;
  }

  return result;
}

void AttachBlobToBucket(SharedMemoryContext *context, RpcContext *rpc,
                        const char *blob_name, BucketID bucket_id,
                        const std::vector<BufferID> &buffer_ids,
                        bool is_swap_blob) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);

  int target_node = HashString(mdm, rpc, blob_name);
  BlobID blob_id = {};
  // NOTE(chogan): A negative node_id indicates a swap blob
  blob_id.bits.node_id = is_swap_blob ? -target_node : target_node;
  blob_id.bits.buffer_ids_offset = AllocateBufferIdList(context, rpc,
                                                        target_node,
                                                        buffer_ids);
  PutBlobId(mdm, rpc, blob_name, blob_id);
  AddBlobIdToBucket(mdm, rpc, blob_id, bucket_id);
}

void FreeBufferIdList(SharedMemoryContext *context, RpcContext *rpc,
                      BlobID blob_id) {
  u32 target_node = GetBlobNodeId(blob_id);
  if (target_node == rpc->node_id) {
    LocalFreeBufferIdList(context, blob_id);
  } else {
    RpcCall<void>(rpc, target_node, "RemoteFreeBufferIdList", blob_id);
  }
}

void LocalDestroyBlobByName(SharedMemoryContext *context, RpcContext *rpc,
                            const char *blob_name, BlobID blob_id) {
  if (!BlobIsInSwap(blob_id)) {
    std::vector<BufferID> buffer_ids = GetBufferIdList(context, rpc, blob_id);
    ReleaseBuffers(context, rpc, buffer_ids);
  } else {
    // TODO(chogan): Invalidate swap region once we have a SwapManager
  }

  FreeBufferIdList(context, rpc, blob_id);

  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  DeleteId(mdm, rpc, blob_name, kMapType_Blob);
}

void LocalDestroyBlobById(SharedMemoryContext *context, RpcContext *rpc,
                          BlobID blob_id) {
  if (!BlobIsInSwap(blob_id)) {
    std::vector<BufferID> buffer_ids = GetBufferIdList(context, rpc, blob_id);
    ReleaseBuffers(context, rpc, buffer_ids);
  } else {
    // TODO(chogan): Invalidate swap region once we have a SwapManager
  }

  FreeBufferIdList(context, rpc, blob_id);

  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  char *blob_name = ReverseGetFromStorage(mdm, blob_id.as_int, kMapType_Blob);

  if (blob_name) {
    DeleteId(mdm, rpc, blob_name, kMapType_Blob);
  } else {
    // TODO(chogan): @errorhandling
    DLOG(INFO) << "Expected to find blob_id " << blob_id.as_int
               << " in Map but didn't" << std::endl;
  }
}

void RemoveBlobFromBucketInfo(SharedMemoryContext *context, RpcContext *rpc,
                              BucketID bucket_id, BlobID blob_id) {
  u32 target_node = bucket_id.bits.node_id;
  if (target_node == rpc->node_id) {
    LocalRemoveBlobFromBucketInfo(context, bucket_id, blob_id);
  } else {
    RpcCall<void>(rpc, target_node, "RemoteRemoveBlobFromBucketInfo", bucket_id,
                  blob_id);
  }
}

void DestroyBlobByName(SharedMemoryContext *context, RpcContext *rpc,
                       BucketID bucket_id, const std::string &blob_name) {
  BlobID blob_id = GetBlobIdByName(context, rpc, blob_name.c_str());
  if (!IsNullBlobId(blob_id)) {
    u32 blob_id_target_node = GetBlobNodeId(blob_id);

    if (blob_id_target_node == rpc->node_id) {
      LocalDestroyBlobByName(context, rpc, blob_name.c_str(), blob_id);
    } else {
      RpcCall<void>(rpc, blob_id_target_node, "RemoteDestroyBlobByName",
                    blob_name, blob_id);
    }
    RemoveBlobFromBucketInfo(context, rpc, bucket_id, blob_id);
  }
}

void RenameBlob(SharedMemoryContext *context, RpcContext *rpc,
                const std::string &old_name, const std::string &new_name) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BlobID blob_id = GetBlobIdByName(context, rpc, old_name.c_str());
  if (!IsNullBlobId(blob_id)) {
    DeleteId(mdm, rpc, old_name, kMapType_Blob);
    PutBlobId(mdm, rpc, new_name, blob_id);
  }
}

bool ContainsBlob(SharedMemoryContext *context, RpcContext *rpc,
                  BucketID bucket_id, const std::string &blob_name) {
  BlobID blob_id = GetBlobIdByName(context, rpc, blob_name.c_str());
  bool result = false;

  if (!IsNullBlobId(blob_id)) {
    u32 target_node = bucket_id.bits.node_id;
    if (target_node == rpc->node_id) {
      result = LocalContainsBlob(context, bucket_id, blob_id);
    } else {
      result = RpcCall<bool>(rpc, target_node, "RemoteContainsBlob", bucket_id,
                             blob_name);
    }
  }

  return result;
}

void DestroyBlobById(SharedMemoryContext *context, RpcContext *rpc, BlobID id) {
  u32 target_node = GetBlobNodeId(id);
  if (target_node == rpc->node_id) {
    LocalDestroyBlobById(context, rpc, id);
  } else {
    RpcCall<void>(rpc, target_node, "RemoteDestroyBlobById", id);
  }
}

void DestroyBucket(SharedMemoryContext *context, RpcContext *rpc,
                   const char *name, BucketID bucket_id) {
  u32 target_node = bucket_id.bits.node_id;
  if (target_node == rpc->node_id) {
    LocalDestroyBucket(context, rpc, name, bucket_id);
  } else {
    RpcCall<void>(rpc, target_node, "RemoteDestroyBucket", std::string(name),
                  bucket_id);
  }
}

void LocalRenameBucket(SharedMemoryContext *context, RpcContext *rpc,
                       BucketID id, const std::string &old_name,
                       const std::string &new_name) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  DeleteId(mdm, rpc, old_name, kMapType_Bucket);
  PutBucketId(mdm, rpc, new_name, id);
}

void RenameBucket(SharedMemoryContext *context, RpcContext *rpc, BucketID id,
                  const std::string &old_name, const std::string &new_name) {
  u32 target_node = id.bits.node_id;
  if (target_node == rpc->node_id) {
    LocalRenameBucket(context, rpc, id, old_name.c_str(), new_name.c_str());
  } else {
    RpcCall<void>(rpc, target_node, "RemoteRenameBucket", id, old_name,
                  new_name);
  }
}

void LocalIncrementRefcount(SharedMemoryContext *context, BucketID id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BucketInfo *info = LocalGetBucketInfoById(mdm, id);
  info->ref_count.fetch_add(1);
}

void IncrementRefcount(SharedMemoryContext *context, RpcContext *rpc,
                       BucketID id) {
  u32 target_node = id.bits.node_id;
  if (target_node == rpc->node_id) {
    LocalIncrementRefcount(context, id);
  } else {
    RpcCall<void>(rpc, target_node, "RemoteIncrementRefcount", id);
  }
}

void LocalDecrementRefcount(SharedMemoryContext *context, BucketID id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BucketInfo *info = LocalGetBucketInfoById(mdm, id);
  info->ref_count.fetch_sub(1);
  assert(info->ref_count.load() >= 0);
}

void DecrementRefcount(SharedMemoryContext *context, RpcContext *rpc,
                       BucketID id) {
  u32 target_node = id.bits.node_id;
  if (target_node == rpc->node_id) {
    LocalDecrementRefcount(context, id);
  } else {
    RpcCall<void>(rpc, target_node, "RemoteDecrementRefcount", id);
  }
}

u64 LocalGetRemainingCapacity(SharedMemoryContext *context, TargetID id) {
  Target *target = GetTargetFromId(context, id);
  u64 result = target->remaining_space.load();

  return result;
}

std::vector<u64> GetRemainingNodeCapacities(SharedMemoryContext *context) {
  std::vector<TargetID> targets = GetNodeTargets(context);
  std::vector<u64> result(targets.size());

  for (size_t i = 0; i < targets.size(); ++i) {
    result[i] = LocalGetRemainingCapacity(context, targets[i]);
  }

  return result;
}

u64 GetRemainingCapacity(SharedMemoryContext *context, RpcContext *rpc,
                         TargetID id) {
  u32 target_node = id.bits.node_id;

  u64 result = 0;
  if (target_node == rpc->node_id) {
    result = LocalGetRemainingCapacity(context, id);
  } else {
    result = RpcCall<u64>(rpc, target_node, "RemoteGetRemainingCapacity", id);
  }

  return result;
}

SystemViewState *GetLocalSystemViewState(MetadataManager *mdm) {
  SystemViewState *result =
    (SystemViewState *)((u8 *)mdm + mdm->system_view_state_offset);

  return result;
}

SystemViewState *GetLocalSystemViewState(SharedMemoryContext *context) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  SystemViewState *result = GetLocalSystemViewState(mdm);

  return result;
}

std::vector<u64> LocalGetGlobalDeviceCapacities(SharedMemoryContext *context) {
  SystemViewState *global_svs = GetGlobalSystemViewState(context);

  std::vector<u64> result(global_svs->num_devices);
  for (size_t i = 0; i < result.size(); ++i) {
    result[i] = global_svs->bytes_available[i].load();
  }

  return result;
}

std::vector<u64> GetGlobalDeviceCapacities(SharedMemoryContext *context,
                                         RpcContext *rpc) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  u32 target_node = mdm->global_system_view_state_node_id;

  std::vector<u64> result;

  if (target_node == rpc->node_id) {
    result = LocalGetGlobalDeviceCapacities(context);
  } else {
    result = RpcCall<std::vector<u64>>(rpc, target_node,
                                       "RemoteGetGlobalDeviceCapacities");
  }

  return result;
}

SystemViewState *GetGlobalSystemViewState(SharedMemoryContext *context) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  SystemViewState *result =
    (SystemViewState *)((u8 *)mdm + mdm->global_system_view_state_offset);
  assert((u8 *)result != (u8 *)mdm);

  return result;
}

void LocalUpdateGlobalSystemViewState(SharedMemoryContext *context,
                                      std::vector<i64> adjustments) {
  for (size_t i = 0; i < adjustments.size(); ++i) {
    SystemViewState *state = GetGlobalSystemViewState(context);
    if (adjustments[i]) {
      state->bytes_available[i].fetch_add(adjustments[i]);
      DLOG(INFO) << "DeviceID " << i << " adjusted by " << adjustments[i]
                 << " bytes\n";
    }
  }
}

void UpdateGlobalSystemViewState(SharedMemoryContext *context,
                                 RpcContext *rpc) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BufferPool *pool = GetBufferPoolFromContext(context);

  bool update_needed = false;
  std::vector<i64> adjustments(pool->num_devices);
  for (size_t i = 0; i < adjustments.size(); ++i) {
    adjustments[i] = pool->capacity_adjustments[i].exchange(0);
    if (adjustments[i] != 0) {
      update_needed = true;
    }
  }

  if (update_needed) {
    u32 target_node = mdm->global_system_view_state_node_id;
    if (target_node == rpc->node_id) {
      LocalUpdateGlobalSystemViewState(context, adjustments);
    } else {
      RpcCall<void>(rpc, target_node, "RemoteUpdateGlobalSystemViewState",
                    adjustments);
    }
  }
}

static ptrdiff_t GetOffsetFromMdm(MetadataManager *mdm, void *ptr) {
  assert((u8 *)ptr >= (u8 *)mdm);
  ptrdiff_t result = (u8 *)ptr - (u8 *)mdm;

  return result;
}

SystemViewState *CreateSystemViewState(Arena *arena, Config *config) {
  SystemViewState *result = PushClearedStruct<SystemViewState>(arena);
  result->num_devices = config->num_devices;
  for (int i = 0; i < result->num_devices; ++i) {
    result->bytes_available[i] = config->capacities[i];
  }

  return result;
}

std::string GetSwapFilename(MetadataManager *mdm, u32 node_id) {
  char *prefix = (char *)((u8 *)mdm + mdm->swap_filename_prefix_offset);
  char *suffix = (char *)((u8 *)mdm + mdm->swap_filename_suffix_offset);
  std::string result = (prefix + std::to_string(node_id) + suffix);

  return result;
}

std::vector<BufferID> SwapBlobToVec(SwapBlob swap_blob) {
  std::vector<BufferID> result(3);
  result[0].as_int = swap_blob.node_id;
  result[1].as_int = swap_blob.offset;
  result[2].as_int = swap_blob.size;

  return result;
}

SwapBlob VecToSwapBlob(std::vector<BufferID> &vec) {
  SwapBlob result = {};
  // TODO(chogan): @metaprogramming count the members of the SwapBlob structure
  if (vec.size() >= 3) {
    result.node_id = (int)vec[0].as_int;
    result.offset = vec[1].as_int;
    result.size = vec[2].as_int;
  } else {
    // TODO(chogan): @errorhandling
    HERMES_NOT_IMPLEMENTED_YET;
  }

  return result;
}

SwapBlob IdArrayToSwapBlob(BufferIdArray ids) {
  SwapBlob result = {};

  // TODO(chogan): @metaprogramming count the members of the SwapBlob structure
  if (ids.length >= 3) {
    result.node_id = (int)ids.ids[0].as_int;
    result.offset = ids.ids[1].as_int;
    result.size = ids.ids[2].as_int;
  } else {
    // TODO(chogan): @errorhandling
    HERMES_NOT_IMPLEMENTED_YET;
  }

  return result;
}

void InitMetadataManager(MetadataManager *mdm, Arena *arena, Config *config,
                         int node_id) {
  // NOTE(chogan): All MetadataManager offsets are relative to the address of
  // the MDM itself.

  arena->error_handler = MetadataArenaErrorHandler;

  mdm->map_seed = 0x4E58E5DF;
  SeedHashForStorage(mdm->map_seed);

  mdm->system_view_state_update_interval_ms =
    config->system_view_state_update_interval_ms;

  // Initialize SystemViewState

  SystemViewState *sv_state = CreateSystemViewState(arena, config);
  mdm->system_view_state_offset = GetOffsetFromMdm(mdm, sv_state);

  // Initialize Global SystemViewState

  if (node_id == 1) {
    // NOTE(chogan): Only Node 1 has the Global SystemViewState
    SystemViewState *global_state = CreateSystemViewState(arena, config);
    mdm->global_system_view_state_offset = GetOffsetFromMdm(mdm, global_state);
  }
  mdm->global_system_view_state_node_id = 1;

  // Initialize BucketInfo array

  BucketInfo *buckets = PushArray<BucketInfo>(arena,
                                              config->max_buckets_per_node);
  mdm->bucket_info_offset = GetOffsetFromMdm(mdm, buckets);
  mdm->first_free_bucket.bits.node_id = (u32)node_id;
  mdm->first_free_bucket.bits.index = 0;
  mdm->num_buckets = 0;
  mdm->max_buckets = config->max_buckets_per_node;

  for (u32 i = 0; i < config->max_buckets_per_node; ++i) {
    BucketInfo *info = buckets + i;
    info->active = false;

    if (i == config->max_buckets_per_node - 1) {
      info->next_free.as_int = 0;
    } else {
      info->next_free.bits.node_id = (u32)node_id;
      info->next_free.bits.index = i + 1;
    }
  }

  // Initialize VBucketInfo array

  VBucketInfo *vbuckets = PushArray<VBucketInfo>(arena,
                                                 config->max_vbuckets_per_node);
  mdm->vbucket_info_offset = GetOffsetFromMdm(mdm, vbuckets);
  mdm->first_free_vbucket.bits.node_id = (u32)node_id;
  mdm->first_free_vbucket.bits.index = 0;
  mdm->num_vbuckets = 0;
  mdm->max_vbuckets = config->max_vbuckets_per_node;

  for (u32 i = 0; i < config->max_vbuckets_per_node; ++i) {
    VBucketInfo *info = vbuckets + i;
    info->active = false;

    if (i == config->max_vbuckets_per_node - 1) {
      info->next_free.as_int = 0;
    } else {
      info->next_free.bits.node_id = (u32)node_id;
      info->next_free.bits.index = i + 1;
    }
  }
}

}  // namespace hermes
