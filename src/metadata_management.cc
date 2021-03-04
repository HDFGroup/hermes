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

#include "metadata_management.h"

#include <string.h>

#include <string>

#include "memory_management.h"
#include "buffer_pool.h"
#include "buffer_pool_internal.h"
#include "rpc.h"
#include "metadata_storage.h"

namespace hermes {

bool IsNameTooLong(const std::string &name, size_t max) {
  bool result = false;
  if (name.size() + 1 >= max) {
    LOG(WARNING) << "Name '" << name << "' exceeds the maximum name size of "
                 << max << " bytes." << std::endl;
    result = true;
  }

  return result;
}

bool IsBlobNameTooLong(const std::string &name) {
  bool result = IsNameTooLong(name, kMaxBlobNameSize);

  return result;
}

bool IsBucketNameTooLong(const std::string &name) {
  bool result = IsNameTooLong(name, kMaxBucketNameSize);

  return result;
}

bool IsVBucketNameTooLong(const std::string &name) {
  bool result = IsNameTooLong(name, kMaxVBucketNameSize);
  return result;
}

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

bool IsNullTargetId(TargetID id) {
  bool result = id.as_int == 0;

  return result;
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

BucketInfo *LocalGetBucketInfoByIndex(MetadataManager *mdm, u32 index) {
  BucketInfo *info_array = (BucketInfo *)((u8 *)mdm + mdm->bucket_info_offset);
  BucketInfo *result = info_array + index;

  return result;
}

BucketInfo *LocalGetBucketInfoById(MetadataManager *mdm, BucketID id) {
  BucketInfo *result = LocalGetBucketInfoByIndex(mdm, id.bits.index);

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

std::vector<BlobID> GetBlobIds(SharedMemoryContext *context, RpcContext *rpc,
                               BucketID bucket_id) {
  std::vector<BlobID> result;
  u32 target_node = bucket_id.bits.node_id;
  if (target_node == rpc->node_id) {
    result = LocalGetBlobIds(context, bucket_id);
  } else {
    result = RpcCall<std::vector<BlobID>>(rpc, target_node, "RemoteGetBlobIds",
                                          bucket_id);
  }

  return result;
}

void PutId(MetadataManager *mdm, RpcContext *rpc, const std::string &name,
           u64 id, MapType map_type) {
  u32 target_node = HashString(mdm, rpc, name.c_str());
  if (target_node == rpc->node_id) {
    LocalPut(mdm, name.c_str(), id, map_type);
  } else {
    RpcCall<bool>(rpc, target_node, "RemotePut", name, id, map_type);
  }
}

void DeleteId(MetadataManager *mdm, RpcContext *rpc, const std::string &name,
              MapType map_type) {
  u32 target_node = HashString(mdm, rpc, name.c_str());

  if (target_node == rpc->node_id) {
    LocalDelete(mdm, name.c_str(), map_type);
  } else {
    RpcCall<bool>(rpc, target_node, "RemoteDelete", name, map_type);
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
  if (!IsNullVBucketId(result)) {
    PutVBucketId(mdm, rpc, name, result);
  }

  return result;
}

VBucketID GetOrCreateVBucketId(SharedMemoryContext *context, RpcContext *rpc,
                               const std::string &name) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);

  BeginTicketMutex(&mdm->vbucket_mutex);
  VBucketID result = GetVBucketIdByName(context, rpc, name.c_str());

  if (result.as_int != 0) {
    LOG(INFO) << "Opening VBucket '" << name << "'" << std::endl;
    IncrementRefcount(context, rpc, result);
  } else {
    LOG(INFO) << "Creating VBucket '" << name << "'" << std::endl;
    result = GetNextFreeVBucketId(context, rpc, name);
  }
  EndTicketMutex(&mdm->vbucket_mutex);
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
    RpcCall<bool>(rpc, target_node, "RemoteAddBlobIdToBucket", bucket_id,
                  blob_id);
  }
}

void AddBlobIdToVBucket(MetadataManager *mdm, RpcContext *rpc, BlobID blob_id,
                        VBucketID vbucket_id) {
  u32 target_node = vbucket_id.bits.node_id;

  if (target_node == rpc->node_id) {
    LocalAddBlobIdToVBucket(mdm, vbucket_id, blob_id);
  } else {
    RpcCall<bool>(rpc, target_node, "RemoteAddBlobIdToVBucket", vbucket_id,
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

BufferIdArray GetBufferIdsFromBlobId(Arena *arena,
                                     SharedMemoryContext *context,
                                     RpcContext *rpc, BlobID blob_id,
                                     u32 **sizes) {
  BufferIdArray result = {};
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

BufferIdArray GetBufferIdsFromBlobName(Arena *arena,
                                       SharedMemoryContext *context,
                                       RpcContext *rpc,
                                       const char *blob_name,
                                       u32 **sizes) {
  BlobID blob_id = GetBlobIdByName(context, rpc, blob_name);
  BufferIdArray result = GetBufferIdsFromBlobId(arena, context, rpc, blob_id,
                                                sizes);

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

void AttachBlobToVBucket(SharedMemoryContext *context, RpcContext *rpc,
                        const char *blob_name, VBucketID vbucket_id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);

  BlobID blob_id = GetBlobIdByName(context, rpc, blob_name);
  AddBlobIdToVBucket(mdm, rpc, blob_id, vbucket_id);
}

void FreeBufferIdList(SharedMemoryContext *context, RpcContext *rpc,
                      BlobID blob_id) {
  u32 target_node = GetBlobNodeId(blob_id);
  if (target_node == rpc->node_id) {
    LocalFreeBufferIdList(context, blob_id);
  } else {
    RpcCall<bool>(rpc, target_node, "RemoteFreeBufferIdList", blob_id);
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

std::string LocalGetBlobNameById(SharedMemoryContext *context, BlobID blob_id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  std::string blob_name =
      ReverseGetFromStorage(mdm, blob_id.as_int, kMapType_Blob);
  return blob_name;
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
  std::string blob_name = ReverseGetFromStorage(mdm, blob_id.as_int,
                                                kMapType_Blob);

  if (blob_name.size() > 0) {
    DeleteId(mdm, rpc, blob_name.c_str(), kMapType_Blob);
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
    RpcCall<bool>(rpc, target_node, "RemoteRemoveBlobFromBucketInfo", bucket_id,
                  blob_id);
  }
}

std::vector<BlobID> GetBlobsFromVBucketInfo(SharedMemoryContext *context,
                                            RpcContext *rpc,
                                            VBucketID vbucket_id) {
  u32 target_node = vbucket_id.bits.node_id;
  if (target_node == rpc->node_id) {
    return LocalGetBlobsFromVBucketInfo(context, vbucket_id);
  } else {
    return RpcCall<std::vector<BlobID>>(
        rpc, target_node, "RemoteGetBlobsFromVBucketInfo", vbucket_id);
  }
}

void RemoveBlobFromVBucketInfo(SharedMemoryContext *context, RpcContext *rpc,
                               VBucketID vbucket_id, const char* blob_name) {
  BlobID blob_id = GetBlobIdByName(context, rpc, blob_name);
  u32 target_node = vbucket_id.bits.node_id;
  if (target_node == rpc->node_id) {
    LocalRemoveBlobFromVBucketInfo(context, vbucket_id, blob_id);
  } else {
    RpcCall<bool>(rpc, target_node, "RemoteRemoveBlobFromVBucketInfo",
                  vbucket_id, blob_id);
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
      RpcCall<bool>(rpc, blob_id_target_node, "RemoteDestroyBlobByName",
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
  } else {
    // TODO(chogan): @errorhandling
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

std::string GetBlobNameById(SharedMemoryContext *context, RpcContext *rpc,
                            BlobID id) {
  u32 target_node = GetBlobNodeId(id);
  if (target_node == rpc->node_id) {
    return LocalGetBlobNameById(context, id);
  } else {
    return RpcCall<std::string>(rpc, target_node, "RemoteGetBlobNameById", id);
  }
}

void DestroyBlobById(SharedMemoryContext *context, RpcContext *rpc, BlobID id) {
  u32 target_node = GetBlobNodeId(id);
  if (target_node == rpc->node_id) {
    LocalDestroyBlobById(context, rpc, id);
  } else {
    RpcCall<bool>(rpc, target_node, "RemoteDestroyBlobById", id);
  }
}

bool DestroyBucket(SharedMemoryContext *context, RpcContext *rpc,
                   const char *name, BucketID bucket_id) {
  u32 target_node = bucket_id.bits.node_id;
  bool destroyed = false;
  if (target_node == rpc->node_id) {
    destroyed = LocalDestroyBucket(context, rpc, name, bucket_id);
  } else {
    destroyed = RpcCall<bool>(rpc, target_node, "RemoteDestroyBucket",
                              std::string(name), bucket_id);
  }

  return destroyed;
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
    RpcCall<bool>(rpc, target_node, "RemoteRenameBucket", id, old_name,
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
    RpcCall<bool>(rpc, target_node, "RemoteIncrementRefcount", id);
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
    RpcCall<bool>(rpc, target_node, "RemoteDecrementRefcount", id);
  }
}

u64 LocalGetRemainingCapacity(SharedMemoryContext *context, TargetID id) {
  Target *target = GetTargetFromId(context, id);
  u64 result = target->remaining_space.load();

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
      RpcCall<bool>(rpc, target_node, "RemoteUpdateGlobalSystemViewState",
                    adjustments);
    }
  }
}

TargetID FindTargetIdFromDeviceId(const std::vector<TargetID> &targets,
                                  DeviceID device_id) {
  TargetID result = {};
  // TODO(chogan): @optimization Inefficient O(n)
  for (size_t target_index = 0;
       target_index < targets.size();
       ++target_index) {
    if (targets[target_index].bits.device_id == device_id) {
      result = targets[target_index];
      break;
    }
  }

  return result;
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
  // TODO(chogan): @metaprogramming Improve this, since it currently only works
  // if each member in SwapBlob (plus padding) is eight bytes.
  static_assert((sizeof(SwapBlob) / 8) == SwapBlobMembers_Count);

  std::vector<BufferID> result(SwapBlobMembers_Count);
  result[SwapBlobMembers_NodeId].as_int = swap_blob.node_id;
  result[SwapBlobMembers_Offset].as_int = swap_blob.offset;
  result[SwapBlobMembers_Size].as_int = swap_blob.size;
  result[SwapBlobMembers_BucketId].as_int = swap_blob.bucket_id.as_int;

  return result;
}

SwapBlob VecToSwapBlob(std::vector<BufferID> &vec) {
  SwapBlob result = {};

  if (vec.size() == SwapBlobMembers_Count) {
    result.node_id = (u32)vec[SwapBlobMembers_NodeId].as_int;
    result.offset = vec[SwapBlobMembers_Offset].as_int;
    result.size = vec[SwapBlobMembers_Size].as_int;
    result.bucket_id.as_int = vec[SwapBlobMembers_BucketId].as_int;
  } else {
    // TODO(chogan): @errorhandling
    HERMES_NOT_IMPLEMENTED_YET;
  }

  return result;
}

SwapBlob IdArrayToSwapBlob(BufferIdArray ids) {
  SwapBlob result = {};

  if (ids.length == SwapBlobMembers_Count) {
    result.node_id = (u32)ids.ids[SwapBlobMembers_NodeId].as_int;
    result.offset = ids.ids[SwapBlobMembers_Offset].as_int;
    result.size = ids.ids[SwapBlobMembers_Size].as_int;
    result.bucket_id.as_int = ids.ids[SwapBlobMembers_BucketId].as_int;
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

VBucketInfo *LocalGetVBucketInfoByIndex(MetadataManager *mdm, u32 index) {
  VBucketInfo *info_array =
      (VBucketInfo *)((u8 *)mdm + mdm->vbucket_info_offset);
  VBucketInfo *result = info_array + index;
  return result;
}

VBucketInfo *LocalGetVBucketInfoById(MetadataManager *mdm, VBucketID id) {
  VBucketInfo *result = LocalGetVBucketInfoByIndex(mdm, id.bits.index);
  return result;
}

void LocalIncrementRefcount(SharedMemoryContext *context, VBucketID id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  VBucketInfo *info = LocalGetVBucketInfoById(mdm, id);
  info->ref_count.fetch_add(1);
}

void IncrementRefcount(SharedMemoryContext *context, RpcContext *rpc,
                       VBucketID id) {
  u32 target_node = id.bits.node_id;
  if (target_node == rpc->node_id) {
    LocalIncrementRefcount(context, id);
  } else {
    RpcCall<bool>(rpc, target_node, "RemoteIncrementRefcountVBucket", id);
  }
}

void LocalDecrementRefcount(SharedMemoryContext *context, VBucketID id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  VBucketInfo *info = LocalGetVBucketInfoById(mdm, id);
  info->ref_count.fetch_sub(1);
  assert(info->ref_count.load() >= 0);
}

void DecrementRefcount(SharedMemoryContext *context, RpcContext *rpc,
                       VBucketID id) {
  u32 target_node = id.bits.node_id;
  if (target_node == rpc->node_id) {
    LocalDecrementRefcount(context, id);
  } else {
    RpcCall<bool>(rpc, target_node, "RemoteDecrementRefcountVBucket", id);
  }
}

u32 GetRelativeNodeId(RpcContext *rpc, int offset) {
  int result = rpc->node_id + offset;
  assert(result >= 0);
  assert(result <= (int)(rpc->num_nodes + 1));

  if (result > (int)rpc->num_nodes) {
    result = 1;
  } else if (result == 0) {
    result = rpc->num_nodes;
  }

  return (u32)result;
}

u32 GetNextNode(RpcContext *rpc) {
  u32 result = GetRelativeNodeId(rpc, 1);

  return result;
}

u32 GetPreviousNode(RpcContext *rpc) {
  u32 result = GetRelativeNodeId(rpc, -1);

  return result;
}

std::vector<TargetID> GetNodeTargets(SharedMemoryContext *context,
                                     RpcContext *rpc, u32 target_node) {
  std::vector<TargetID> result;

  if (target_node == rpc->node_id) {
    result = LocalGetNodeTargets(context);
  } else {
    result = RpcCall<std::vector<TargetID>>(rpc, target_node,
                                            "RemoteGetNodeTargets");
  }

  return result;
}

std::vector<TargetID> GetNeighborhoodTargets(SharedMemoryContext *context,
                                             RpcContext *rpc) {
  // TODO(chogan): Inform the concept of "neighborhood" with a network topology
  // NOTE(chogan): For now, each node has 2 neighbors, NodeID-1 and NodeID+1
  // (wrapping around for nodes 1 and N).
  std::vector<TargetID> result;

  switch (rpc->num_nodes) {
    case 1: {
      // No neighbors
      break;
    }
    case 2: {
      // One neighbor
      u32 next_node = GetNextNode(rpc);
      result = GetNodeTargets(context, rpc, next_node);
      break;
    }
    default: {
      // Two neighbors
      u32 next_node = GetNextNode(rpc);
      std::vector<TargetID> next_targets = GetNodeTargets(context, rpc,
                                                          next_node);
      u32 prev_node = GetPreviousNode(rpc);
      std::vector<TargetID> prev_targets = GetNodeTargets(context, rpc,
                                                          prev_node);

      result.reserve(next_targets.size() + prev_targets.size());
      result.insert(result.end(), next_targets.begin(), next_targets.end());
      result.insert(result.end(), prev_targets.begin(), prev_targets.end());
    }
  }

  return result;
}

}  // namespace hermes
