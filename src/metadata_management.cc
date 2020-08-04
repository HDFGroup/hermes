#include "metadata_management.h"

#include <string.h>

#include <string>

#include <thallium/serialization/stl/string.hpp>

#define STBDS_REALLOC(heap, ptr, size) hermes::HeapRealloc(heap, ptr, size)
#define STBDS_FREE(heap, ptr) hermes::HeapFree(heap, ptr)

#define STBDS_ASSERT(x) assert((x))

#include "memory_management.h"

#define STB_DS_IMPLEMENTATION
#include "stb_ds.h"

#include "buffer_pool.h"
#include "buffer_pool_internal.h"
#include "rpc.h"

namespace tl = thallium;

namespace hermes {

bool IsNullBucketId(BucketID id) {
  bool result = id.as_int == 0;

  return result;
}

bool IsNullVBucketId(VBucketID id) {
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

static IdMap *GetMapByOffset(MetadataManager *mdm, u32 offset) {
  IdMap *result =(IdMap *)((u8 *)mdm + offset);

  return result;
}

static IdMap *GetBucketMap(MetadataManager *mdm) {
  IdMap *result = GetMapByOffset(mdm, mdm->bucket_map_offset);

  return result;
}

static IdMap *GetVBucketMap(MetadataManager *mdm) {
  IdMap *result = GetMapByOffset(mdm, mdm->vbucket_map_offset);

  return result;
}

static IdMap *GetBlobMap(MetadataManager *mdm) {
  IdMap *result = GetMapByOffset(mdm, mdm->blob_map_offset);

  return result;
}

Heap *GetMapHeap(MetadataManager *mdm) {
  Heap *result = (Heap *)((u8 *)mdm + mdm->map_heap_offset);

  return result;
}

Heap *GetIdHeap(MetadataManager *mdm) {
  Heap *result = (Heap *)((u8 *)mdm + mdm->id_heap_offset);

  return result;
}

static char *GetKey(MetadataManager *mdm, IdMap *map, u32 index) {
  u32 key_offset = (u64)map[index].key;
  Heap *map_heap = GetMapHeap(mdm);
  char *result = (char *)HeapOffsetToPtr(map_heap, key_offset);

  return result;
}

void CheckHeapOverlap(MetadataManager *mdm) {
  Heap *map_heap = GetMapHeap(mdm);
  Heap *id_heap = GetIdHeap(mdm);

  u8 *map_heap_end = HeapExtentToPtr(map_heap);
  u8 *id_heap_start = HeapExtentToPtr(id_heap);

  if (map_heap_end >= id_heap_start) {
    LOG(FATAL) << "Metadata Heaps have overlapped. Please increase "
               << "metadata_arena_percentage in Hermes configuration."
               << std::endl;
  }
}

IdMap *GetMap(MetadataManager *mdm, MapType map_type) {
  IdMap *result = 0;
  switch (map_type) {
    case kMapType_Bucket: {
      result = GetBucketMap(mdm);
      break;
    }
    case kMapType_VBucket: {
      result = GetVBucketMap(mdm);
      break;
    }
    case kMapType_Blob: {
      result = GetBlobMap(mdm);
      break;
    }
  }

  return result;
}

void LocalPut(MetadataManager *mdm, const char *key, u64 val,
              MapType map_type) {
  Heap *heap = GetMapHeap(mdm);
  TicketMutex *mutex = GetMapMutex(mdm, map_type);

  BeginTicketMutex(mutex);
  IdMap *map = GetMap(mdm, map_type);
  shput(map, key, val, heap);
  EndTicketMutex(mutex);

  // TODO(chogan): Maybe wrap this in a DEBUG only macro?
  CheckHeapOverlap(mdm);
}

u64 LocalGet(MetadataManager *mdm, const char *key, MapType map_type) {
  Heap *heap = GetMapHeap(mdm);
  TicketMutex *mutex = GetMapMutex(mdm, map_type);

  BeginTicketMutex(mutex);
  IdMap *map = GetMap(mdm, map_type);
  u64 result = shget(map, key, heap);
  EndTicketMutex(mutex);

  return result;
}

void LocalDelete(MetadataManager *mdm, const char *key, MapType map_type) {
  Heap *heap = GetMapHeap(mdm);
  TicketMutex *mutex = GetMapMutex(mdm, map_type);

  BeginTicketMutex(mutex);
  IdMap *map = GetMap(mdm, map_type);
  shdel(map, key, heap);
  EndTicketMutex(mutex);

  // TODO(chogan): Maybe wrap this in a DEBUG only macro?
  CheckHeapOverlap(mdm);
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
  int result =
    (stbds_hash_string((char *)str, mdm->map_seed) % rpc->num_nodes) + 1;

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

void AllocateOrGrowBlobIdList(MetadataManager *mdm, BlobIdList *blobs) {
  Heap *id_heap = GetIdHeap(mdm);
  BeginTicketMutex(&mdm->id_mutex);
  if (blobs->capacity == 0) {
    u8 *ids = HeapPushSize(id_heap, sizeof(BlobID) * kIdListChunkSize);
    if (ids) {
      blobs->capacity = kIdListChunkSize;
      blobs->length = 0;
      blobs->head_offset = GetHeapOffset(id_heap, (u8 *)ids);
    }
  } else {
    // NOTE(chogan): grow capacity by kIdListChunkSize IDs
    BlobID *ids = (BlobID *)HeapOffsetToPtr(id_heap, blobs->head_offset);
    size_t new_capacity = blobs->capacity + kIdListChunkSize;
    BlobID *new_ids = HeapPushArray<BlobID>(id_heap, new_capacity);
    CopyIds((u64 *)new_ids, (u64 *)ids, blobs->length);
    HeapFree(id_heap, ids);

    blobs->capacity += kIdListChunkSize;
    blobs->head_offset = GetHeapOffset(id_heap, (u8 *)new_ids);
  }
  EndTicketMutex(&mdm->id_mutex);

  CheckHeapOverlap(mdm);
}

void LocalAddBlobIdToBucket(MetadataManager *mdm, BucketID bucket_id,
                            BlobID blob_id) {
  Heap *id_heap = GetIdHeap(mdm);

  // TODO(chogan): Think about lock granularity
  BeginTicketMutex(&mdm->bucket_mutex);
  BucketInfo *info = LocalGetBucketInfoById(mdm, bucket_id);
  BlobIdList *blobs = &info->blobs;

  if (blobs->length >= blobs->capacity) {
    AllocateOrGrowBlobIdList(mdm, blobs);
  }

  BlobID *head = (BlobID *)HeapOffsetToPtr(id_heap, blobs->head_offset);
  head[blobs->length++] = blob_id;
  EndTicketMutex(&mdm->bucket_mutex);

  CheckHeapOverlap(mdm);
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

u32 LocalAllocateBufferIdList(MetadataManager *mdm,
                         const std::vector<BufferID> &buffer_ids) {
  static_assert(sizeof(BufferIdList) == sizeof(BufferID));
  Heap *id_heap = GetIdHeap(mdm);
  u32 length = (u32)buffer_ids.size();
  // NOTE(chogan): Add 1 extra for the embedded BufferIdList
  BufferID *id_list_memory = HeapPushArray<BufferID>(id_heap, length + 1);
  BufferIdList *id_list = (BufferIdList *)id_list_memory;
  id_list->length = length;
  id_list->head_offset = GetHeapOffset(id_heap, (u8 *)(id_list + 1));
  CopyIds((u64 *)(id_list + 1), (u64 *)buffer_ids.data(), length);

  u32 result = GetHeapOffset(id_heap, (u8 *)id_list);

  CheckHeapOverlap(mdm);

  return result;
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

std::vector<BufferID> LocalGetBufferIdList(MetadataManager *mdm,
                                           BlobID blob_id) {
  Heap *id_heap = GetIdHeap(mdm);
  BufferIdList *id_list =
    (BufferIdList *)HeapOffsetToPtr(id_heap, blob_id.bits.buffer_ids_offset);
  BufferID *ids = (BufferID *)HeapOffsetToPtr(id_heap, id_list->head_offset);
  std::vector<BufferID> result(id_list->length);
  CopyIds((u64 *)result.data(), (u64 *)ids, id_list->length);

  return result;
}

void LocalGetBufferIdList(Arena *arena, MetadataManager *mdm, BlobID blob_id,
                          BufferIdArray *buffer_ids) {
  Heap *id_heap = GetIdHeap(mdm);
  BufferIdList *id_list =
    (BufferIdList *)HeapOffsetToPtr(id_heap, blob_id.bits.buffer_ids_offset);
  BufferID *ids = (BufferID *)HeapOffsetToPtr(id_heap, id_list->head_offset);
  buffer_ids->ids = PushArray<BufferID>(arena, id_list->length);
  buffer_ids->length = id_list->length;
  CopyIds((u64 *)buffer_ids->ids, (u64 *)ids, id_list->length);
}

void GetBufferIdList(Arena *arena, SharedMemoryContext *context,
                     RpcContext *rpc, BlobID blob_id,
                     BufferIdArray *buffer_ids) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  u32 target_node = blob_id.bits.node_id;

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
  u32 target_node = blob_id.bits.node_id;

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
                                       const char *blob_name) {
  BufferIdArray result = {};
  BlobID blob_id = GetBlobIdByName(context, rpc, blob_name);
  GetBufferIdList(arena, context, rpc, blob_id, &result);

  return result;
}

void AttachBlobToBucket(SharedMemoryContext *context, RpcContext *rpc,
                        const char *blob_name, BucketID bucket_id,
                        const std::vector<BufferID> &buffer_ids) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);

  u32 target_node = HashString(mdm, rpc, blob_name);
  BlobID blob_id = {};
  blob_id.bits.node_id = target_node;
  blob_id.bits.buffer_ids_offset = AllocateBufferIdList(context, rpc,
                                                        target_node,
                                                        buffer_ids);
  PutBlobId(mdm, rpc, blob_name, blob_id);
  AddBlobIdToBucket(mdm, rpc, blob_id, bucket_id);
}

void LocalFreeBufferIdList(SharedMemoryContext *context, BlobID blob_id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  Heap *id_heap = GetIdHeap(mdm);
  u8 *to_free = HeapOffsetToPtr(id_heap, blob_id.bits.buffer_ids_offset);

  HeapFree(id_heap, to_free);
  CheckHeapOverlap(mdm);
}

void FreeBufferIdList(SharedMemoryContext *context, RpcContext *rpc,
                      BlobID blob_id) {
  u32 target_node = blob_id.bits.node_id;
  if (target_node == rpc->node_id) {
    LocalFreeBufferIdList(context, blob_id);
  } else {
    RpcCall<void>(rpc, target_node, "RemoteFreeBufferIdList", blob_id);
  }
}

void LocalDestroyBlobByName(SharedMemoryContext *context, RpcContext *rpc,
                            const char *blob_name, BlobID blob_id) {

  std::vector<BufferID> buffer_ids = GetBufferIdList(context, rpc, blob_id);
  ReleaseBuffers(context, rpc, buffer_ids);
  FreeBufferIdList(context, rpc, blob_id);

  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  DeleteId(mdm, rpc, blob_name, kMapType_Blob);
}

void LocalDestroyBlobById(SharedMemoryContext *context, RpcContext *rpc,
                          BlobID blob_id) {
  std::vector<BufferID> buffer_ids = GetBufferIdList(context, rpc, blob_id);
  ReleaseBuffers(context, rpc, buffer_ids);
  FreeBufferIdList(context, rpc, blob_id);

  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  IdMap *blob_map = GetBlobMap(mdm);
  char *blob_name = 0;

  // TODO(chogan): @optimization This could be more efficient if necessary
  for (int i = 0; i < shlen(blob_map); ++i) {
    if (blob_map[i].value == blob_id.as_int) {
      blob_name = GetKey(mdm, blob_map, i);
      break;
    }
  }
  if (blob_name) {
    DeleteId(mdm, rpc, blob_name, kMapType_Blob);
  } else {
    // TODO(chogan): @errorhandling
    DLOG(INFO) << "Expected to find blob_id " << blob_id.as_int
               << " in Map but didn't" << std::endl;
  }
}

void LocalRemoveBlobFromBucketInfo(SharedMemoryContext *context,
                                   BucketID bucket_id, BlobID blob_id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BucketInfo *info = LocalGetBucketInfoById(mdm, bucket_id);
  BlobIdList *blobs = &info->blobs;
  Heap *id_heap = GetIdHeap(mdm);

  BeginTicketMutex(&mdm->bucket_mutex);
  BlobID *blobs_arr = (BlobID *)HeapOffsetToPtr(id_heap, blobs->head_offset);
  for (u32 i = 0; i < blobs->length; ++i) {
    if (blobs_arr[i].as_int == blob_id.as_int) {
      blobs_arr[i] = blobs_arr[--blobs->length];
      break;
    }
  }
  EndTicketMutex(&mdm->bucket_mutex);
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

  u32 blob_id_target_node = blob_id.bits.node_id;

  if (blob_id_target_node == rpc->node_id) {
    LocalDestroyBlobByName(context, rpc, blob_name.c_str(), blob_id);
  } else {
    RpcCall<void>(rpc, blob_id_target_node, "RemoteDestroyBlobByName",
                  blob_name, blob_id);
  }

  RemoveBlobFromBucketInfo(context, rpc, bucket_id, blob_id);
}

void RenameBlob(SharedMemoryContext *context, RpcContext *rpc,
                const std::string &old_name, const std::string &new_name) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BlobID blob_id = GetBlobIdByName(context, rpc, old_name.c_str());

  DeleteId(mdm, rpc, old_name, kMapType_Blob);
  PutBlobId(mdm, rpc, new_name, blob_id);
}

bool LocalContainsBlob(SharedMemoryContext *context, BucketID bucket_id,
                       BlobID blob_id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BucketInfo *info = LocalGetBucketInfoById(mdm, bucket_id);
  BlobIdList *blobs = &info->blobs;
  Heap *id_heap = GetIdHeap(mdm);
  BlobID *blob_id_arr = (BlobID *)HeapOffsetToPtr(id_heap, blobs->head_offset);

  bool result = false;
  for (u32 i = 0; i < blobs->length; ++i) {
    if (blob_id_arr[i].as_int == blob_id.as_int) {
      result = true;
      break;
    }
  }

  return result;
}

bool ContainsBlob(SharedMemoryContext *context, RpcContext *rpc,
                  BucketID bucket_id, const std::string &blob_name) {
  BlobID blob_id = GetBlobIdByName(context, rpc, blob_name.c_str());
  bool result = false;

  u32 target_node = bucket_id.bits.node_id;
  if (target_node == rpc->node_id) {
    result = LocalContainsBlob(context, bucket_id, blob_id);
  } else {
    result = RpcCall<bool>(rpc, target_node, "RemoteContainsBlob", bucket_id,
                           blob_name);
  }

  return result;
}

void DestroyBlobById(SharedMemoryContext *context, RpcContext *rpc, BlobID id) {
  u32 target_node = id.bits.node_id;
  if (target_node == rpc->node_id) {
    LocalDestroyBlobById(context, rpc, id);
  } else {
    RpcCall<void>(rpc, target_node, "RemoteDestroyBlobById", id);
  }
}

void LocalDestroyBucket(SharedMemoryContext *context, RpcContext *rpc,
                        const char *bucket_name, BucketID bucket_id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BucketInfo *info = LocalGetBucketInfoById(mdm, bucket_id);
  Heap *id_heap = GetIdHeap(mdm);
  BlobID *blobs = (BlobID *)HeapOffsetToPtr(id_heap, info->blobs.head_offset);

  // TODO(chogan): @optimization Lock granularity can probably be relaxed if
  // this is slow
  BeginTicketMutex(&mdm->bucket_mutex);
  int ref_count = info->ref_count.load();
  if (ref_count == 1) {
    for (u32 i = 0; i < info->blobs.length; ++i) {
      BlobID blob_id = *(blobs + i);
      DestroyBlobById(context, rpc, blob_id);
    }

    // Delete BlobId list
    HeapFree(id_heap, blobs);

    info->blobs.length = 0;
    info->blobs.capacity = 0;
    info->blobs.head_offset = 0;

    // Reset BucketInfo to initial values
    info->ref_count.store(0);
    info->active = false;
    info->stats = {};

    mdm->num_buckets--;
    info->next_free = mdm->first_free_bucket;
    mdm->first_free_bucket = bucket_id;

    // Remove (name -> bucket_id) map entry
    LocalDelete(mdm, bucket_name, kMapType_Bucket);
  }
  EndTicketMutex(&mdm->bucket_mutex);
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

std::vector<u64> LocalGetGlobalTierCapacities(SharedMemoryContext *context) {

  SystemViewState *global_svs = GetGlobalSystemViewState(context);

  std::vector<u64> result(global_svs->num_tiers);
  for (size_t i = 0; i < result.size(); ++i) {
    result[i] = global_svs->bytes_available[i].load();
  }

  return result;
}

std::vector<u64> GetGlobalTierCapacities(SharedMemoryContext *context,
                                         RpcContext *rpc) {

  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  u32 target_node = mdm->global_system_view_state_node_id;

  std::vector<u64> result;

  if (target_node == rpc->node_id) {
    result = LocalGetGlobalTierCapacities(context);
  } else {
    result = RpcCall<std::vector<u64>>(rpc, target_node,
                                       "RemoteGetGlobalTierCapacities");
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
      DLOG(INFO) << "TierID " << i << " adjusted by " << adjustments[i]
                 << " bytes\n";
    }
  }
}

void UpdateGlobalSystemViewState(SharedMemoryContext *context,
                                 RpcContext *rpc) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BufferPool *pool = GetBufferPoolFromContext(context);

  bool update_needed = false;
  std::vector<i64> adjustments(pool->num_tiers);
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
  result->num_tiers = config->num_tiers;
  for (int i = 0; i < result->num_tiers; ++i) {
    result->bytes_available[i] = config->capacities[i];
  }

  return result;
}

void InitMetadataManager(MetadataManager *mdm, Arena *arena, Config *config,
                         int node_id) {

  // NOTE(chogan): All MetadataManager offsets are relative to the address of
  // the MDM itself.

  arena->error_handler = MetadataArenaErrorHandler;

  mdm->map_seed = 0x4E58E5DF;
  stbds_rand_seed(mdm->map_seed);

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

  // Heaps

  u32 heap_alignment = 8;
  Heap *map_heap = InitHeapInArena(arena, true, heap_alignment);
  mdm->map_heap_offset = GetOffsetFromMdm(mdm, map_heap);

  // NOTE(chogan): This Heap is constructed at the end of the Metadata Arena and
  // will grow towards smaller addresses.
  Heap *id_heap = InitHeapInArena(arena, false, heap_alignment);
  mdm->id_heap_offset = GetOffsetFromMdm(mdm, id_heap);

  // ID Maps

  i64 total_map_capacity = GetHeapFreeList(map_heap)->size / 3;

  IdMap *bucket_map = 0;
  // TODO(chogan): We can either calculate an average expected size here, or
  // make HeapRealloc actually use realloc semantics so it can grow as big as
  // needed. But that requires updating offsets for the map and the heap's free
  // list
  sh_new_strdup(bucket_map, config->max_buckets_per_node, map_heap);
  shdefault(bucket_map, 0, map_heap);
  mdm->bucket_map_offset = GetOffsetFromMdm(mdm, bucket_map);
  u32 bucket_map_num_bytes = map_heap->extent;
  total_map_capacity -= bucket_map_num_bytes;

  // TODO(chogan): Just one map means better size estimate, but it's probably
  // slower because they'll all share a lock.

  IdMap *vbucket_map = 0;
  sh_new_strdup(vbucket_map, config->max_vbuckets_per_node, map_heap);
  shdefault(vbucket_map, 0, map_heap);
  mdm->vbucket_map_offset = GetOffsetFromMdm(mdm, vbucket_map);
  u32 vbucket_map_num_bytes = map_heap->extent - bucket_map_num_bytes;
  total_map_capacity -= vbucket_map_num_bytes;

  IdMap *blob_map = 0;
  // NOTE(chogan): Each map element requires twice its size for storage.
  size_t blob_map_capacity = total_map_capacity / (2 * sizeof(IdMap));
  sh_new_strdup(blob_map, blob_map_capacity, map_heap);
  shdefault(blob_map, 0, map_heap);
  mdm->blob_map_offset = GetOffsetFromMdm(mdm, blob_map);
}

}  // namespace hermes
