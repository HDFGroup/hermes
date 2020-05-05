#include "metadata_management.h"

#include <string.h>

#include <string>

#include <thallium/serialization/stl/string.hpp>

#define STBDS_REALLOC(heap, ptr, size) hermes::HeapRealloc(heap, ptr, size)
#define STBDS_FREE(heap, ptr) hermes::HeapFree(heap, ptr)

#define STBDS_ASSERT(x) assert((x))

#define STB_DS_IMPLEMENTATION
#include "stb_ds.h"

#include "memory_arena.h"
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
  // TicketMutex *mutex = 0;
  // switch (map_type) {
  //   case MapType::kBucket: {
  //     mutex = &mdm->bucket_map_mutex;
  //     break;
  //   }
  //   case MapType::kVBucket: {
  //     mutex = &mdm->vbucket_map_mutex;
  //     break;
  //   }
  //   case MapType::kBlob: {
  //     mutex = &mdm->blob_map_mutex;
  //     break;
  //   }
  //   default: {
  //     assert(!"Invalid code path\n");
  //   }
  // }
  (void)map_type;
  return &mdm->map_mutex;
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

static Heap *GetMapHeap(MetadataManager *mdm) {
  Heap *result = (Heap *)((u8 *)mdm + mdm->map_heap_offset);

  return result;
}

static Heap *GetIdHeap(MetadataManager *mdm) {
  Heap *result = (Heap *)((u8 *)mdm + mdm->id_heap_offset);

  return result;
}

IdMap *GetMap(MetadataManager *mdm, MapType map_type) {
  IdMap *result = 0;
  switch (map_type) {
    case MapType::kBucket: {
      result = GetBucketMap(mdm);
      break;
    }
    case MapType::kVBucket: {
      result = GetVBucketMap(mdm);
      break;
    }
    case MapType::kBlob: {
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

int HashString(MetadataManager *mdm, CommunicationContext *comm,
               const char *str) {
  int result =
    (stbds_hash_string((char *)str, mdm->map_seed) % comm->num_nodes) + 1;

  return result;
}

u64 GetIdByName(SharedMemoryContext *context, CommunicationContext *comm,
                RpcContext *rpc, const char *name, MapType map_type) {
  u64 result = 0;

  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  int node_id = HashString(mdm, comm, name);

  if (node_id == comm->node_id) {
    result = LocalGet(mdm, name, map_type);
  } else {
    result = RpcCall<u64>(rpc, 0, "RemoteGet", std::string(name), map_type);
  }

  return result;
}

BucketID GetBucketIdByName(SharedMemoryContext *context,
                           CommunicationContext *comm, RpcContext *rpc,
                           const char *name) {
  BucketID result = {};
  result.as_int = GetIdByName(context, comm, rpc, name, MapType::kBucket);

  return result;
}

VBucketID GetVBucketIdByName(SharedMemoryContext *context,
                             CommunicationContext *comm, RpcContext *rpc,
                             const char *name) {
  VBucketID result = {};
  result.as_int = GetIdByName(context, comm, rpc, name, MapType::kVBucket);

  return result;
}

BlobID GetBlobIdByName(SharedMemoryContext *context,
                       CommunicationContext *comm, RpcContext *rpc,
                       const char *name) {
  BlobID result = {};
  result.as_int = GetIdByName(context, comm, rpc, name, MapType::kBlob);

  return result;
}

void PutId(MetadataManager *mdm, CommunicationContext *comm,
           RpcContext *rpc, const std::string &name, u64 id, MapType map_type) {
  int node_id = HashString(mdm, comm, name.c_str());

  // TODO(chogan): Check for overlapping heaps here
  // if (mdm->map_heap_end_offset >= mdm->id_heap_start_offset) {
  //   uh-oh
  // }

  if (node_id == comm->node_id) {
    LocalPut(mdm, name.c_str(), id, map_type);
  } else {
    RpcCall<void>(rpc, 0, "RemotePut", name, id, map_type);
  }
}

void DeleteId(MetadataManager *mdm, CommunicationContext *comm,
              RpcContext *rpc, const std::string &name, MapType map_type) {
  int node_id = HashString(mdm, comm, name.c_str());

  // TODO(chogan): Update mdm->map_heap_end_offset

  if (node_id == comm->node_id) {
    LocalDelete(mdm, name.c_str(), map_type);
  } else {
    RpcCall<void>(rpc, node_id, "RemoteDelete", name, map_type);
  }
}

void PutBucketId(MetadataManager *mdm, CommunicationContext *comm,
                 RpcContext *rpc, const std::string &name, BucketID id) {
  PutId(mdm, comm, rpc, name, id.as_int, MapType::kBucket);
}

void PutVBucketId(MetadataManager *mdm, CommunicationContext *comm,
                  RpcContext *rpc, const std::string &name, VBucketID id) {
  PutId(mdm, comm, rpc, name, id.as_int, MapType::kVBucket);
}

void PutBlobId(MetadataManager *mdm, CommunicationContext *comm,
               RpcContext *rpc, const std::string &name, BlobID id) {
  PutId(mdm, comm, rpc, name, id.as_int, MapType::kBlob);
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

BucketID GetNextFreeBucketId(SharedMemoryContext *context,
                             CommunicationContext *comm, RpcContext *rpc,
                             const std::string &name) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BucketID result = {};

  // TODO(chogan): Could replace this with lock-free version if/when it matters
  BeginTicketMutex(&mdm->bucket_mutex);
  if (mdm->num_buckets < mdm->max_buckets) {
    result = mdm->first_free_bucket;
    assert(result.bits.node_id == (u32)comm->node_id);

    if (!IsNullBucketId(result)) {
      BucketInfo *info = LocalGetBucketInfoByIndex(mdm, result.bits.index);
      info->blobs = {};
      info->stats = {};
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
  EndTicketMutex(&mdm->bucket_mutex);

  if (!IsNullBucketId(result)) {
    // NOTE(chogan): Add metadata entry
    PutBucketId(mdm, comm, rpc, name, result);
  }

  return result;
}

VBucketID GetNextFreeVBucketId(SharedMemoryContext *context,
                               CommunicationContext *comm, RpcContext *rpc,
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
    PutVBucketId(mdm, comm, rpc, name, result);
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
}

void AddBlobIdToBucket(MetadataManager *mdm, CommunicationContext *comm,
                       RpcContext *rpc, BlobID blob_id, BucketID bucket_id) {
  u32 target_node = bucket_id.bits.node_id;

  if (target_node == (u32)comm->node_id) {
    LocalAddBlobIdToBucket(mdm, bucket_id, blob_id);
  } else {
    RpcCall<void>(rpc, 0, "RemoteAddBlobIdToBucket", bucket_id, blob_id);
  }
}

u32 AllocateBufferIdList(MetadataManager *mdm,
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
                     CommunicationContext *comm, RpcContext *rpc,
                     BlobID blob_id, BufferIdArray *buffer_ids) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  u32 target_node = blob_id.bits.node_id;

  if (target_node == (u32)comm->node_id) {
    LocalGetBufferIdList(arena, mdm, blob_id, buffer_ids);
  } else {
    std::vector<BufferID> result =
      RpcCall<std::vector<BufferID>>(rpc, 0, "RemoteGetBufferIdList", blob_id);
    buffer_ids->ids = PushArray<BufferID>(arena, result.size());
    buffer_ids->length = (u32)result.size();
    CopyIds((u64 *)buffer_ids->ids, (u64 *)result.data(), result.size());
  }
}

BufferIdArray GetBufferIdsFromBlobName(Arena *arena,
                                       SharedMemoryContext *context,
                                       CommunicationContext *comm,
                                       RpcContext *rpc,
                                       const char *blob_name) {
  BufferIdArray result = {};
  BlobID blob_id = GetBlobIdByName(context, comm, rpc, blob_name);
  GetBufferIdList(arena, context, comm, rpc, blob_id, &result);

  return result;
}

void AttachBlobToBucket(SharedMemoryContext *context,
                        CommunicationContext *comm, RpcContext *rpc,
                        const char *blob_name, BucketID bucket_id,
                        const std::vector<BufferID> &buffer_ids) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);

  BlobID blob_id = {};
  blob_id.bits.node_id = comm->node_id;
  blob_id.bits.buffer_ids_offset = AllocateBufferIdList(mdm, buffer_ids);
  PutBlobId(mdm, comm, rpc, blob_name, blob_id);
  AddBlobIdToBucket(mdm, comm, rpc, blob_id, bucket_id);
}

void LocalDestroyBucket(SharedMemoryContext *context, const char *bucket_name,
                        BucketID bucket_id, u32 current_node) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BucketInfo *info = LocalGetBucketInfoById(mdm, bucket_id);
  Heap *id_heap = GetIdHeap(mdm);
  BlobID *blobs = (BlobID *)HeapOffsetToPtr(id_heap, info->blobs.head_offset);

  // 1. Iterate through all BlobIDs in this Bucket
  for (u32 i = 0; i < info->blobs.length; ++i) {
    BlobID *blob_id = blobs + i;

    if (blob_id->bits.node_id == current_node) {
      // 2. ReleaseBuffers on each list of BufferIDs
      std::vector<BufferID> buffer_ids = LocalGetBufferIdList(mdm, *blob_id);

      for (size_t j = 0; j < buffer_ids.size(); ++j) {
        if (buffer_ids[j].bits.node_id == current_node) {
          ReleaseBuffer(context, buffer_ids[j]);
        } else {
          // TODO(chogan):
          // RpcCall<>(rpc, 0, "ReleaseBuffer", buffer_ids[j]);
        }
      }
      // Delete BufferID list
      u8 *to_free = HeapOffsetToPtr(id_heap, blob_id->bits.buffer_ids_offset);
      HeapFree(id_heap, to_free);

    } else {
      // TODO(chogan):
      // RpcCall<>(rpc, 0, );
    }
  }

  // Delete BlobId list
  HeapFree(id_heap, blobs);
  info->blobs.length = 0;
  info->blobs.capacity = 0;
  info->blobs.head_offset = 0;

  // Delete BucketInfo
  info->ref_count.store(0);
  info->active = false;
  info->stats = {};

  BeginTicketMutex(&mdm->bucket_mutex);
  mdm->num_buckets--;
  info->next_free = mdm->first_free_bucket;
  mdm->first_free_bucket = bucket_id;
  EndTicketMutex(&mdm->bucket_mutex);

  LocalDelete(mdm, bucket_name, MapType::kBucket);
}

void DestroyBucket(SharedMemoryContext *context, CommunicationContext *comm,
                   RpcContext *rpc, const char *name, BucketID bucket_id) {
  // TODO(chogan): Check refcounts

  // TODO(chogan): Locks

  u32 target_node = bucket_id.bits.node_id;
  if (target_node == (u32)comm->node_id) {
    LocalDestroyBucket(context, name, bucket_id, (u32)comm->node_id);
  } else {
    RpcCall<void>(rpc, target_node, "RemoteDestroyBucket", std::string(name),
                  bucket_id);
  }
}

void InitMetadataManager(MetadataManager *mdm, Arena *arena, Config *config,
                         int node_id) {

  // NOTE(chogan): All MetadataManager offsets are relative to the address of the
  // MDM itself.

  arena->error_handler = MetadataArenaErrorHandler;

  mdm->map_seed = 0x4E58E5DF;
  stbds_rand_seed(mdm->map_seed);

  // Initialize BucketInfo array

  BucketInfo *buckets = PushArray<BucketInfo>(arena,
                                              config->max_buckets_per_node);
  mdm->bucket_info_offset = (u8 *)buckets - (u8 *)mdm;
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
  mdm->vbucket_info_offset = (u8 *)vbuckets - (u8 *)mdm;
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
  mdm->map_heap_offset = (u8 *)map_heap - (u8 *)mdm;

  // NOTE(chogan): This Heap is constructed at the end of the Metadata Arena and
  // will grow towards smaller addresses.
  Heap *id_heap = InitHeapInArena(arena, false, heap_alignment);
  mdm->id_heap_offset = (u8 *)id_heap - (u8 *)mdm;

  // ID Maps

  i64 total_map_capacity = GetHeapFreeList(map_heap)->size / 3;

  IdMap *bucket_map = 0;
  // TODO(chogan): We can either calculate an average expected size here, or
  // make HeapRealloc acutally use realloc semantics so it can grow as big as
  // needed. But that requires updating offsets for the map and the heap's free
  // list
  sh_new_strdup(bucket_map, config->max_buckets_per_node, map_heap);
  shdefault(bucket_map, 0, map_heap);
  mdm->bucket_map_offset = (u8 *)bucket_map - (u8 *)mdm;
  total_map_capacity -= config->max_buckets_per_node * sizeof(IdMap);

  // TODO(chogan): Just one map means better size estimate, but it's probably
  // slower because they'll all share a lock.

  IdMap *vbucket_map = 0;
  sh_new_strdup(vbucket_map, config->max_vbuckets_per_node, map_heap);
  shdefault(vbucket_map, 0, map_heap);
  mdm->vbucket_map_offset = (u8 *)vbucket_map - (u8 *)mdm;
  total_map_capacity -= config->max_buckets_per_node * sizeof(VBucketID);

  IdMap *blob_map = 0;
  size_t blob_map_capacity = total_map_capacity / sizeof(BlobID);
  sh_new_strdup(blob_map, blob_map_capacity, map_heap);
  shdefault(blob_map, 0, map_heap);
  mdm->blob_map_offset = (u8 *)blob_map - (u8 *)mdm;
}

}  // namespace hermes
