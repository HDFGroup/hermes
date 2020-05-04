#include "metadata_management.h"

#include <string.h>

#include <string>

#include <thallium/serialization/stl/string.hpp>

#define STBDS_REALLOC(heap, ptr, size) hermes::HeapRealloc(heap, ptr, size)
#define STBDS_FREE(heap, ptr) hermes::HeapFree(heap, ptr)

#define STB_DS_IMPLEMENTATION
#include "stb_ds.h"

#include "memory_arena.h"
#include "buffer_pool.h"
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
    case MapType::kBucket: {
      mutex = &mdm->bucket_map_mutex;
      break;
    }
    case MapType::kVBucket: {
      mutex = &mdm->vbucket_map_mutex;
      break;
    }
    case MapType::kBlob: {
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

void LocalPut(MetadataManager *mdm, const char *key, u64 val, MapType map_type){
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


BucketID GetBucketIdByName(SharedMemoryContext *context, const char *name,
                           CommunicationContext *comm, RpcContext *rpc) {
  BucketID result = {};

  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  int node_id = HashString(mdm, comm, name);

  if (node_id == comm->node_id) {
    result.as_int = LocalGet(mdm, name, MapType::kBucket);
  } else {
    result.as_int = rpc->call1("RemoteGet", std::string(name), MapType::kBucket);
  }

  return result;
}

void PutId(MetadataManager *mdm, CommunicationContext *comm,
           RpcContext *rpc, const std::string &name, u64 id, MapType map_type) {
  int node_id = HashString(mdm, comm, name.c_str());

  // TODO(chogan): Check for overlapping heaps here

  if (node_id == comm->node_id) {
    LocalPut(mdm, name.c_str(), id, map_type);
  } else {
    rpc->call2("RemotePut", name, id, map_type);
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

BucketInfo *GetBucketInfoByIndex(MetadataManager *mdm, u32 index) {
  BucketInfo *info_array = (BucketInfo *)((u8 *)mdm + mdm->bucket_info_offset);
  BucketInfo *result = info_array + index;

  return result;
}

// Assumes you're already on the correct node
BucketInfo *GetBucketInfoById(MetadataManager *mdm, BucketID id) {
  BucketInfo *result = GetBucketInfoByIndex(mdm, id.bits.index);

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

  // TODO(chogan): Could replace this with lock-free version if/when it matters
  BeginTicketMutex(&mdm->bucket_mutex);
  BucketID result = mdm->first_free_bucket;
  if (!IsNullBucketId(result)) {
    BucketInfo *info = GetBucketInfoByIndex(mdm, result.bits.index);
    info->blobs = {};
    info->stats = {};
    info->active = true;
    mdm->first_free_bucket = info->next_free;
  }
  EndTicketMutex(&mdm->bucket_mutex);

  // NOTE(chogan): Add metadata entry
  PutBucketId(mdm, comm, rpc, name, result);

  return result;
}

VBucketID GetNextFreeVBucketId(SharedMemoryContext *context,
                               CommunicationContext *comm, RpcContext *rpc,
                               const std::string &name) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);

  // TODO(chogan): Could replace this with lock-free version if/when it matters
  BeginTicketMutex(&mdm->vbucket_mutex);
  VBucketID result = mdm->first_free_vbucket;
  if (!IsNullVBucketId(result)) {
    VBucketInfo *info = GetVBucketInfoByIndex(mdm, result.bits.index);
    info->blobs = {};
    info->stats = {};
    memset(info->traits, 0, sizeof(TraitID) * kMaxTraitsPerVBucket);
    info->active = true;
    mdm->first_free_vbucket = info->next_free;
  }
  EndTicketMutex(&mdm->vbucket_mutex);

  PutVBucketId(mdm, comm, rpc, name, result);

  return result;
}

void CopyBlobIds(BlobID *new_ids, BlobID *old_ids, u32 count) {
  for (u32 i = 0; i < count; ++i) {
    new_ids[i] = old_ids[i];
  } 
}

void AllocateBlobIdList(MetadataManager *mdm, BlobIdList *blobs) {
  Heap *id_heap = GetIdHeap(mdm);
  BeginTicketMutex(&mdm->id_mutex);
  if (blobs->capacity == 0) {
    u8 *ids = HeapPushSize(id_heap, sizeof(BlobID) * kBlobIdListChunkSize);
    if (ids) {
      blobs->capacity = kBlobIdListChunkSize;
      blobs->length = 0;
      blobs->head_offset = GetHeapOffset(id_heap, (u8 *)ids);
    }
  } else {
    // grow capacity by kBlobIdListChunkSize
    BlobID *ids = (BlobID *)HeapOffsetToPtr(id_heap, blobs->head_offset);
    size_t new_capacity = blobs->capacity + kBlobIdListChunkSize;
    BlobID *new_ids = HeapPushArray<BlobID>(id_heap, new_capacity);
    CopyBlobIds(new_ids, ids, blobs->length);
    // HeapFree(id_heap, ids);
    // TODO(chogan): Update any other data?
  }
  EndTicketMutex(&mdm->id_mutex);
}

void LocalAddBlobIdToBucket(MetadataManager *mdm, BucketID bucket_id,
                            BlobID blob_id) {
  // TODO(chogan): Think about lock granularity
  BeginTicketMutex(&mdm->bucket_mutex);
  BucketInfo *info = GetBucketInfoById(mdm, bucket_id);
  BlobIdList *blobs = &info->blobs;

  if (blobs->length >= blobs->capacity) {
    AllocateBlobIdList(mdm, blobs);
  }

  Heap *id_heap = GetIdHeap(mdm);
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
    // TODO(chogan):
    // rpc->call3("RemoteAddBlobIdToBucket", bucket_id, blob_id);
  }
}

u32 AllocateBufferIdList(MetadataManager *mdm,
                         const std::vector<BufferID> &buffer_ids) {
  Heap *heap = GetIdHeap(mdm);
  // TODO(chogan):
  u32 result = 0;

  return result;
}

void AttachBlobToBucket(SharedMemoryContext *context,
                        CommunicationContext *comm, RpcContext *rpc,
                        const char *blob_name, BucketID bucket_id,
                        const std::vector<BufferID> &buffer_ids) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);

  // Get a free BlobID
  BlobID blob_id = {};
  blob_id.bits.node_id = comm->node_id;
  // Associate buffer_ids with the BlobID
  blob_id.bits.buffer_ids_offset = AllocateBufferIdList(mdm, buffer_ids);

  // Update the blob map to associate "name" with the BlobID
  PutBlobId(mdm, comm, rpc, blob_name, blob_id);

  // Add the BlobID to BucketInfo.BlobList
  AddBlobIdToBucket(mdm, comm, rpc, blob_id, bucket_id);
}

void InitMetadataManager(MetadataManager *mdm, Arena *arena, Config *config,
                         int node_id) {

  // NOTE(chogan): All MetadatManager offsets are relative to the address of the
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
