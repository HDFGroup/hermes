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

IdMap *GetMapByOffset(MetadataManager *mdm, u32 offset) {
  IdMap *result =(IdMap *)((u8 *)mdm + offset);

  return result;
}

IdMap *GetBucketMap(MetadataManager *mdm) {
  IdMap *result = GetMapByOffset(mdm, mdm->bucket_map_offset);

  return result;
}

IdMap *GetVBucketMap(MetadataManager *mdm) {
  IdMap *result = GetMapByOffset(mdm, mdm->bucket_map_offset);

  return result;
}

IdMap *GetBlobMap(MetadataManager *mdm) {
  IdMap *result = GetMapByOffset(mdm, mdm->bucket_map_offset);

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
  // TODO(chogan):
  Heap *heap = NULL;
  TicketMutex *mutex = GetMapMutex(mdm, map_type);

  BeginTicketMutex(mutex);
  IdMap *map = GetMap(mdm, map_type);
  shput(map, key, val, heap);
  EndTicketMutex(mutex);
}

u64 LocalGet(MetadataManager *mdm, const char *key, MapType map_type) {
  // TODO(chogan):
  Heap *heap = NULL;
  TicketMutex *mutex = GetMapMutex(mdm, map_type);

  BeginTicketMutex(mutex);
  IdMap *map = GetMap(mdm, map_type);
  // TODO(chogan): Make sure default map value is 0
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

void PutBucketId(MetadataManager *mdm, CommunicationContext *comm,
                 RpcContext *rpc, const std::string &name, BucketID id) {
  int node_id = HashString(mdm, comm, name.c_str());

  // TODO(chogan): Check for overlapping heaps here

  if (node_id == comm->node_id) {
    LocalPut(mdm, name.c_str(), id.as_int, MapType::kBucket);
  } else {
    rpc->call2("RemotePut", name, id.as_int, MapType::kBucket);
  }
}

BucketInfo *GetBucketInfoByIndex(MetadataManager *mdm, u32 index) {
  BucketInfo *info_array = (BucketInfo *)((u8 *)mdm + mdm->bucket_info_offset);
  BucketInfo *result = info_array + index;

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
    info->blobs_offset = 0;
    info->num_blobs = 0;
    info->stats = {};
    info->active = true;
    mdm->first_free_bucket = info->next_free;
  }
  EndTicketMutex(&mdm->bucket_mutex);

  BeginTicketMutex(&mdm->bucket_map_mutex);
  PutBucketId(mdm, comm, rpc, name, result);
  EndTicketMutex(&mdm->bucket_map_mutex);

  return result;
}

VBucketID GetNextFreeVBucketId(SharedMemoryContext *context) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);

  // TODO(chogan): Could replace this with lock-free version if/when it matters
  BeginTicketMutex(&mdm->vbucket_mutex);
  VBucketID result = mdm->first_free_vbucket;
  if (!IsNullVBucketId(result)) {
    VBucketInfo *info = GetVBucketInfoByIndex(mdm, result.bits.index);
    info->blobs_offset = 0;
    info->num_blobs = 0;
    info->stats = {};
    memset(info->traits, 0, sizeof(TraitID) * kMaxTraitsPerVBucket);
    info->active = true;
    mdm->first_free_vbucket = info->next_free;
  }
  EndTicketMutex(&mdm->vbucket_mutex);

  return result;
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

  IdMap *bucket_map = HeapPushStruct<IdMap>(map_heap);
  mdm->bucket_map_offset = (u8 *)bucket_map - (u8 *)mdm;
  IdMap *vbucket_map = HeapPushStruct<IdMap>(map_heap);
  mdm->vbucket_map_offset = (u8 *)vbucket_map - (u8 *)mdm;
  IdMap *blob_map = HeapPushStruct<IdMap>(map_heap);
  mdm->blob_map_offset = (u8 *)blob_map - (u8 *)mdm;
}

}  // namespace hermes
