
#define STBDS_REALLOC(heap, ptr, size) hermes::HeapRealloc(heap, ptr, size)
#define STBDS_FREE(heap, ptr) hermes::HeapFree(heap, ptr)

#define STBDS_ASSERT(x) assert((x))

#define STB_DS_IMPLEMENTATION
#include "stb_ds.h"
#include "buffer_pool_internal.h"

namespace hermes {

struct IdMap {
  char *key;
  u64 value;
};

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

/** Convert a key offset into the pointer where the string is stored.
 *
 * Even though our maps are char* -> u64, the keys are not actually pointers to
 * strings, but rather offsets into the shared memory Heap where the strings are
 * stored. This function converts the key at @p index from an offset to a char*.
 * This produces the equivalent of:
 *   char *result = map[index].key; 
 */
static char *GetKey(MetadataManager *mdm, IdMap *map, u32 index) {
  u32 key_offset = (u64)map[index].key;
  Heap *map_heap = GetMapHeap(mdm);
  char *result = (char *)HeapOffsetToPtr(map_heap, key_offset);

  return result;
}

void AllocateOrGrowBlobIdList(MetadataManager *mdm, ChunkedIdList *blobs) {
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
  ChunkedIdList *blobs = &info->blobs;

  if (blobs->length >= blobs->capacity) {
    AllocateOrGrowBlobIdList(mdm, blobs);
  }

  BlobID *head = (BlobID *)HeapOffsetToPtr(id_heap, blobs->head_offset);
  head[blobs->length++] = blob_id;
  EndTicketMutex(&mdm->bucket_mutex);

  CheckHeapOverlap(mdm);
}

IdList *AllocateIdList(MetadataManager *mdm, u32 length) {
  static_assert(sizeof(IdList) == sizeof(u64));
  Heap *id_heap = GetIdHeap(mdm);
  // NOTE(chogan): Add 1 extra for the embedded BufferIdList
  u64 *id_list_memory = HeapPushArray<u64>(id_heap, length + 1);
  IdList *result = (IdList *)id_list_memory;
  result->length = length;
  result->head_offset = GetHeapOffset(id_heap, (u8 *)(result + 1));
  CheckHeapOverlap(mdm);

  return result;
}

u64 *GetIdsPtr(MetadataManager *mdm, IdList id_list) {
  Heap *id_heap = GetIdHeap(mdm);
  u64 *result = (u64 *)HeapOffsetToPtr(id_heap, id_list.head_offset);

  return result;
}

u32 LocalAllocateBufferIdList(MetadataManager *mdm,
                              const std::vector<BufferID> &buffer_ids) {
  static_assert(sizeof(IdList) == sizeof(BufferID));
  u32 length = (u32)buffer_ids.size();
  IdList *id_list = AllocateIdList(mdm, length);
  CopyIds(GetIdsPtr(mdm, *id_list), (u64 *)buffer_ids.data(), length);

  Heap *id_heap = GetIdHeap(mdm);
  u32 result = GetHeapOffset(id_heap, (u8 *)id_list);

  return result;
}

std::vector<BufferID> LocalGetBufferIdList(MetadataManager *mdm,
                                           BlobID blob_id) {
  Heap *id_heap = GetIdHeap(mdm);
  IdList *id_list =
    (IdList *)HeapOffsetToPtr(id_heap, blob_id.bits.buffer_ids_offset);
  BufferID *ids = (BufferID *)HeapOffsetToPtr(id_heap, id_list->head_offset);
  std::vector<BufferID> result(id_list->length);
  CopyIds((u64 *)result.data(), (u64 *)ids, id_list->length);

  return result;
}

void LocalGetBufferIdList(Arena *arena, MetadataManager *mdm, BlobID blob_id,
                          BufferIdArray *buffer_ids) {
  Heap *id_heap = GetIdHeap(mdm);
  IdList *id_list =
    (IdList *)HeapOffsetToPtr(id_heap, blob_id.bits.buffer_ids_offset);
  BufferID *ids = (BufferID *)HeapOffsetToPtr(id_heap, id_list->head_offset);
  buffer_ids->ids = PushArray<BufferID>(arena, id_list->length);
  buffer_ids->length = id_list->length;
  CopyIds((u64 *)buffer_ids->ids, (u64 *)ids, id_list->length);
}

void LocalFreeBufferIdList(SharedMemoryContext *context, BlobID blob_id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  Heap *id_heap = GetIdHeap(mdm);
  u8 *to_free = HeapOffsetToPtr(id_heap, blob_id.bits.buffer_ids_offset);

  HeapFree(id_heap, to_free);
  CheckHeapOverlap(mdm);
}

void LocalRemoveBlobFromBucketInfo(SharedMemoryContext *context,
                                   BucketID bucket_id, BlobID blob_id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BucketInfo *info = LocalGetBucketInfoById(mdm, bucket_id);
  ChunkedIdList *blobs = &info->blobs;
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

bool LocalContainsBlob(SharedMemoryContext *context, BucketID bucket_id,
                       BlobID blob_id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BucketInfo *info = LocalGetBucketInfoById(mdm, bucket_id);
  ChunkedIdList *blobs = &info->blobs;
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

std::vector<TargetID> GetNodeTargets(SharedMemoryContext *context) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  Heap *id_heap = GetIdHeap(mdm);
  u64 *targets = (u64 *)HeapOffsetToPtr(id_heap, mdm->node_targets.head_offset);
  u32 length = mdm->node_targets.length;
  std::vector<TargetID> result(length);

  for (u32 i = 0; i < length; ++i) {
    TargetID id = {};
    id.as_int = targets[i];
    result[i] = id;
  }

  return result;
}

void PutToStorage(MetadataManager *mdm, const char *key, u64 val,
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

u64 GetFromStorage(MetadataManager *mdm, const char *key, MapType map_type) {
  Heap *heap = GetMapHeap(mdm);
  TicketMutex *mutex = GetMapMutex(mdm, map_type);

  BeginTicketMutex(mutex);
  IdMap *map = GetMap(mdm, map_type);
  u64 result = shget(map, key, heap);
  EndTicketMutex(mutex);

  return result;
}

char *ReverseGetFromStorage(MetadataManager *mdm, u64 id,
                            MapType map_type) {
  IdMap *map = GetMap(mdm, map_type);
  char *result = 0;

  // TODO(chogan): @optimization This could be more efficient if necessary
  for (size_t i = 0; i < GetStoredMapSize(mdm, map_type); ++i) {
    if (map[i].value == id) {
      result = GetKey(mdm, map, i);
      break;
    }
  }

  return result;
}

void DeleteFromStorage(MetadataManager *mdm, const char *key,
                       MapType map_type) {
  Heap *heap = GetMapHeap(mdm);
  TicketMutex *mutex = GetMapMutex(mdm, map_type);

  BeginTicketMutex(mutex);
  IdMap *map = GetMap(mdm, map_type);
  shdel(map, key, heap);
  EndTicketMutex(mutex);

  // TODO(chogan): Maybe wrap this in a DEBUG only macro?
  CheckHeapOverlap(mdm);
}

size_t GetStoredMapSize(MetadataManager *mdm, MapType map_type) {
  IdMap *map = GetMap(mdm, map_type);
  size_t result = shlen(map);

  return result;
}

u32 HashStringForStorage(MetadataManager *mdm, RpcContext *rpc,
                         const char *str) {
  int result =
    (stbds_hash_string((char *)str, mdm->map_seed) % rpc->num_nodes) + 1;

  return result;
}

void SeedHashForStorage(size_t seed) {
  stbds_rand_seed(seed);
}

void InitSwapSpaceFilename(MetadataManager *mdm, Arena *arena, Config *config) {
  std::string swap_filename_prefix("swap");
  size_t swap_mount_length = config->swap_mount.size();
  bool ends_in_slash = config->swap_mount[swap_mount_length - 1] == '/';
  std::string full_swap_path = (config->swap_mount + (ends_in_slash ? "" : "/")
                                + swap_filename_prefix);
  size_t full_swap_path_size = full_swap_path.size() + 1;

  char *swap_filename_memory = PushArray<char>(arena, full_swap_path_size);
  memcpy(swap_filename_memory, full_swap_path.c_str(), full_swap_path.size());
  swap_filename_memory[full_swap_path.size()] = '\0';
  mdm->swap_filename_prefix_offset =
    GetOffsetFromMdm(mdm, swap_filename_memory);

  const char swap_file_suffix[] = ".hermes";
  char *swap_file_suffix_memory = PushArray<char>(arena,
                                                  sizeof(swap_file_suffix));
  memcpy(swap_file_suffix_memory, swap_file_suffix,
         sizeof(swap_file_suffix));
  mdm->swap_filename_suffix_offset =
    GetOffsetFromMdm(mdm, swap_file_suffix_memory);
}

void InitMetadataStorage(SharedMemoryContext *context, MetadataManager *mdm,
                         Arena *arena, Config *config) {
  InitSwapSpaceFilename(mdm, arena, config);

  // Heaps

  u32 heap_alignment = 8;
  Heap *map_heap = InitHeapInArena(arena, true, heap_alignment);
  mdm->map_heap_offset = GetOffsetFromMdm(mdm, map_heap);

  // NOTE(chogan): This Heap is constructed at the end of the Metadata Arena and
  // will grow towards smaller addresses.
  Heap *id_heap = InitHeapInArena(arena, false, heap_alignment);
  mdm->id_heap_offset = GetOffsetFromMdm(mdm, id_heap);

  // NOTE(chogan): Rank Targets default to one Target per Device
  IdList *node_targets = AllocateIdList(mdm, config->num_devices);
  TargetID *target_ids = (TargetID *)GetIdsPtr(mdm, *node_targets);
  for (u32 i = 0; i < node_targets->length; ++i) {
    Target *target = GetTarget(context, i);
    target_ids[i] = target->id;
  }
  mdm->node_targets = *node_targets;


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
  assert(total_map_capacity > 0);

  // TODO(chogan): Just one map means better size estimate, but it's probably
  // slower because they'll all share a lock.

  IdMap *vbucket_map = 0;
  sh_new_strdup(vbucket_map, config->max_vbuckets_per_node, map_heap);
  shdefault(vbucket_map, 0, map_heap);
  mdm->vbucket_map_offset = GetOffsetFromMdm(mdm, vbucket_map);
  u32 vbucket_map_num_bytes = map_heap->extent - bucket_map_num_bytes;
  total_map_capacity -= vbucket_map_num_bytes;
  assert(total_map_capacity > 0);

  IdMap *blob_map = 0;
  // NOTE(chogan): Each map element requires twice its size for storage.
  size_t blob_map_capacity = total_map_capacity / (2 * sizeof(IdMap));
  sh_new_strdup(blob_map, blob_map_capacity, map_heap);
  shdefault(blob_map, 0, map_heap);
  mdm->blob_map_offset = GetOffsetFromMdm(mdm, blob_map);
}

}  // namespace hermes
