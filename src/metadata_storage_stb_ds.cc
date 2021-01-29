/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Distributed under BSD 3-Clause license.                                   *
* Copyright by The HDF Group.                                               *
* Copyright by the Illinois Institute of Technology.                        *
* All rights reserved.                                                      *
*                                                                           *
* This file is part of Hermes. The full Hermes copyright notice, including  *
* terms governing use, modification, and redistribution, is contained in    *
* the COPYFILE, which can be found at the top directory. If you do not have *
* access to either file, you may request a copy from help@hdfgroup.org.     *
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

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
      HERMES_INVALID_CODE_PATH;
    }
  }

  return mutex;
}

/**
 * Get a pointer to an IdMap in shared memory.
 *
 * This function acquires a lock because it's pointing to a shared data
 * structure. Make sure to call `ReleaseMap` when you're finished with the
 * IdMap.
 */
IdMap *GetMap(MetadataManager *mdm, MapType map_type) {
  IdMap *result = 0;
  TicketMutex *mutex = GetMapMutex(mdm, map_type);
  BeginTicketMutex(mutex);

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
    default:
      HERMES_INVALID_CODE_PATH;
  }

  return result;
}

/**
 * Releases the lock acquired by `GetMap`.
 */
void ReleaseMap(MetadataManager *mdm, MapType map_type) {
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
      HERMES_INVALID_CODE_PATH;
    }
  }

  EndTicketMutex(mutex);
}

/**
 * Return a pointer to the the internal array of IDs that the `id_list`
 * represents.
 *
 * T must be an `IdList` or a `ChunkedIdList`. This call acquires a lock, and
 * must be paired with a corresponding call to `ReleaseIdsPtr` to release the
 * lock.
 */
template<typename T>
u64 *GetIdsPtr(MetadataManager *mdm, T id_list) {
  Heap *id_heap = GetIdHeap(mdm);
  BeginTicketMutex(&mdm->id_mutex);
  u64 *result = (u64 *)HeapOffsetToPtr(id_heap, id_list.head_offset);

  return result;
}

/**
 * Returns a copy of an embedded `IdList`.
 *
 * An `IdList` that consists of `BufferID`s contains an embedded `IdList` as the
 * first element of the list. This is so the `BlobID` can find information about
 * its buffers from a single offset. If you want a pointer to the `BufferID`s in
 * an `IdList`, then you have to first retrieve the embedded `IdList` using this
 * function, and then use the resulting `IdList` in `GetIdsPtr`.
 */
IdList GetEmbeddedIdList(MetadataManager *mdm, u32 offset) {
  Heap *id_heap = GetIdHeap(mdm);
  BeginTicketMutex(&mdm->id_mutex);
  IdList *embedded_id_list = (IdList *)HeapOffsetToPtr(id_heap, offset);
  IdList result = *embedded_id_list;
  EndTicketMutex(&mdm->id_mutex);

  return result;
}

/**
 * Return a pointer to the internal array of `BufferID`s associated with the
 * `blob_id`.
 *
 * Acquires a lock, and must be paired with a call to `ReleaseIdsPtr` to release
 * the lock. The length of the array is returned in the `length` parameter.
 */
BufferID *GetBufferIdsPtrFromBlobId(MetadataManager *mdm, BlobID blob_id,
                                    size_t &length) {
  IdList id_list = GetEmbeddedIdList(mdm, blob_id.bits.buffer_ids_offset);
  length = id_list.length;
  BufferID *result = (BufferID *)GetIdsPtr(mdm, id_list);

  return result;
}

void ReleaseIdsPtr(MetadataManager *mdm) {
  EndTicketMutex(&mdm->id_mutex);
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

template<typename T>
void FreeIdList(MetadataManager *mdm, T id_list) {
  Heap *id_heap = GetIdHeap(mdm);
  BeginTicketMutex(&mdm->id_mutex);
  u8 *ptr = HeapOffsetToPtr(id_heap, id_list.head_offset);
  HeapFree(id_heap, ptr);
  EndTicketMutex(&mdm->id_mutex);
}

void FreeEmbeddedIdList(MetadataManager *mdm, u32 offset) {
  Heap *id_heap = GetIdHeap(mdm);
  BeginTicketMutex(&mdm->id_mutex);
  u8 *to_free = HeapOffsetToPtr(id_heap, offset);
  HeapFree(id_heap, to_free);
  EndTicketMutex(&mdm->id_mutex);
}

/**
 * Assumes the caller has protected @p id_list with a lock.
 */
void AllocateOrGrowIdList(MetadataManager *mdm, ChunkedIdList *id_list) {
  Heap *id_heap = GetIdHeap(mdm);
  // NOTE(chogan): grow capacity by kIdListChunkSize IDs
  size_t new_capacity = id_list->capacity + kIdListChunkSize;
  u64 *new_ids = HeapPushArray<u64>(id_heap, new_capacity);

  if (id_list->capacity != 0) {
    // NOTE(chogan): Copy over old ids and then free them
    u64 *ids = GetIdsPtr(mdm, *id_list);
    CopyIds(new_ids, ids, id_list->length);
    ReleaseIdsPtr(mdm);
    FreeIdList(mdm, *id_list);
  } else {
    // NOTE(chogan): This is the initial allocation so initialize length
    id_list->length = 0;
  }

  id_list->capacity = new_capacity;
  id_list->head_offset = GetHeapOffset(id_heap, (u8 *)new_ids);
}

/**
 * Assumes the caller has protected @p id_list with a lock.
 */
void AppendToChunkedIdList(MetadataManager *mdm, ChunkedIdList *id_list,
                           u64 id) {
  if (id_list->length >= id_list->capacity) {
    AllocateOrGrowIdList(mdm, id_list);
  }

  u64 *head = GetIdsPtr(mdm, *id_list);
  head[id_list->length++] = id;
  ReleaseIdsPtr(mdm);
}

void LocalAddBlobIdToBucket(MetadataManager *mdm, BucketID bucket_id,
                            BlobID blob_id) {
  BeginTicketMutex(&mdm->bucket_mutex);
  BucketInfo *info = LocalGetBucketInfoById(mdm, bucket_id);
  AppendToChunkedIdList(mdm, &info->blobs, blob_id.as_int);
  EndTicketMutex(&mdm->bucket_mutex);

  CheckHeapOverlap(mdm);
}

void LocalAddBlobIdToVBucket(MetadataManager *mdm, VBucketID vbucket_id,
                             BlobID blob_id) {
  BeginTicketMutex(&mdm->vbucket_mutex);
  VBucketInfo *info = LocalGetVBucketInfoById(mdm, vbucket_id);
  AppendToChunkedIdList(mdm, &info->blobs, blob_id.as_int);
  EndTicketMutex(&mdm->vbucket_mutex);

  CheckHeapOverlap(mdm);
}

IdList AllocateIdList(MetadataManager *mdm, u32 length) {
  static_assert(sizeof(IdList) == sizeof(u64));
  Heap *id_heap = GetIdHeap(mdm);
  BeginTicketMutex(&mdm->id_mutex);
  u64 *id_list_memory = HeapPushArray<u64>(id_heap, length);
  IdList result = {};
  result.length = length;
  result.head_offset = GetHeapOffset(id_heap, (u8 *)(id_list_memory));
  EndTicketMutex(&mdm->id_mutex);
  CheckHeapOverlap(mdm);

  return result;
}

u32 AllocateEmbeddedIdList(MetadataManager *mdm, u32 length) {
  static_assert(sizeof(IdList) == sizeof(u64));
  Heap *id_heap = GetIdHeap(mdm);
  BeginTicketMutex(&mdm->id_mutex);
  // NOTE(chogan): Add 1 extra for the embedded IdList
  u64 *id_list_memory = HeapPushArray<u64>(id_heap, length + 1);
  IdList *embedded_id_list = (IdList *)id_list_memory;
  embedded_id_list->length = length;
  embedded_id_list->head_offset =
    GetHeapOffset(id_heap, (u8 *)(embedded_id_list + 1));
  u32 result = GetHeapOffset(id_heap, (u8 *)embedded_id_list);
  EndTicketMutex(&mdm->id_mutex);
  CheckHeapOverlap(mdm);

  return result;
}

std::vector<BlobID> LocalGetBlobIds(SharedMemoryContext *context,
                                    BucketID bucket_id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BucketInfo *info = LocalGetBucketInfoById(mdm, bucket_id);
  u32 num_blobs = info->blobs.length;
  std::vector<BlobID> result(num_blobs);

  BlobID *blob_ids = (BlobID *)GetIdsPtr(mdm, info->blobs);
  for (u32 i = 0; i < num_blobs; ++i) {
    result[i] = blob_ids[i];
  }
  ReleaseIdsPtr(mdm);

  return result;
}

u32 LocalAllocateBufferIdList(MetadataManager *mdm,
                              const std::vector<BufferID> &buffer_ids) {
  static_assert(sizeof(IdList) == sizeof(BufferID));
  u32 length = (u32)buffer_ids.size();
  u32 id_list_offset = AllocateEmbeddedIdList(mdm, length);
  IdList id_list = GetEmbeddedIdList(mdm, id_list_offset);
  u64 *ids = (u64 *)GetIdsPtr(mdm, id_list);
  CopyIds(ids, (u64 *)buffer_ids.data(), length);
  ReleaseIdsPtr(mdm);

  u32 result = id_list_offset;

  return result;
}

std::vector<BufferID> LocalGetBufferIdList(MetadataManager *mdm,
                                           BlobID blob_id) {
  size_t length = 0;
  BufferID *ids = GetBufferIdsPtrFromBlobId(mdm, blob_id, length);
  std::vector<BufferID> result(length);
  CopyIds((u64 *)result.data(), (u64 *)ids, length);
  ReleaseIdsPtr(mdm);

  return result;
}

void LocalGetBufferIdList(Arena *arena, MetadataManager *mdm, BlobID blob_id,
                          BufferIdArray *buffer_ids) {
  size_t length = 0;
  BufferID *ids = GetBufferIdsPtrFromBlobId(mdm, blob_id, length);
  buffer_ids->ids = PushArray<BufferID>(arena, length);
  buffer_ids->length = length;
  CopyIds((u64 *)buffer_ids->ids, (u64 *)ids, length);
  ReleaseIdsPtr(mdm);
}

void LocalFreeBufferIdList(SharedMemoryContext *context, BlobID blob_id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  FreeEmbeddedIdList(mdm, blob_id.bits.buffer_ids_offset);
  CheckHeapOverlap(mdm);
}

void LocalRemoveBlobFromBucketInfo(SharedMemoryContext *context,
                                   BucketID bucket_id, BlobID blob_id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BeginTicketMutex(&mdm->bucket_mutex);
  BucketInfo *info = LocalGetBucketInfoById(mdm, bucket_id);
  ChunkedIdList *blobs = &info->blobs;

  BlobID *blobs_arr = (BlobID *)GetIdsPtr(mdm, *blobs);
  for (u32 i = 0; i < blobs->length; ++i) {
    if (blobs_arr[i].as_int == blob_id.as_int) {
      blobs_arr[i] = blobs_arr[--blobs->length];
      break;
    }
  }
  ReleaseIdsPtr(mdm);
  EndTicketMutex(&mdm->bucket_mutex);
}

bool LocalContainsBlob(SharedMemoryContext *context, BucketID bucket_id,
                       BlobID blob_id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BeginTicketMutex(&mdm->bucket_mutex);
  BucketInfo *info = LocalGetBucketInfoById(mdm, bucket_id);
  ChunkedIdList *blobs = &info->blobs;
  BlobID *blob_id_arr = (BlobID *)GetIdsPtr(mdm, *blobs);

  bool result = false;
  for (u32 i = 0; i < blobs->length; ++i) {
    if (blob_id_arr[i].as_int == blob_id.as_int) {
      result = true;
      break;
    }
  }
  ReleaseIdsPtr(mdm);
  EndTicketMutex(&mdm->bucket_mutex);

  return result;
}

static inline bool HasAllocatedBlobs(BucketInfo *info) {
  bool result = info->blobs.capacity > 0;

  return result;
}

bool LocalDestroyBucket(SharedMemoryContext *context, RpcContext *rpc,
                        const char *bucket_name, BucketID bucket_id) {
  bool destroyed = false;
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BeginTicketMutex(&mdm->bucket_mutex);
  BucketInfo *info = LocalGetBucketInfoById(mdm, bucket_id);

  // TODO(chogan): @optimization Lock granularity can probably be relaxed if
  // this is slow
  int ref_count = info->ref_count.load();
  if (ref_count == 1) {
    if (HasAllocatedBlobs(info)) {
      // NOTE(chogan): Collecting the blobs to destroy first and destroying them
      // later avoids locking complications
      std::vector<BlobID> blobs_to_destroy;
      blobs_to_destroy.reserve(info->blobs.length);
      BlobID *blobs = (BlobID *)GetIdsPtr(mdm, info->blobs);
      for (u32 i = 0; i < info->blobs.length; ++i) {
        BlobID blob_id = *(blobs + i);
        blobs_to_destroy.push_back(blob_id);
      }
      ReleaseIdsPtr(mdm);

      for (auto blob_id : blobs_to_destroy) {
        DestroyBlobById(context, rpc, blob_id);
      }
      // Delete BlobId list
      FreeIdList(mdm, info->blobs);
    }

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
    destroyed = true;
  } else {
    LOG(INFO) << "Cannot destroy bucket " << bucket_name
              << ". It's refcount is " << ref_count << std::endl;
  }
  EndTicketMutex(&mdm->bucket_mutex);

  return destroyed;
}

std::vector<TargetID> LocalGetNodeTargets(SharedMemoryContext *context) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  u32 length = mdm->node_targets.length;
  std::vector<TargetID> result(length);

  u64 *target_ids = GetIdsPtr(mdm, mdm->node_targets);
  for (u32 i = 0; i < length; ++i) {
    result[i].as_int = target_ids[i];
  }
  ReleaseIdsPtr(mdm);

  return result;
}

void PutToStorage(MetadataManager *mdm, const char *key, u64 val,
                  MapType map_type) {
  Heap *heap = GetMapHeap(mdm);
  IdMap *map = GetMap(mdm, map_type);
  shput(map, key, val, heap);
  ReleaseMap(mdm, map_type);

  // TODO(chogan): Maybe wrap this in a DEBUG only macro?
  CheckHeapOverlap(mdm);
}

u64 GetFromStorage(MetadataManager *mdm, const char *key, MapType map_type) {
  Heap *heap = GetMapHeap(mdm);
  IdMap *map = GetMap(mdm, map_type);
  u64 result = shget(map, key, heap);
  ReleaseMap(mdm, map_type);

  return result;
}

std::string ReverseGetFromStorage(MetadataManager *mdm, u64 id,
                                  MapType map_type) {
  std::string result;
  size_t map_size = GetStoredMapSize(mdm, map_type);
  IdMap *map = GetMap(mdm, map_type);

  // TODO(chogan): @optimization This could be more efficient if necessary
  for (size_t i = 0; i < map_size; ++i) {
    if (map[i].value == id) {
      char *key = GetKey(mdm, map, i);
      if (key) {
        result = key;
      }
      break;
    }
  }
  ReleaseMap(mdm, map_type);

  return result;
}

void DeleteFromStorage(MetadataManager *mdm, const char *key,
                       MapType map_type) {
  Heap *heap = GetMapHeap(mdm);
  IdMap *map = GetMap(mdm, map_type);
  shdel(map, key, heap);
  ReleaseMap(mdm, map_type);

  // TODO(chogan): Maybe wrap this in a DEBUG only macro?
  CheckHeapOverlap(mdm);
}

size_t GetStoredMapSize(MetadataManager *mdm, MapType map_type) {
  IdMap *map = GetMap(mdm, map_type);
  size_t result = shlen(map);
  ReleaseMap(mdm, map_type);

  return result;
}

std::vector<u64>
GetRemainingNodeCapacities(SharedMemoryContext *context,
                           const std::vector<TargetID> &targets) {
  std::vector<u64> result(targets.size());

  for (size_t i = 0; i < targets.size(); ++i) {
    result[i] = LocalGetRemainingCapacity(context, targets[i]);
  }

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

void InitNeighborhoodTargets(SharedMemoryContext *context, RpcContext *rpc) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  std::vector<TargetID> neighborhood_targets =
    GetNeighborhoodTargets(context, rpc);
  size_t num_targets = neighborhood_targets.size();
  IdList targets = AllocateIdList(mdm, num_targets);
  TargetID *ids = (TargetID *)GetIdsPtr(mdm, targets);
  for (size_t i = 0; i < num_targets; ++i) {
    ids[i] = neighborhood_targets[i];
  }
  ReleaseIdsPtr(mdm);

  mdm->neighborhood_targets = targets;
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

  // NOTE(chogan): Local Targets default to one Target per Device
  IdList node_targets = AllocateIdList(mdm, config->num_devices);
  TargetID *target_ids = (TargetID *)GetIdsPtr(mdm, node_targets);
  for (u32 i = 0; i < node_targets.length; ++i) {
    Target *target = GetTarget(context, i);
    target_ids[i] = target->id;
  }
  ReleaseIdsPtr(mdm);
  mdm->node_targets = node_targets;

  // ID Maps

  i64 total_map_capacity = GetHeapFreeList(map_heap)->size / 3;

  IdMap *bucket_map = 0;
  // TODO(chogan): We can either calculate an average expected size here, or
  // make HeapRealloc actually use realloc semantics so it can grow as big as
  // needed. But that requires updating offsets for the map and the heap's free
  // list

  // NOTE(chogan): Make the capacity one larger than necessary because the
  // stb_ds map tries to grow when it reaches capacity.
  u32 max_buckets = config->max_buckets_per_node + 1;
  u32 max_vbuckets = config->max_vbuckets_per_node + 1;

  sh_new_strdup(bucket_map, max_buckets, map_heap);
  shdefault(bucket_map, 0, map_heap);
  mdm->bucket_map_offset = GetOffsetFromMdm(mdm, bucket_map);
  u32 bucket_map_num_bytes = map_heap->extent;
  total_map_capacity -= bucket_map_num_bytes;
  assert(total_map_capacity > 0);

  // TODO(chogan): Just one map means better size estimate, but it's probably
  // slower because they'll all share a lock.

  IdMap *vbucket_map = 0;
  sh_new_strdup(vbucket_map, max_vbuckets, map_heap);
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
