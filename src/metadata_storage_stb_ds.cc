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

#define STBDS_REALLOC(heap, ptr, size) hermes::HeapRealloc(heap, ptr, size)
#define STBDS_FREE(heap, ptr) hermes::HeapFree(heap, ptr)

#define STBDS_ASSERT(x) assert((x))

#define STB_DS_IMPLEMENTATION
#include "stb_ds.h"
#include "buffer_pool_internal.h"

namespace hermes {

/**
  A structure to represent ID <key, value> map
*/
struct IdMap {
  char *key;                    /**< ID string as key */
  u64 value;                    /**< ID value */
};

/**
  A structure to represent BLOB information <key, value> map
*/
struct BlobInfoMap {
  BlobID key;                   /**< BLOB ID as key */
  BlobInfo value;               /**< BLOB information value */
};

/** get map by offset */
static IdMap *GetMapByOffset(MetadataManager *mdm, u32 offset) {
  IdMap *result =(IdMap *)((u8 *)mdm + offset);

  return result;
}

/** get bucket map */
static IdMap *GetBucketMap(MetadataManager *mdm) {
  IdMap *result = GetMapByOffset(mdm, mdm->bucket_map_offset);

  return result;
}

/** get virtual bucket map */
static IdMap *GetVBucketMap(MetadataManager *mdm) {
  IdMap *result = GetMapByOffset(mdm, mdm->vbucket_map_offset);

  return result;
}

/** get BLOB ID map */
static IdMap *GetBlobIdMap(MetadataManager *mdm) {
  IdMap *result = GetMapByOffset(mdm, mdm->blob_id_map_offset);

  return result;
}

/** get map heap */
Heap *GetMapHeap(MetadataManager *mdm) {
  Heap *result = (Heap *)((u8 *)mdm + mdm->map_heap_offset);

  return result;
}

/** get ID heap */
Heap *GetIdHeap(MetadataManager *mdm) {
  Heap *result = (Heap *)((u8 *)mdm + mdm->id_heap_offset);

  return result;
}

/** check if heap overlaps */
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

/** get ticket mutex based on \a map_type */
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
    case kMapType_BlobId: {
      mutex = &mdm->blob_id_map_mutex;
      break;
    }
    case kMapType_BlobInfo: {
      mutex = &mdm->blob_info_map_mutex;
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
    case kMapType_BlobId: {
      result = GetBlobIdMap(mdm);
      break;
    }
    default:
      HERMES_INVALID_CODE_PATH;
  }

  return result;
}

/** get BLOB information map without mutex locking */
BlobInfoMap *GetBlobInfoMapNoLock(MetadataManager *mdm) {
  BlobInfoMap *result = (BlobInfoMap *)((u8 *)mdm + mdm->blob_info_map_offset);

  return result;
}

/** get BLOB information map */
BlobInfoMap *GetBlobInfoMap(MetadataManager *mdm) {
  TicketMutex *mutex = GetMapMutex(mdm, kMapType_BlobInfo);
  BeginTicketMutex(mutex);
  BlobInfoMap *result = GetBlobInfoMapNoLock(mdm);

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
    case kMapType_BlobId: {
      mutex = &mdm->blob_id_map_mutex;
      break;
    }
    case kMapType_BlobInfo: {
      mutex = &mdm->blob_info_map_mutex;
      break;
    }
    default: {
      HERMES_INVALID_CODE_PATH;
    }
  }

  EndTicketMutex(mutex);
}

/**
 * Takes a lock on mdm->blob_info_map_mutex. Requires a corresponding
 * ReleaseBlobInfoPtr call.
 */
BlobInfo *GetBlobInfoPtr(MetadataManager *mdm, BlobID blob_id) {
  Heap *map_heap = GetMapHeap(mdm);
  BlobInfoMap *map = GetBlobInfoMap(mdm);
  BlobInfoMap *element = hmgetp_null(map, blob_id, map_heap);

  BlobInfo *result = 0;
  if (element) {
    result = &element->value;
  }

  return result;
}

/** release pointer to BLOB information */
void ReleaseBlobInfoPtr(MetadataManager *mdm) {
  EndTicketMutex(&mdm->blob_info_map_mutex);
}

/** get BLOB stats locally */
Stats LocalGetBlobStats(SharedMemoryContext *context, BlobID blob_id) {
  Stats result = {};
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  Heap *map_heap = GetMapHeap(mdm);
  BlobInfoMap *map = GetBlobInfoMap(mdm);
  BlobInfoMap *info = hmgetp_null(map, blob_id, map_heap);
  if (info) {
    result = info->value.stats;
  }
  ReleaseMap(mdm, kMapType_BlobInfo);

  return result;
}

/**
 * Return a pointer to the internal array of IDs that the `id_list`
 * represents.
 *
 * This call acquires a lock, and must be paired with a corresponding call to
 * `ReleaseIdsPtr` to release the lock.
 */
u64 *GetIdsPtr(MetadataManager *mdm, IdList id_list) {
  Heap *id_heap = GetIdHeap(mdm);
  BeginTicketMutex(&mdm->id_mutex);
  u64 *result = (u64 *)HeapOffsetToPtr(id_heap, id_list.head_offset);

  return result;
}

/**
   get pointer to the internal array of IDs that the chucked \a id_list
   represents
*/
u64 *GetIdsPtr(MetadataManager *mdm, ChunkedIdList id_list) {
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

/** release IDs pointer */
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

/** free ID list */
template<typename T>
void FreeIdList(MetadataManager *mdm, T id_list) {
  Heap *id_heap = GetIdHeap(mdm);
  BeginTicketMutex(&mdm->id_mutex);
  u8 *ptr = HeapOffsetToPtr(id_heap, id_list.head_offset);
  HeapFree(id_heap, ptr);
  EndTicketMutex(&mdm->id_mutex);
}

/** free \a id_list ID list */
void FreeIdList(MetadataManager *mdm, IdList id_list) {
  Heap *id_heap = GetIdHeap(mdm);
  BeginTicketMutex(&mdm->id_mutex);
  u8 *ptr = HeapOffsetToPtr(id_heap, id_list.head_offset);
  HeapFree(id_heap, ptr);
  EndTicketMutex(&mdm->id_mutex);
}

/** free embedded ID list */
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
 *
 * @return The index of the appended @p id.
 */
u32 AppendToChunkedIdList(MetadataManager *mdm, ChunkedIdList *id_list,
                          u64 id) {
  if (id_list->length >= id_list->capacity) {
    AllocateOrGrowIdList(mdm, id_list);
  }

  u64 *head = GetIdsPtr(mdm, *id_list);
  u32 result = id_list->length;
  head[id_list->length++] = id;
  ReleaseIdsPtr(mdm);

  return result;
}

/**
 * Assumes the caller has protected @p id_list with a lock.
 *
 * @return A vector of the IDs.
 */
std::vector<u64> GetChunkedIdList(MetadataManager *mdm, ChunkedIdList id_list) {
  std::vector<u64> result(id_list.length);
  if (id_list.length > 0) {
    u64 *head = GetIdsPtr(mdm, id_list);
    for (u32 i = 0; i < id_list.length; ++i) {
      result[i] = head[i];
    }
    ReleaseIdsPtr(mdm);
  }

  return result;
}

/** get chuncked ID list element */
u64 GetChunkedIdListElement(MetadataManager *mdm, ChunkedIdList *id_list,
                            u32 index) {
  u64 result = 0;
  if (id_list->length && index < id_list->length) {
    u64 *head = GetIdsPtr(mdm, *id_list);
    result = head[index];
    ReleaseIdsPtr(mdm);
  }

  return result;
}

/** set chuncked ID list element */
void SetChunkedIdListElement(MetadataManager *mdm, ChunkedIdList *id_list,
                             u32 index, u64 value) {
  if (id_list->length && index >= id_list->length) {
    LOG(WARNING) << "Attempting to set index " << index
                 << " on a ChunkedIdList of length " << id_list->length
                 << std::endl;
  } else {
    u64 *head = GetIdsPtr(mdm, *id_list);
    head[index] = value;
    ReleaseIdsPtr(mdm);
  }
}

void LocalIncrementBlobStats(MetadataManager *mdm, BlobID blob_id) {
  BlobInfo *info = GetBlobInfoPtr(mdm, blob_id);
  if (info) {
    info->stats.frequency++;
    info->stats.recency = mdm->clock++;
  }
  ReleaseBlobInfoPtr(mdm);
}

void IncrementBlobStats(SharedMemoryContext *context, RpcContext *rpc,
                        BlobID blob_id) {
  u32 target_node = GetBlobNodeId(blob_id);
  if (target_node == rpc->node_id) {
    MetadataManager *mdm = GetMetadataManagerFromContext(context);
    LocalIncrementBlobStats(mdm, blob_id);
  } else {
    RpcCall<bool>(rpc, target_node, "RemoteIncrementBlobStats", blob_id);
  }
}

/** get index of ID */
i64 GetIndexOfId(MetadataManager *mdm, ChunkedIdList *id_list, u64 id) {
  i64 result = -1;

  u64 *head = GetIdsPtr(mdm, *id_list);
  for (i64 i = 0; i < id_list->length; ++i) {
    if (head[i] == id) {
      result = i;
      break;
    }
  }
  ReleaseIdsPtr(mdm);

  return result;
}

void LocalReplaceBlobIdInBucket(SharedMemoryContext *context,
                                BucketID bucket_id, BlobID old_blob_id,
                                BlobID new_blob_id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BeginTicketMutex(&mdm->bucket_mutex);
  BucketInfo *info = LocalGetBucketInfoById(mdm, bucket_id);

  if (info && info->active) {
    ChunkedIdList *blobs = &info->blobs;

    BlobID *blobs_arr = (BlobID *)GetIdsPtr(mdm, *blobs);
    for (u32 i = 0; i < blobs->length; ++i) {
      if (blobs_arr[i].as_int == old_blob_id.as_int) {
        blobs_arr[i] = new_blob_id;
        break;
      }
    }
    ReleaseIdsPtr(mdm);
  }

  EndTicketMutex(&mdm->bucket_mutex);
}

/** add BLOB ID to bucket locally */
void LocalAddBlobIdToBucket(MetadataManager *mdm, BucketID bucket_id,
                            BlobID blob_id, bool track_stats) {
  BeginTicketMutex(&mdm->bucket_mutex);
  BucketInfo *info = LocalGetBucketInfoById(mdm, bucket_id);
  AppendToChunkedIdList(mdm, &info->blobs, blob_id.as_int);
  if (track_stats) {
    LocalIncrementBlobStats(mdm, blob_id);
  }
  EndTicketMutex(&mdm->bucket_mutex);

  CheckHeapOverlap(mdm);
}

/** add BLOB ID to virtual bucket locally */
void LocalAddBlobIdToVBucket(MetadataManager *mdm, VBucketID vbucket_id,
                             BlobID blob_id) {
  BeginTicketMutex(&mdm->vbucket_mutex);
  VBucketInfo *info = LocalGetVBucketInfoById(mdm, vbucket_id);
  AppendToChunkedIdList(mdm, &info->blobs, blob_id.as_int);
  EndTicketMutex(&mdm->vbucket_mutex);

  CheckHeapOverlap(mdm);
}

/** allocate ID list */
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

/** allocate embedded ID list */
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

/** get BLOB IDs locally */
std::vector<BlobID> LocalGetBlobIds(SharedMemoryContext *context,
                                    BucketID bucket_id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BeginTicketMutex(&mdm->bucket_mutex);
  BucketInfo *info = LocalGetBucketInfoById(mdm, bucket_id);
  u32 num_blobs = info->blobs.length;
  std::vector<BlobID> result(num_blobs);

  BlobID *blob_ids = (BlobID *)GetIdsPtr(mdm, info->blobs);
  for (u32 i = 0; i < num_blobs; ++i) {
    result[i] = blob_ids[i];
  }
  ReleaseIdsPtr(mdm);
  EndTicketMutex(&mdm->bucket_mutex);

  return result;
}

/** allocate buffer ID list locally */
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

/** get buffer ID list locally */
std::vector<BufferID> LocalGetBufferIdList(MetadataManager *mdm,
                                           BlobID blob_id) {
  size_t length = 0;
  BufferID *ids = GetBufferIdsPtrFromBlobId(mdm, blob_id, length);
  std::vector<BufferID> result(length);
  CopyIds((u64 *)result.data(), (u64 *)ids, length);
  ReleaseIdsPtr(mdm);

  return result;
}

/** get buffer ID list into \a buffer_ids locally */
void LocalGetBufferIdList(Arena *arena, MetadataManager *mdm, BlobID blob_id,
                          BufferIdArray *buffer_ids) {
  size_t length = 0;
  BufferID *ids = GetBufferIdsPtrFromBlobId(mdm, blob_id, length);
  buffer_ids->ids = PushArray<BufferID>(arena, length);
  buffer_ids->length = length;
  CopyIds((u64 *)buffer_ids->ids, (u64 *)ids, length);
  ReleaseIdsPtr(mdm);
}

/** free buffer ID list locally */
void LocalFreeBufferIdList(SharedMemoryContext *context, BlobID blob_id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  FreeEmbeddedIdList(mdm, blob_id.bits.buffer_ids_offset);
  CheckHeapOverlap(mdm);
}

/** remove BLOB from bucket locally */
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

/** does \a bucket_id contain \a blob_id BLOB locally? */
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

/** is \a list's capacity greater than 0?  */
static inline bool HasAllocated(ChunkedIdList *list) {
  bool result = list->capacity > 0;

  return result;
}

/** Has virtual bucket \a info allocated BLOBS?  */
static inline bool HasAllocatedBlobs(VBucketInfo *info) {
  bool result = HasAllocated(&info->blobs);

  return result;
}

/** Has bucket \a info allocated BLOBS?  */
static inline bool HasAllocatedBlobs(BucketInfo *info) {
  bool result = HasAllocated(&info->blobs);

  return result;
}

/** destroy bucket locally */
bool LocalDestroyBucket(SharedMemoryContext *context, RpcContext *rpc,
                        const char *bucket_name, BucketID bucket_id) {
  bool destroyed = false;
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BeginWriterLock(&mdm->bucket_delete_lock);
  BeginTicketMutex(&mdm->bucket_mutex);
  BucketInfo *info = LocalGetBucketInfoById(mdm, bucket_id);

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
        DestroyBlobById(context, rpc, blob_id, bucket_id);
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
  EndWriterLock(&mdm->bucket_delete_lock);

  return destroyed;
}

/** destroy virtual bucket locally */
bool LocalDestroyVBucket(SharedMemoryContext *context, const char *vbucket_name,
                         VBucketID vbucket_id) {
  bool destroyed = false;
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BeginTicketMutex(&mdm->vbucket_mutex);
  VBucketInfo *info = LocalGetVBucketInfoById(mdm, vbucket_id);

  // TODO(chogan): @optimization Lock granularity can probably be relaxed if
  // this is slow
  int ref_count = info->ref_count.load();
  if (ref_count == 1) {
    if (HasAllocatedBlobs(info)) {
      FreeIdList(mdm, info->blobs);
    }

    info->blobs.length = 0;
    info->blobs.capacity = 0;
    info->blobs.head_offset = 0;

    // Reset VBucketInfo to initial values
    info->ref_count.store(0);
    info->async_flush_count.store(0);
    info->active = false;

    mdm->num_vbuckets--;
    info->next_free = mdm->first_free_vbucket;
    mdm->first_free_vbucket = vbucket_id;

    // Remove (name -> vbucket_id) map entry
    LocalDelete(mdm, vbucket_name, kMapType_VBucket);
    destroyed = true;
  } else {
    LOG(INFO) << "Cannot destroy vbucket " << vbucket_name
              << ". Its refcount is " << ref_count << std::endl;
  }
  EndTicketMutex(&mdm->vbucket_mutex);
  return destroyed;
}

/** get targets locally */
std::vector<TargetID> LocalGetTargets(SharedMemoryContext *context,
                                      IdList target_list) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  u32 length = target_list.length;
  std::vector<TargetID> result;
  result.reserve(length);

  u64 *target_ids = GetIdsPtr(mdm, target_list);
  for (u32 i = 0; i < length; ++i) {
    TargetID id = {};
    id.as_int = target_ids[i];
    u64 remaining_capacity = LocalGetRemainingTargetCapacity(context, id);
    if (remaining_capacity) {
      result.push_back(id);
    }
  }
  ReleaseIdsPtr(mdm);

  return result;
}

/** get node targets locally */
std::vector<TargetID> LocalGetNodeTargets(SharedMemoryContext *context) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  std::vector<TargetID> result = LocalGetTargets(context, mdm->node_targets);

  return result;
}

/** get neighborhood targets locally */
std::vector<TargetID>
LocalGetNeighborhoodTargets(SharedMemoryContext *context) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  std::vector<TargetID> result = LocalGetTargets(context,
                                                 mdm->neighborhood_targets);

  return result;
}

/** put to storage */
void PutToStorage(MetadataManager *mdm, BlobID key, const BlobInfo &val) {
  Heap *heap = GetMapHeap(mdm);
  BlobInfoMap *map = GetBlobInfoMap(mdm);
  hmput(map, key, val, heap);
  ReleaseMap(mdm, kMapType_BlobInfo);

  CheckHeapOverlap(mdm);
}

/** put \a map_type to storage */
void PutToStorage(MetadataManager *mdm, const char *key, u64 val,
                  MapType map_type) {
  Heap *heap = GetMapHeap(mdm);
  IdMap *map = GetMap(mdm, map_type);
  shput(map, key, val, heap);
  ReleaseMap(mdm, map_type);

  // TODO(chogan): Maybe wrap this in a DEBUG only macro?
  CheckHeapOverlap(mdm);
}

/** get from storage */
u64 GetFromStorage(MetadataManager *mdm, const char *key, MapType map_type) {
  Heap *heap = GetMapHeap(mdm);
  IdMap *map = GetMap(mdm, map_type);
  u64 result = shget(map, key, heap);
  ReleaseMap(mdm, map_type);

  return result;
}

/** reverse the get from storage operation */
std::string ReverseGetFromStorage(MetadataManager *mdm, u64 id,
                                  MapType map_type) {
  std::string result;
  IdMap *map = GetMap(mdm, map_type);
  size_t map_size = shlen(map);

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

/** delete from storage */
void DeleteFromStorage(MetadataManager *mdm, BlobID key, bool lock) {
  Heap *heap = GetMapHeap(mdm);
  BlobInfoMap *map = 0;

  if (lock) {
    map = GetBlobInfoMap(mdm);
  } else {
    map = GetBlobInfoMapNoLock(mdm);
  }

  hmdel(map, key, heap);

  if (lock) {
    ReleaseMap(mdm, kMapType_BlobInfo);
  }
}

/** delete \a map_type from storage */
void DeleteFromStorage(MetadataManager *mdm, const char *key,
                       MapType map_type) {
  Heap *heap = GetMapHeap(mdm);
  IdMap *map = GetMap(mdm, map_type);
  shdel(map, key, heap);
  ReleaseMap(mdm, map_type);

  // TODO(chogan): Maybe wrap this in a DEBUG only macro?
  CheckHeapOverlap(mdm);
}

/** get the size of stored map */
size_t GetStoredMapSize(MetadataManager *mdm, MapType map_type) {
  IdMap *map = GetMap(mdm, map_type);
  size_t result = shlen(map);
  ReleaseMap(mdm, map_type);

  return result;
}

/** generate hash string for storage  */
u32 HashStringForStorage(MetadataManager *mdm, RpcContext *rpc,
                         const char *str) {
  int result =
    (stbds_hash_string((char *)str, mdm->map_seed) % rpc->num_nodes) + 1;

  return result;
}

/** seed hash for storage  */
void SeedHashForStorage(size_t seed) {
  stbds_rand_seed(seed);
}

/** initialize swap space file name */
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

/** initialize neighborhood targets */
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

/** initialize metadata storage */
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

  // Maps

  i64 remaining_map_capacity = GetHeapFreeList(map_heap)->size / 3;

  IdMap *bucket_map = 0;
  // TODO(chogan): We can either calculate an average expected size here, or
  // make HeapRealloc actually use realloc semantics so it can grow as big as
  // needed. But that requires updating offsets for the map and the heap's free
  // list

  // NOTE(chogan): Make the capacity one larger than necessary because the
  // stb_ds map tries to grow when it reaches capacity.
  u32 max_buckets = config->max_buckets_per_node + 1;
  u32 max_vbuckets = config->max_vbuckets_per_node + 1;

  // Create Bucket name -> BucketID map
  sh_new_strdup(bucket_map, max_buckets, map_heap);
  shdefault(bucket_map, 0, map_heap);
  mdm->bucket_map_offset = GetOffsetFromMdm(mdm, bucket_map);
  u32 bucket_map_num_bytes = map_heap->extent;
  remaining_map_capacity -= bucket_map_num_bytes;
  assert(remaining_map_capacity > 0);

  // Create VBucket name -> VBucketID map
  IdMap *vbucket_map = 0;
  sh_new_strdup(vbucket_map, max_vbuckets, map_heap);
  shdefault(vbucket_map, 0, map_heap);
  mdm->vbucket_map_offset = GetOffsetFromMdm(mdm, vbucket_map);
  u32 vbucket_map_num_bytes = map_heap->extent - bucket_map_num_bytes;
  remaining_map_capacity -= vbucket_map_num_bytes;
  assert(remaining_map_capacity > 0);

  // NOTE(chogan): Each map element requires twice its size for storage.
  size_t id_map_element_size = 2 * sizeof(IdMap);
  size_t blob_info_map_element_size = 2 * sizeof(BlobInfoMap);
  size_t bytes_per_blob = id_map_element_size + blob_info_map_element_size;
  size_t num_blobs_supported = remaining_map_capacity / bytes_per_blob;
  LOG(INFO) << "Metadata can support " << num_blobs_supported
            << " Blobs per node\n";

  // Create Blob name -> BlobID map
  IdMap *blob_map = 0;
  sh_new_strdup(blob_map, num_blobs_supported, map_heap);
  shdefault(blob_map, 0, map_heap);
  mdm->blob_id_map_offset = GetOffsetFromMdm(mdm, blob_map);

  // Create BlobID -> BlobInfo map
  BlobInfoMap *blob_info_map = 0;
  blob_info_map =
    (BlobInfoMap *)stbds_arrgrowf(blob_info_map, sizeof(*blob_info_map), 0,
                                  num_blobs_supported, map_heap);
  blob_info_map =
    (BlobInfoMap *)STBDS_ARR_TO_HASH(blob_info_map, sizeof(*blob_info_map));
  BlobInfo default_blob_info = {};
  hmdefault(blob_info_map, default_blob_info, map_heap);
  mdm->blob_info_map_offset = GetOffsetFromMdm(mdm, blob_info_map);
}

/** get BLOBs from virtual bucket information locally */
std::vector<BlobID> LocalGetBlobsFromVBucketInfo(SharedMemoryContext *context,
                                                 VBucketID vbucket_id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BeginTicketMutex(&mdm->vbucket_mutex);
  VBucketInfo *info = LocalGetVBucketInfoById(mdm, vbucket_id);
  ChunkedIdList *blobs = &info->blobs;
  BlobID *blobs_arr = (BlobID *)GetIdsPtr(mdm, *blobs);
  std::vector<BlobID> blobids(blobs_arr, blobs_arr + blobs->length);
  ReleaseIdsPtr(mdm);
  EndTicketMutex(&mdm->vbucket_mutex);
  return blobids;
}

/** remove BLOB from virtual bucket locally */
void LocalRemoveBlobFromVBucketInfo(SharedMemoryContext *context,
                                    VBucketID vbucket_id, BlobID blob_id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  BeginTicketMutex(&mdm->vbucket_mutex);
  VBucketInfo *info = LocalGetVBucketInfoById(mdm, vbucket_id);
  ChunkedIdList *blobs = &info->blobs;
  BlobID *blobs_arr = (BlobID *)GetIdsPtr(mdm, *blobs);
  for (u32 i = 0; i < blobs->length; ++i) {
    if (blobs_arr[i].as_int == blob_id.as_int) {
      blobs_arr[i] = blobs_arr[--blobs->length];
      break;
    }
  }
  ReleaseIdsPtr(mdm);
  EndTicketMutex(&mdm->vbucket_mutex);
}

/** get BLOB's importance score locally */
f32 LocalGetBlobImportanceScore(SharedMemoryContext *context, BlobID blob_id) {
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  Stats stats = LocalGetBlobStats(context, blob_id);

  f32 result = ScoringFunction(mdm, &stats);

  return result;
}

/**
   Get the importance score of \a blob_id BLOB.
 */   
f32 GetBlobImportanceScore(SharedMemoryContext *context, RpcContext *rpc,
                           BlobID blob_id) {
  f32 result = 0;
  u32 target_node = GetBlobNodeId(blob_id);
  if (target_node == rpc->node_id) {
    result = LocalGetBlobImportanceScore(context, blob_id);
  } else {
    result = RpcCall<f32>(rpc, target_node, "RemoteGetBlobImportanceScore",
                          blob_id);
  }

  return result;
}

}  // namespace hermes
