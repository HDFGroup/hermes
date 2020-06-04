#ifndef HERMES_METADATA_MANAGEMENT_H_
#define HERMES_METADATA_MANAGEMENT_H_

#include <string.h>

#include <atomic>
#include <string>

#include "memory_management.h"
#include "buffer_pool.h"

namespace hermes {

struct RpcContext;

enum class MapType {
  kBucket,
  kVBucket,
  kBlob,
};

union BucketID {
  struct {
    u32 index;
    u32 node_id;
  } bits;

  u64 as_int;
};

union VBucketID {
  struct {
    u32 index;
    u32 node_id;
  } bits;

  u64 as_int;
};

union BlobID {
  struct {
    u32 buffer_ids_offset;
    u32 node_id;
  } bits;

  u64 as_int;
};

enum class TraitID : u8 {
  None,
  Placement,
  Flush,
  Replication,
  Checksum,
  BlobSort,
  BlobFilter,
  Index,

  Count,
};

struct Stats {

};

const int kIdListChunkSize = 10;

struct BlobIdList {
  u32 head_offset;
  u32 length;
  u32 capacity;
};

struct BufferIdList {
  u32 head_offset;
  u32 length;
};

struct BufferIdArray {
  BufferID *ids;
  u32 length;
};

struct BucketInfo {
  BucketID next_free;
  BlobIdList blobs;
  std::atomic<int> ref_count;
  bool active;
  Stats stats;
};

static constexpr int kMaxTraitsPerVBucket = 8;

struct VBucketInfo {
  VBucketID next_free;
  BlobIdList blobs;
  std::atomic<int> ref_count;
  TraitID traits[kMaxTraitsPerVBucket];
  bool active;
  Stats stats;
};

struct IdMap {
  char *key;
  u64 value;
};

struct MetadataManager {
  // All offsets are relative to the beginning of the MDM
  ptrdiff_t bucket_info_offset;
  BucketID first_free_bucket;

  ptrdiff_t vbucket_info_offset;
  VBucketID first_free_vbucket;

  ptrdiff_t id_heap_offset;
  ptrdiff_t map_heap_offset;

  ptrdiff_t bucket_map_offset;
  ptrdiff_t vbucket_map_offset;
  ptrdiff_t blob_map_offset;

  TicketMutex bucket_mutex;
  TicketMutex vbucket_mutex;

  TicketMutex bucket_map_mutex;
  TicketMutex vbucket_map_mutex;
  TicketMutex blob_map_mutex;
  TicketMutex id_mutex;

  size_t map_seed;

  u32 num_buckets;
  u32 max_buckets;
  u32 num_vbuckets;
  u32 max_vbuckets;

  u32 rpc_server_name_offset;
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
void DestroyBucket(SharedMemoryContext *context, RpcContext *rpc,
                   const char *name, BucketID bucket_id);

/**
 *
 */
void DestroyBlob(SharedMemoryContext *context, RpcContext *rpc,
                 BucketID bucket_id, const std::string &blob_name);

/**
 *
 */
void RenameBlob(SharedMemoryContext *context, RpcContext *rpc,
                const std::string &old_name, const std::string &new_name);

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
BufferIdArray GetBufferIdsFromBlobName(Arena *arena,
                                       SharedMemoryContext *context,
                                       RpcContext *rpc, const char *blob_name);

/**
 *
 */
BucketID GetOrCreateBucketId(SharedMemoryContext *context, RpcContext *rpc,
                             const std::string &name);

/**
 *
 */
void AttachBlobToBucket(SharedMemoryContext *context, RpcContext *rpc,
                        const char *blob_name, BucketID bucket_id,
                        const std::vector<BufferID> &buffer_ids);

/**
 *
 */
void IncrementRefcount(SharedMemoryContext *context, RpcContext *rpc,
                       BucketID id);

/**
 *
 */
void DecrementRefcount(SharedMemoryContext *context, RpcContext *rpc,
                       BucketID id);

// internal
MetadataManager *GetMetadataManagerFromContext(SharedMemoryContext *context);
BucketInfo *LocalGetBucketInfoByIndex(MetadataManager *mdm, u32 index);
VBucketInfo *GetVBucketInfoByIndex(MetadataManager *mdm, u32 index);

void LocalAddBlobIdToBucket(MetadataManager *mdm, BucketID bucket_id,
                            BlobID blob_id);
std::vector<BufferID> LocalGetBufferIdList(MetadataManager *mdm,
                                           BlobID blob_id);
void LocalFreeBufferIdList(SharedMemoryContext *context, BlobID blob_id);
void LocalDestroyBucket(SharedMemoryContext *context, RpcContext *rpc,
                        const char *bucket_name, BucketID bucket_id);
void LocalDestroyBlob(SharedMemoryContext *context, RpcContext *rpc,
                      const char *blob_name, BlobID blob_id);
void LocalRenameBucket(SharedMemoryContext *context, RpcContext *rpc,
                       BucketID id, const std::string &old_name,
                       const std::string &new_name);
bool LocalContainsBlob(SharedMemoryContext *context, BucketID bucket_id,
                       BlobID blob_id);
void LocalRemoveBlobFromBucketInfo(SharedMemoryContext *context,
                                   BucketID bucket_id, BlobID blob_id);
void LocalIncrementRefcount(SharedMemoryContext *context, BucketID id);
void LocalDecrementRefcount(SharedMemoryContext *context, BucketID id);

u64 LocalGet(MetadataManager *mdm, const char *key, MapType map_type);
void LocalPut(MetadataManager *mdm, const char *key, u64 val, MapType map_type);
void LocalDelete(MetadataManager *mdm, const char *key, MapType map_type);


Heap *GetIdHeap(MetadataManager *mdm);
Heap *GetMapHeap(MetadataManager *mdm);

/**
 *  Lets Thallium know how to serialize a BucketID.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param bucket_id The BucketID to serialize.
 */
template<typename A>
void serialize(A &ar, BucketID &bucket_id) {
  ar & bucket_id.as_int;
}

/**
 *  Lets Thallium know how to serialize a BlobID.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param blob_id The BlobID to serialize.
 */
template<typename A>
void serialize(A &ar, BlobID &blob_id) {
  ar & blob_id.as_int;
}

/**
 *  Lets Thallium know how to serialize a MapType.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param map_type The MapType to serialize.
 */
template<typename A>
void serialize(A &ar, MapType map_type) {
  ar & (int)map_type;
}

} // namespace hermes

#endif  // HERMES_METADATA_MANAGEMENT_H_
