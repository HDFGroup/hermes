#ifndef HERMES_METADATA_MANAGEMENT_H_
#define HERMES_METADATA_MANAGEMENT_H_

#include <string.h>

#include <string>

#include "memory_arena.h"
#include "buffer_pool.h"

namespace hermes {

enum class MapType {
  kBucket,
  kVBucket,
  kBlob,
};

struct String {
  char *str;
  size_t length;
};

struct HeapString {
  u32 str_offset;
  u32 length;
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
    u32 index;
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

struct BucketInfo {
  BucketID next_free;
  u32 blobs_offset;  // BlobID array
  u32 num_blobs;
  bool active;
  Stats stats;
};

static constexpr int kMaxTraitsPerVBucket = 8;

struct VBucketInfo {
  VBucketID next_free;
  u32 blobs_offset;  // BlobID array
  u32 num_blobs;
  TraitID traits[kMaxTraitsPerVBucket];
  bool active;
  Stats stats;
};

struct IdMap {
  char *key;
  u64 value;
};

struct MetadataManager {
  ptrdiff_t bucket_info_offset;
  BucketID first_free_bucket;

  ptrdiff_t vbucket_info_offset;
  VBucketID first_free_vbucket;

  ptrdiff_t id_heap_offset;
  ptrdiff_t map_heap_offset;

  // TODO(chogan): Make sure these don't cross
  ptrdiff_t id_heap_start_offset;
  ptrdiff_t map_heap_end_offset;

  ptrdiff_t bucket_map_offset;
  ptrdiff_t vbucket_map_offset;
  ptrdiff_t blob_map_offset;

  TicketMutex bucket_mutex;
  TicketMutex vbucket_mutex;
  TicketMutex blob_id_mutex;
  TicketMutex buffer_id_mutex;

  TicketMutex bucket_map_mutex;
  TicketMutex vbucket_map_mutex;
  TicketMutex blob_map_mutex;

  size_t map_seed;

  u32 num_buckets;
  u32 max_buckets;
  u32 num_vbuckets;
  u32 max_vbuckets;

  u32 rpc_server_name_offset;
};

struct RpcContext;

void InitMetadataManager(MetadataManager *mdm, Arena *arena, Config *config,
                         int node_id);
BucketID GetBucketIdByName(SharedMemoryContext *context, const char *name,
                           CommunicationContext *comm, RpcContext *rpc);
BucketID GetNextFreeBucketId(SharedMemoryContext *context,
                             CommunicationContext *comm, RpcContext *rpc,
                             const std::string &name);

// internal
MetadataManager *GetMetadataManagerFromContext(SharedMemoryContext *context);
BucketInfo *GetBucketInfoByIndex(MetadataManager *mdm, u32 index);
VBucketInfo *GetVBucketInfoByIndex(MetadataManager *mdm, u32 index);

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

template<typename A>
void serialize(A &ar, MapType map_type) {
  ar & (int)map_type;
}

} // namespace hermes

#endif  // HERMES_METADATA_MANAGEMENT_H_
