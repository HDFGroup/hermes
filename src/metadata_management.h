#ifndef HERMES_METADATA_MANAGEMENT_H_
#define HERMES_METADATA_MANAGEMENT_H_

#include <string.h>

#include <string>

#include "memory_arena.h"
#include "buffer_pool.h"

namespace hermes {

struct String {
  char *str;
  size_t length;
};

struct FreeBlock {
  ptrdiff_t next_offset;
  size_t size;
};

// TODO(chogan): Threadsafe Heap
struct Heap {
  Arena arena;
  ptrdiff_t free_list_offset;
  size_t alignment;
};

struct Pool {
  ptrdiff_t free_list_offset;
  u32 used;
  u32 capacity;
  u8 alignment;
  u8 object_size;
};

union BucketID {
  struct {
    u32 index;
    u32 node_id;
  } bits;

  u64 as_int;
};

struct VBucketID {
  struct {
    u32 index;
    u32 node_id;
  } bits;

  u64 as_int;
};

struct BlobID {
  u64 val;
};

enum class TraitID : u8 {

};

struct Stats {

};

struct BucketInfo {
  Stats stats;
  ptrdiff_t blobs_offset;  // BlobID array
};

static constexpr int kMaxTraitsPerVBucket = 8;

struct VBucketInfo {
  Stats stats;
  ptrdiff_t blobs_offset;  // BlobID array
  TraitID traits[kMaxTraitsPerVBucket];
};

struct BlobNode {
  BlobID id;
  ptrdiff_t next_offset;
};

struct BufferNode {
  BufferID id;
  ptrdiff_t next_offset;
};

struct BucketMap {
  char *key;
  BucketID value;
};

struct VBucketMap {
  char *key;
  VBucketID value;
};

struct BlobMap {
  char *key;
  BlobID value;
};

struct MetadataManager {
  ptrdiff_t bucket_free_list_offset;
  ptrdiff_t vbucket_free_list_offset;
  ptrdiff_t bucket_info_offset;
  ptrdiff_t vbucket_info_offset;
  ptrdiff_t blob_id_free_list_offset;
  ptrdiff_t buffer_id_free_list_offset;

  ptrdiff_t bucket_map_offset;
  ptrdiff_t vbucket_map_offset;
  ptrdiff_t blob_map_offset;

  u32 num_buckets;
  u32 num_vbuckets;
};

}  // namespace hermes

#endif  // HERMES_METADATA_MANAGEMENT_H_
