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
  u32 blobs_offset;  // BlobID array
  u32 num_blobs;
  Stats stats;
};

static constexpr int kMaxTraitsPerVBucket = 8;

struct VBucketInfo {
  u32 blobs_offset;  // BlobID array
  u32 num_blobs;
  TraitID traits[kMaxTraitsPerVBucket];
  Stats stats;
};

struct IdMap {
  char *key;
  u64 value;
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
