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

#include "metadata_management.h"

#include <string.h>

#include <iomanip>
#include <string>

#include "buffer_pool.h"
#include "buffer_pool_internal.h"
#include "buffer_organizer.h"
#include "rpc.h"
#include "metadata_storage.h"

namespace hermes {

/** "not equal" comparision operator */
bool operator!=(const TargetID &lhs, const TargetID &rhs) {
  return lhs.as_int != rhs.as_int;
}

/** is \a name too long for \a max? */
static bool IsNameTooLong(const std::string &name, size_t max) {
  bool result = false;
  if (name.size() + 1 >= max) {
    LOG(WARNING) << "Name '" << name << "' exceeds the maximum name size of "
                 << max << " bytes." << std::endl;
    result = true;
  }

  return result;
}

/** is BLOB's \a name too long for kMaxBlobNameSize? */
bool IsBlobNameTooLong(const std::string &name) {
  bool result = IsNameTooLong(name, kMaxBlobNameSize);

  return result;
}

/** is Bucket's \a name too long for kMaxBucketNameSize? */
bool IsBucketNameTooLong(const std::string &name) {
  bool result = IsNameTooLong(name, kMaxBucketNameSize);

  return result;
}

/** is vBucket's \a name too long for kMaxVBucketNameSize? */
bool IsVBucketNameTooLong(const std::string &name) {
  bool result = IsNameTooLong(name, kMaxVBucketNameSize);
  return result;
}

/** put \a key, \a value, and \a map_type locally */
void LocalPut(const char *key, u64 val,
              MapType map_type) {
  PutToStorage(key, val, map_type);
}

/** put \a key and \a value locally */
void LocalPut(BlobID key, const BlobInfo &value) {
  PutToStorage(key, value);
}

/** get the value of \a key and \a map_type locally */
u64 LocalGet(const char *key, MapType map_type) {
  u64 result = GetFromStorage(key, map_type);

  return result;
}

/** delete \a key locally */
void LocalDelete(BlobID key) {
  DeleteFromStorage(key, false);
}

/** delete \a map_type locally */
void LocalDelete(const char *key, MapType map_type) {
  DeleteFromStorage(key, map_type);
}

/** get hash string for metadata storage */
u32 HashString(const char *str) {
  u32 result = HashStringForStorage(rpc, str);

  return result;
}

/** get id */
u64 GetId(const char *name,
          MapType map_type) {
  u64 result = 0;

  mdm = GetMetadataManagerFromContext(context);
  u32 target_node = HashString(rpc, name);

  if (target_node == rpc->node_id) {
    result = LocalGet(name, map_type);
  } else {
    result = RpcCall<u64>(rpc, target_node, "RemoteGet", std::string(name),
                          map_type);
  }

  return result;
}

/** get bucket id */
BucketID GetBucketId(
                     const char *name) {
  BucketID result = {};
  result.as_int = GetId(context, rpc, name, kMapType_Bucket);

  return result;
}

/** get local bucket id */
BucketID LocalGetBucketId(const char *name) {
  mdm = GetMetadataManagerFromContext(context);
  BucketID result = {};
  result.as_int = LocalGet(name, kMapType_Bucket);

  return result;
}
/** get virtual bucket id */
VBucketID GetVBucketId(
                       const char *name) {
  VBucketID result = {};
  result.as_int = GetId(context, rpc, name, kMapType_VBucket);

  return result;
}

/** get local virtual bucket id */
VBucketID LocalGetVBucketId(const char *name) {
  mdm = GetMetadataManagerFromContext(context);
  VBucketID result = {};
  result.as_int = LocalGet(name, kMapType_VBucket);

  return result;
}

/** make an internal BLOB name */
std::string MakeInternalBlobName(const std::string &name, BucketID id) {
  std::stringstream ss;

  // NOTE(chogan): Store the bytes of \p id at the beginning of the name. We
  // can't just stick the raw bytes in there because the Blob name will
  // eventually be treated as a C string, which means a null byte will be
  // treated as a null terminator. Instead, we store the string representation
  // of each byte in hex, which means we need two bytes to represent one byte.
  for (int i = sizeof(BucketID) - 1; i >= 0 ; --i) {
    // TODO(chogan): @portability Need to perform this loop in reverse on a
    // big-endian platform
    u8 *byte = ((u8 *)&id.as_int) + i;
    ss << std::hex << std::setw(2) << std::setfill('0') << (int)(*byte);
  }
  ss << name;

  std::string result = ss.str();

  return result;
}

/** get BLOB id */
BlobID GetBlobId(
                 const std::string &name, BucketID bucket_id,
                 bool track_stats) {
  std::string internal_name = MakeInternalBlobName(name, bucket_id);
  BlobID result = {};
  result.as_int = GetId(context, rpc, internal_name.c_str(), kMapType_BlobId);
  if (!IsNullBlobId(result) && track_stats) {
    IncrementBlobStats(context, rpc, result);
  }

  return result;
}

/** put BLOB id */
void PutId(const std::string &name,
           u64 id, MapType map_type) {
  u32 target_node = HashString(rpc, name.c_str());
  if (target_node == rpc->node_id) {
    LocalPut(name.c_str(), id, map_type);
  } else {
    RpcCall<bool>(rpc, target_node, "RemotePut", name, id, map_type);
  }
}

/** put bucket id */
void PutBucketId(const std::string &name,
                 BucketID id) {
  PutId(rpc, name, id.as_int, kMapType_Bucket);
}

/** put bucket id locally */
void LocalPutBucketId(const std::string &name,
                      BucketID id) {
  LocalPut(name.c_str(), id.as_int, kMapType_Bucket);
}

/** put virtual bucket id */
void PutVBucketId(
                  const std::string &name, VBucketID id) {
  PutId(rpc, name, id.as_int, kMapType_VBucket);
}

/** put virtual bucket id locally */
void LocalPutVBucketId(const std::string &name,
                       VBucketID id) {
  LocalPut(name.c_str(), id.as_int, kMapType_VBucket);
}

/** put BLOB id */
void PutBlobId(const std::string &name,
               BlobID id, BucketID bucket_id) {
  std::string internal_name = MakeInternalBlobName(name, bucket_id);
  PutId(rpc, internal_name, id.as_int, kMapType_BlobId);
}

/** delete id */
void DeleteId(const std::string &name,
              MapType map_type) {
  u32 target_node = HashString(rpc, name.c_str());

  if (target_node == rpc->node_id) {
    LocalDelete(name.c_str(), map_type);
  } else {
    RpcCall<bool>(rpc, target_node, "RemoteDelete", name, map_type);
  }
}

/** delete bucket id */
void DeleteBucketId(
                    const std::string &name) {
  DeleteId(rpc, name, kMapType_Bucket);
}

/** delete virtual bucket id */
void DeleteVBucketId(
                     const std::string &name) {
  DeleteId(rpc, name, kMapType_VBucket);
}

/** delete BLOB information locally */
void LocalDeleteBlobInfo(BlobID blob_id) {
  LocalDelete(blob_id);
}

/** delete BLOB id locally */
void LocalDeleteBlobId(const std::string &name,
                  BucketID bucket_id) {
  std::string internal_name = MakeInternalBlobName(name, bucket_id);
  LocalDelete(internal_name.c_str(), kMapType_BlobId);
}

/** delete BLOB id */
void DeleteBlobId(
                  const std::string &name, BucketID bucket_id) {
  std::string internal_name = MakeInternalBlobName(name, bucket_id);
  DeleteId(rpc, internal_name, kMapType_BlobId);
}

/** get bucket information by \a index index locally */
BucketInfo *LocalGetBucketInfoByIndex(u32 index) {
  BucketInfo *info_array = (BucketInfo *)((u8 *)mdm + bucket_info_offset);
  BucketInfo *result = info_array + index;

  return result;
}

/** get BLOB name from \a blob_id locally */
std::string LocalGetBlobNameFromId(
                                   BlobID blob_id) {
  mdm = GetMetadataManagerFromContext(context);
  std::string blob_name = ReverseGetFromStorage(blob_id.as_int,
                                                kMapType_BlobId);

  std::string result;
  if (blob_name.size() > kBucketIdStringSize) {
    result = blob_name.substr(kBucketIdStringSize, std::string::npos);
  }

  return result;
}

/** get BLOB name from \a blob_id */
std::string GetBlobNameFromId(
                              BlobID blob_id) {
  u32 target_node = GetBlobNodeId(blob_id);
  std::string result;
  if (target_node == rpc->node_id) {
    result = LocalGetBlobNameFromId(context, blob_id);
  } else {
    result = RpcCall<std::string>(rpc, target_node, "RemoteGetBlobNameFromId",
                                  blob_id);
  }

  return result;
}

/** \note (chogan): Lookup table for HexStringToU64() */
static const u64 hextable[] = {
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0, 0, 0, 10, 11, 12,
  13, 14, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 10, 11, 12, 13, 14, 15, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0
};

u64 HexStringToU64(const std::string &s) {
  u64 result = 0;
  for (size_t i = 0; i < kBucketIdStringSize; ++i) {
    result = (result << 4) | hextable[(int)s[i]];
  }

  return result;
}

/** get bucket ID from \a blob_id locally */
BucketID LocalGetBucketIdFromBlobId(BlobID id) {
  mdm = GetMetadataManagerFromContext(context);
  std::string internal_name = ReverseGetFromStorage(id.as_int,
                                                    kMapType_BlobId);
  BucketID result = {};
  if (internal_name.size() > kBucketIdStringSize) {
    result.as_int = HexStringToU64(internal_name);
  }

  return result;
}

/** get bucket ID from \a blob_id */
BucketID GetBucketIdFromBlobId(
                               BlobID id) {
  BucketID result = {};
  u32 target_node = GetBlobNodeId(id);
  if (target_node == rpc->node_id) {
    result = LocalGetBucketIdFromBlobId(context, id);
  } else {
    result = RpcCall<BucketID>(rpc, target_node, "RemoteGetBucketIdFromBlobId",
                               id);
  }

  return result;
}

/** get bucket information from \a bucket_id */
BucketInfo *LocalGetBucketInfoById(BucketID id) {
  BucketInfo *result = LocalGetBucketInfoByIndex(id.bits.index);

  return result;
}

/** get BLOB IDs from \a bucket_id */
std::vector<BlobID> GetBlobIds(
                               BucketID bucket_id) {
  std::vector<BlobID> result;
  u32 target_node = bucket_id.bits.node_id;
  if (target_node == rpc->node_id) {
    result = LocalGetBlobIds(context, bucket_id);
  } else {
    result = RpcCall<std::vector<BlobID>>(rpc, target_node, "RemoteGetBlobIds",
                                          bucket_id);
  }

  return result;
}

/** get virtual bucket information by \a index */
VBucketInfo *GetVBucketInfoByIndex(u32 index) {
  VBucketInfo *info_array =
    (VBucketInfo *)((u8 *)mdm + vbucket_info_offset);
  VBucketInfo *result = info_array + index;

  return result;
}

/**
 * Returns an available BucketID and marks it as in use in the MDM.
 *
 * Assumes bucket_mutex is already held by the caller.
 */
BucketID LocalGetNextFreeBucketId(
                                  const std::string &name) {
  mdm = GetMetadataManagerFromContext(context);
  BucketID result = {};

  if (num_buckets < max_buckets) {
    result = first_free_bucket;

    if (!IsNullBucketId(result)) {
      BucketInfo *info = LocalGetBucketInfoByIndex(result.bits.index);
      info->blobs = {};
      info->ref_count.store(1);
      info->active = true;
      first_free_bucket = info->next_free;
      num_buckets++;
    }
  } else {
    LOG(ERROR) << "Exceeded max allowed buckets. "
               << "Increase max_buckets_per_node in the Hermes configuration."
               << std::endl;
  }

  if (!IsNullBucketId(result)) {
    // NOTE(chogan): Add metadata entry
    LocalPutBucketId(name, result);
  }

  return result;
}

/** get or create a bucket ID locally */
BucketID LocalGetOrCreateBucketId(
                                  const std::string &name) {
  mdm = GetMetadataManagerFromContext(context);
  bucket_mutex.Lock();;
  BucketID result = LocalGetBucketId(context, name.c_str());

  if (result.as_int != 0) {
    LOG(INFO) << "Opening Bucket '" << name << "'" << std::endl;
    LocalIncrementRefcount(context, result);
  } else {
    LOG(INFO) << "Creating Bucket '" << name << "'" << std::endl;
    result = LocalGetNextFreeBucketId(context, name);
  }
  bucket_mutex.Unlock();

  return result;
}

/** get or create a bucket ID */
BucketID GetOrCreateBucketId(
                             const std::string &name) {
  mdm = GetMetadataManagerFromContext(context);
  u32 target_node = HashString(rpc, name.c_str());
  BucketID result = {};
  if (target_node == rpc->node_id) {
    result = LocalGetOrCreateBucketId(context, name);
  } else {
    result = RpcCall<BucketID>(rpc, target_node, "RemoteGetOrCreateBucketId",
                               name);
  }

  return result;
}

/**
 * Returns an available VBucketID and marks it as in use in the MDM.
 *
 * Assumes MetadataManager::vbucket_mutex is already held by the caller.
 */
VBucketID LocalGetNextFreeVBucketId(
                                    const std::string &name) {
  mdm = GetMetadataManagerFromContext(context);
  VBucketID result = {};

  // TODO(chogan): Could replace this with lock-free version if/when it matters

  if (num_vbuckets < max_vbuckets) {
    result = first_free_vbucket;
    if (!IsNullVBucketId(result)) {
      VBucketInfo *info = GetVBucketInfoByIndex(result.bits.index);
      info->blobs = {};
      memset(info->traits, 0, sizeof(TraitID) * kMaxTraitsPerVBucket);
      info->ref_count.store(1);
      info->active = true;
      first_free_vbucket = info->next_free;
      num_vbuckets++;
    }
  } else {
    LOG(ERROR) << "Exceeded max allowed vbuckets. "
               << "Increase max_vbuckets_per_node in the Hermes configuration."
               << std::endl;
  }
  if (!IsNullVBucketId(result)) {
    LocalPutVBucketId(name, result);
  }

  return result;
}

/** get or create a virtual bucket ID locally */
VBucketID LocalGetOrCreateVBucketId(
                                    const std::string &name) {
  mdm = GetMetadataManagerFromContext(context);

  vbucket_mutex.Lock();
  VBucketID result = LocalGetVBucketId(context, name.c_str());

  if (result.as_int != 0) {
    LOG(INFO) << "Opening VBucket '" << name << "'" << std::endl;
    LocalIncrementRefcount(context, result);
  } else {
    LOG(INFO) << "Creating VBucket '" << name << "'" << std::endl;
    result = LocalGetNextFreeVBucketId(context, name);
  }
  vbucket_mutex.Unlock();

  return result;
}

/** get or create a virtual bucket ID */
VBucketID GetOrCreateVBucketId(
                               const std::string &name) {
  VBucketID result = {};
  mdm = GetMetadataManagerFromContext(context);
  u32 target_node = HashString(rpc, name.c_str());
  if (target_node == rpc->node_id) {
    result = LocalGetOrCreateVBucketId(context, name);
  } else {
    result = RpcCall<VBucketID>(rpc, target_node, "RemoteGetOrCreateVBucketId",
                                name);
  }

  return result;
}

/** copy IDs */
void CopyIds(u64 *dest, u64 *src, u32 count) {
  static_assert(sizeof(BlobID) == sizeof(BufferID));
  for (u32 i = 0; i < count; ++i) {
    dest[i] = src[i];
  }
}

void ReplaceBlobIdInBucket(
                           BucketID bucket_id, BlobID old_blob_id,
                           BlobID new_blob_id) {
  u32 target_node = bucket_id.bits.node_id;
  if (target_node == rpc->node_id) {
    LocalReplaceBlobIdInBucket(context, bucket_id, old_blob_id, new_blob_id);
  } else {
    RpcCall<bool>(rpc, target_node, "RemoteReplaceBlobIdInBucket", bucket_id,
                  old_blob_id, new_blob_id);
  }
}

/** add BLOB ID to bucket */
void AddBlobIdToBucket(BlobID blob_id,
                       BucketID bucket_id) {
  u32 target_node = bucket_id.bits.node_id;

  if (target_node == rpc->node_id) {
    LocalAddBlobIdToBucket(bucket_id, blob_id);
  } else {
    RpcCall<bool>(rpc, target_node, "RemoteAddBlobIdToBucket", bucket_id,
                  blob_id);
  }
}

/** add BLOB ID to virtual bucket */
void AddBlobIdToVBucket(BlobID blob_id,
                        VBucketID vbucket_id) {
  u32 target_node = vbucket_id.bits.node_id;

  if (target_node == rpc->node_id) {
    LocalAddBlobIdToVBucket(vbucket_id, blob_id);
  } else {
    RpcCall<bool>(rpc, target_node, "RemoteAddBlobIdToVBucket", vbucket_id,
                  blob_id);
  }
}

/** allocate buffer ID list */
u32 AllocateBufferIdList(
                         u32 target_node,
                         const std::vector<BufferID> &buffer_ids) {
  mdm = GetMetadataManagerFromContext(context);
  u32 result = 0;

  if (target_node == rpc->node_id) {
    result = LocalAllocateBufferIdList(buffer_ids);
  } else {
    result = RpcCall<u32>(rpc, target_node, "RemoteAllocateBufferIdList",
                          buffer_ids);
  }

  return result;
}

/** get buffer ID list */
void GetBufferIdList(
                     BlobID blob_id,
                     BufferIdArray *buffer_ids) {
  mdm = GetMetadataManagerFromContext(context);
  u32 target_node = GetBlobNodeId(blob_id);

  if (target_node == rpc->node_id) {
    LocalGetBufferIdList(blob_id, buffer_ids);
  } else {
    std::vector<BufferID> result =
      RpcCall<std::vector<BufferID>>(rpc, target_node, "RemoteGetBufferIdList",
                                     blob_id);
    buffer_ids->ids = PushArray<BufferID>(result.size());
    buffer_ids->length = (u32)result.size();
    CopyIds((u64 *)buffer_ids->ids, (u64 *)result.data(), result.size());
  }
}

/** get buffer ID list as vector */
std::vector<BufferID> GetBufferIdList(BlobID blob_id) {
  mdm = GetMetadataManagerFromContext(context);
  u32 target_node = GetBlobNodeId(blob_id);

  std::vector<BufferID> result;

  if (target_node == rpc->node_id) {
    result = LocalGetBufferIdList(blob_id);
  } else {
    result = RpcCall<std::vector<BufferID>>(rpc, target_node,
                                            "RemoteGetBufferIdList", blob_id);
  }

  return result;
}

/** get buffer IDs from BLOB id */
BufferIdArray GetBufferIdsFromBlobId(BlobID blob_id,
                                     u32 **sizes) {
  BufferIdArray result = {};
  GetBufferIdList(context, rpc, blob_id, &result);

  if (sizes) {
    u32 *buffer_sizes = PushArray<u32>(result.length);
    for (u32 i = 0; i < result.length; ++i) {
      buffer_sizes[i] = GetBufferSize(context, rpc, result.ids[i]);
    }
    *sizes = buffer_sizes;
  }

  return result;
}

/** create BLOB metadata locally */
void LocalCreateBlobMetadata(const std::string &blob_name, BlobID blob_id,
                             TargetID effective_target) {
  LocalPut(blob_name.c_str(), blob_id.as_int, kMapType_BlobId);
  BlobInfo blob_info = {};
  blob_info.stats.frequency = 1;
  blob_info.stats.recency = clock++;
  blob_info.effective_target = effective_target;

  if (effective_target != kSwapTargetId) {
    assert(blob_id.bits.node_id == (int)effective_target.bits.node_id);
    Target *target = GetTargetFromId(context, effective_target);
    target->effective_blobs_lock.Lock();
    AppendToChunkedIdList(&target->effective_blobs, blob_id.as_int);
    target->effective_blobs_lock.Unlock();
  }

  LocalPut(blob_id, blob_info);
}

/** create BLOB metadata */
void CreateBlobMetadata(const std::string &blob_name, BlobID blob_id,
                        TargetID effective_target) {
  mdm = GetMetadataManagerFromContext(context);
  u32 target_node = GetBlobNodeId(blob_id);
  if (target_node == rpc->node_id) {
    LocalCreateBlobMetadata(context, blob_name, blob_id, effective_target);
  } else {
    RpcCall<bool>(rpc, target_node, "RemoteCreateBlobMetadata", blob_name,
                  blob_id, effective_target);
  }
}

/** attach BLOB to bucket */
void AttachBlobToBucket(const char *blob_name, BucketID bucket_id,
                        const std::vector<BufferID> &buffer_ids,
                        TargetID effective_target, bool is_swap_blob,
                        bool called_from_buffer_organizer) {
  mdm = GetMetadataManagerFromContext(context);

  std::string internal_name = MakeInternalBlobName(blob_name, bucket_id);
  int target_node = HashString(rpc, internal_name.c_str());

  BlobID blob_id = {};
  if (called_from_buffer_organizer) {
    blob_id = GetBlobId(context, rpc, blob_name, bucket_id);

    if (!IsNullBlobId(blob_id)) {
      // Remove old BlobID from the bucket's list of Blobs
      RemoveBlobFromBucketInfo(context, rpc, bucket_id, blob_id);

      // Release the IDs that represented the SwapBlob info
      FreeBufferIdList(context, rpc, blob_id);
    } else {
      LOG(WARNING) << "Expected to find BlobID " << blob_id.as_int
                   << " in Map but didn't." << std::endl;
    }
  }

  // NOTE(chogan): A negative node_id indicates a swap blob
  blob_id.bits.node_id = is_swap_blob ? -target_node : target_node;
  blob_id.bits.buffer_ids_offset = AllocateBufferIdList(context, rpc,
                                                        target_node,
                                                        buffer_ids);
  CreateBlobMetadata(context, rpc, internal_name, blob_id, effective_target);
  AddBlobIdToBucket(rpc, blob_id, bucket_id);
}

/** free buffer ID list */
void FreeBufferIdList(BlobID blob_id) {
  u32 target_node = GetBlobNodeId(blob_id);
  if (target_node == rpc->node_id) {
    LocalFreeBufferIdList(context, blob_id);
  } else {
    RpcCall<bool>(rpc, target_node, "RemoteFreeBufferIdList", blob_id);
  }
}

/** delete BLOB metadata locally */
void LocalDeleteBlobMetadata(const char *blob_name,
                             BlobID blob_id, BucketID bucket_id) {
  LocalDeleteBlobId(blob_name, bucket_id);
  LocalDeleteBlobInfo(blob_id);
}

/** wait for outstanding BLOB operations */
void WaitForOutstandingBlobOps(BlobID blob_id) {
  BlobInfo *blob_info = GetBlobInfoPtr(blob_id);
  if (blob_info) {
    blob_info->lock.Lock();
    blob_info->lock.Unlock();
  } else {
    ReleaseBlobInfoPtr(mdm);
  }
}

/** destroy BLOB by name locally */
void LocalDestroyBlobByName(const char *blob_name, BlobID blob_id,
                            BucketID bucket_id) {
  mdm = GetMetadataManagerFromContext(context);

  // NOTE(chogan): Holding the blob_info_map_mutex
  WaitForOutstandingBlobOps(blob_id);

  if (!BlobIsInSwap(blob_id)) {
    std::vector<BufferID> buffer_ids = GetBufferIdList(context, rpc, blob_id);
    ReleaseBuffers(context, rpc, buffer_ids);
  } else {
    // TODO(chogan): Invalidate swap region once we have a SwapManager
  }

  FreeBufferIdList(context, rpc, blob_id);

  LocalDeleteBlobMetadata(blob_name, blob_id, bucket_id);

  ReleaseBlobInfoPtr(mdm);
}

/** destroy BLOB by ID locally */
void LocalDestroyBlobById(BlobID blob_id, BucketID bucket_id) {
  std::string blob_name = LocalGetBlobNameFromId(context, blob_id);
  if (blob_name.size() > 0) {
    LocalDestroyBlobByName(context, rpc, blob_name.c_str(), blob_id, bucket_id);
  } else {
    DLOG(INFO) << "Expected to find blob_id " << blob_id.as_int
               << " in Map but didn't" << std::endl;
  }
}

void RemoveBlobFromBucketInfo(BucketID bucket_id, BlobID blob_id) {
  u32 target_node = bucket_id.bits.node_id;
  if (target_node == rpc->node_id) {
    LocalRemoveBlobFromBucketInfo(context, bucket_id, blob_id);
  } else {
    RpcCall<bool>(rpc, target_node, "RemoteRemoveBlobFromBucketInfo", bucket_id,
                  blob_id);
  }
}

/** destroy BLOB by name */
void DestroyBlobByName(BucketID bucket_id, const std::string &blob_name) {
  BlobID blob_id = GetBlobId(context, rpc, blob_name, bucket_id);
  if (!IsNullBlobId(blob_id)) {
    u32 blob_id_target_node = GetBlobNodeId(blob_id);

    if (blob_id_target_node == rpc->node_id) {
      LocalDestroyBlobByName(context, rpc, blob_name.c_str(), blob_id,
                             bucket_id);
    } else {
      RpcCall<bool>(rpc, blob_id_target_node, "RemoteDestroyBlobByName",
                    blob_name, blob_id, bucket_id);
    }
    RemoveBlobFromBucketInfo(context, rpc, bucket_id, blob_id);
  }
}

/** rename BLOB */
void RenameBlob(const std::string &old_name, const std::string &new_name,
                BucketID bucket_id) {
  mdm = GetMetadataManagerFromContext(context);
  BlobID blob_id = GetBlobId(context, rpc, old_name, bucket_id);
  if (!IsNullBlobId(blob_id)) {
    DeleteBlobId(rpc, old_name, bucket_id);
    PutBlobId(rpc, new_name, blob_id, bucket_id);
  } else {
    LOG(ERROR) << "Invalid BlobID for Blob '" << old_name << "'" << std::endl;
  }
}

/** does \a bucket_id bucket contain \a blob_name BLOB? */
bool ContainsBlob(BucketID bucket_id, const std::string &blob_name) {
  BlobID blob_id = GetBlobId(context, rpc, blob_name, bucket_id);
  bool result = false;

  if (!IsNullBlobId(blob_id)) {
    u32 target_node = bucket_id.bits.node_id;
    if (target_node == rpc->node_id) {
      result = LocalContainsBlob(context, bucket_id, blob_id);
    } else {
      result = RpcCall<bool>(rpc, target_node, "RemoteContainsBlob", bucket_id,
                             blob_id);
    }
  }

  return result;
}

/** destroy BLOB by ID */
void DestroyBlobById(BlobID id,
                     BucketID bucket_id) {
  u32 target_node = GetBlobNodeId(id);
  if (target_node == rpc->node_id) {
    LocalDestroyBlobById(context, rpc, id, bucket_id);
  } else {
    RpcCall<bool>(rpc, target_node, "RemoteDestroyBlobById", id, bucket_id);
  }
}

/** destroy bucket */
bool DestroyBucket(const char *name, BucketID bucket_id) {
  u32 target_node = bucket_id.bits.node_id;
  bool destroyed = false;
  if (target_node == rpc->node_id) {
    destroyed = LocalDestroyBucket(context, rpc, name, bucket_id);
  } else {
    destroyed = RpcCall<bool>(rpc, target_node, "RemoteDestroyBucket",
                              std::string(name), bucket_id);
  }

  return destroyed;
}

/** destroy virtual bucket */
bool DestroyVBucket(const char *name, VBucketID vbucket_id) {
  u32 target_node = vbucket_id.bits.node_id;
  bool destroyed = false;
  if (target_node == rpc->node_id) {
    destroyed = LocalDestroyVBucket(context, name, vbucket_id);
  } else {
    destroyed = RpcCall<bool>(rpc, target_node, "RemoteDestroyVBucket",
                              std::string(name), vbucket_id);
  }

  return destroyed;
}

/** rename bucket locally */
void LocalRenameBucket(BucketID id, const std::string &old_name,
                       const std::string &new_name) {
  mdm = GetMetadataManagerFromContext(context);
  DeleteBucketId(rpc, old_name);
  PutBucketId(rpc, new_name, id);
}

/** rename bucket */
void RenameBucket(BucketID id,
                  const std::string &old_name, const std::string &new_name) {
  u32 target_node = id.bits.node_id;
  if (target_node == rpc->node_id) {
    LocalRenameBucket(context, rpc, id, old_name, new_name);
  } else {
    RpcCall<bool>(rpc, target_node, "RemoteRenameBucket", id, old_name,
                  new_name);
  }
}

/** increment reference count locally */
void LocalIncrementRefcount(BucketID id) {
  mdm = GetMetadataManagerFromContext(context);
  BucketInfo *info = LocalGetBucketInfoById(id);
  info->ref_count.fetch_add(1);
}

/** decrement reference count locally */
void LocalDecrementRefcount(BucketID id) {
  mdm = GetMetadataManagerFromContext(context);
  BucketInfo *info = LocalGetBucketInfoById(id);
  info->ref_count.fetch_sub(1);
  assert(info->ref_count.load() >= 0);
}

/** decrement reference count  */
void DecrementRefcount(BucketID id) {
  u32 target_node = id.bits.node_id;
  if (target_node == rpc->node_id) {
    LocalDecrementRefcount(context, id);
  } else {
    RpcCall<bool>(rpc, target_node, "RemoteDecrementRefcount", id);
  }
}

/** get remaning target capacity locally */
u64 LocalGetRemainingTargetCapacity(TargetID id) {
  Target *target = GetTargetFromId(context, id);
  u64 result = target->remaining_space.load();

  return result;
}

/** get local system view state from \a mdm metadata mamanger */
SystemViewState *GetLocalSystemViewState() {
  SystemViewState *result =
    (SystemViewState *)((u8 *)mdm + system_view_state_offset);

  return result;
}

/** get local system view state from \a context */
SystemViewState *GetLocalSystemViewState(SharedMemoryContext *context) {
  mdm = GetMetadataManagerFromContext(context);
  SystemViewState *result = GetLocalSystemViewState(mdm);

  return result;
}

/** get global device capacities locally */
std::vector<u64> LocalGetGlobalDeviceCapacities(SharedMemoryContext *context) {
  GlobalSystemViewState *global_svs = GetGlobalSystemViewState(context);

  std::vector<u64> result(global_svs->num_devices);
  for (size_t i = 0; i < result.size(); ++i) {
    result[i] = global_svs->bytes_available[i].load();
  }

  return result;
}

/** get global device capacities */
std::vector<u64> GetGlobalDeviceCapacities(
                                           RpcContext *rpc) {
  mdm = GetMetadataManagerFromContext(context);
  u32 target_node = global_system_view_state_node_id;

  std::vector<u64> result;

  if (target_node == rpc->node_id) {
    result = LocalGetGlobalDeviceCapacities(context);
  } else {
    result = RpcCall<std::vector<u64>>(rpc, target_node,
                                       "RemoteGetGlobalDeviceCapacities");
  }

  return result;
}

/** get global system view state from \a context */
GlobalSystemViewState *GetGlobalSystemViewState(SharedMemoryContext *context) {
  mdm = GetMetadataManagerFromContext(context);
  GlobalSystemViewState *result =
    (GlobalSystemViewState *)((u8 *)mdm + global_system_view_state_offset);
  assert((u8 *)result != (u8 *)mdm);

  return result;
}

/** update global system view state locally */
std::vector<ViolationInfo>
LocalUpdateGlobalSystemViewState(u32 node_id,
                                 std::vector<i64> adjustments) {
  std::vector<ViolationInfo> result;
  for (size_t device_idx = 0; device_idx < adjustments.size(); ++device_idx) {
    GlobalSystemViewState *state = GetGlobalSystemViewState(context);
    u32 target_idx = ((node_id - 1) * adjustments.size()) + device_idx;
    if (adjustments[device_idx]) {
      state->bytes_available[target_idx].fetch_add(adjustments[device_idx]);
      DLOG(INFO) << "DeviceID " << device_idx << " on node " << node_id
                 << " adjusted by " << adjustments[device_idx] << " bytes\n";
    }

    // Collect devices for which to trigger the BufferOrganizer if the
    // capacities are beyond the min/max thresholds
    float percentage_available = 0.0f;
    if (state->bytes_available[target_idx] > 0) {
      percentage_available = ((f32)state->bytes_available[target_idx].load() /
                              (f32)state->capacities[device_idx]);
    }

    ViolationInfo info = {};
    float percentage_violation = 0.0f;
    f32 percentage_used = 1.0f - percentage_available;
    float min_threshold = state->bo_capacity_thresholds[device_idx].min;
    float max_threshold = state->bo_capacity_thresholds[device_idx].max;

    if (percentage_used > max_threshold) {
      percentage_violation = percentage_used - max_threshold;
      info.violation = ThresholdViolation::kMax;
    }
    if (percentage_used < min_threshold) {
      percentage_violation = min_threshold - percentage_used;
      info.violation = ThresholdViolation::kMin;
    }

    if (percentage_violation > 0.0f) {
      TargetID target_id = {};
      target_id.bits.node_id = node_id;
      target_id.bits.device_id = (DeviceID)device_idx;
      // TODO(chogan): This needs to change when we support num_devices !=
      // num_targets
      target_id.bits.index = device_idx;

      info.target_id = target_id;
      info.violation_size =
        (size_t)(percentage_violation * state->capacities[device_idx]);
      result.push_back(info);
    }
  }

  return result;
}

/** update global system view state */
void UpdateGlobalSystemViewState(
                                 RpcContext *rpc) {
  mdm = GetMetadataManagerFromContext(context);
  BufferPool *pool = GetBufferPoolFromContext(context);

  bool update_needed = false;
  std::vector<i64> adjustments(pool->num_devices);
  for (size_t i = 0; i < adjustments.size(); ++i) {
    adjustments[i] = pool->capacity_adjustments[i].exchange(0);
    if (adjustments[i] != 0) {
      update_needed = true;
    }
  }

  std::vector<ViolationInfo> devices_to_organize;
  if (update_needed) {
    u32 target_node = global_system_view_state_node_id;
    if (target_node == rpc->node_id) {
      devices_to_organize =
        LocalUpdateGlobalSystemViewState(context, rpc->node_id, adjustments);
    } else {
      devices_to_organize =
        RpcCall<std::vector<ViolationInfo>>(rpc, target_node,
                                            "RemoteUpdateGlobalSystemViewState",
                                            adjustments);
    }
  }

  for (size_t i = 0; i < devices_to_organize.size(); ++i) {
    EnforceCapacityThresholds(context, rpc, devices_to_organize[i]);
  }
}

/** find target ID from device ID */
TargetID FindTargetIdFromDeviceId(const std::vector<TargetID> &targets,
                                  DeviceID device_id) {
  TargetID result = {};
  // TODO(chogan): @optimization Inefficient O(n)
  for (size_t target_index = 0;
       target_index < targets.size();
       ++target_index) {
    if (targets[target_index].bits.device_id == device_id) {
      result = targets[target_index];
      break;
    }
  }

  return result;
}

/** get offset from metadata manager */
static ptrdiff_t GetOffsetFromMdm(void *ptr) {
  assert((u8 *)ptr >= (u8 *)mdm);
  ptrdiff_t result = (u8 *)ptr - (u8 *)mdm;

  return result;
}

/** create system view state */
SystemViewState *CreateSystemViewState(Config *config) {
  SystemViewState *result = PushClearedStruct<SystemViewState>(arena);
  result->num_devices = config->num_devices;
  for (int i = 0; i < result->num_devices; ++i) {
    result->capacities[i] = config->capacities[i];
    result->bytes_available[i] = config->capacities[i];

    // Min and max thresholds
    result->bo_capacity_thresholds[i] = config->bo_capacity_thresholds[i];
  }

  return result;
}

/** create global system view state */
GlobalSystemViewState *CreateGlobalSystemViewState(Config *config) {
  GlobalSystemViewState *result =
    PushClearedStruct<GlobalSystemViewState>(arena);
  result->num_devices = config->num_devices;

  for (int i = 0; i < result->num_devices; ++i) {
    result->capacities[i] = config->capacities[i];
    // Min and max thresholds
    result->bo_capacity_thresholds[i] = config->bo_capacity_thresholds[i];
  }
  size_t num_targets = config->num_devices * rpc->num_nodes;
  result->num_targets = num_targets;
  result->bytes_available =
    PushClearedArray<std::atomic<u64>>(num_targets);

  for (u32 node_idx = 0; node_idx < rpc->num_nodes; ++node_idx) {
    for (int device_idx = 0; device_idx < result->num_devices; ++device_idx) {
      u64 index = (node_idx * result->num_devices) + device_idx;
      result->bytes_available[index].store(result->capacities[device_idx]);
    }
  }

  return result;
}

/** get swap file name */
std::string GetSwapFilename(u32 node_id) {
  char *prefix = (char *)((u8 *)mdm + swap_filename_prefix_offset);
  char *suffix = (char *)((u8 *)mdm + swap_filename_suffix_offset);
  std::string result = (prefix + std::to_string(node_id) + suffix);

  return result;
}

/** swap BLOB to vector */
std::vector<BufferID> SwapBlobToVec(SwapBlob swap_blob) {
  // TODO(chogan): @metaprogramming Improve this, since it currently only works
  // if each member in SwapBlob (plus padding) is eight bytes.
  static_assert((sizeof(SwapBlob) / 8) == SwapBlobMembers_Count);

  std::vector<BufferID> result(SwapBlobMembers_Count);
  result[SwapBlobMembers_NodeId].as_int = swap_blob.node_id;
  result[SwapBlobMembers_Offset].as_int = swap_blob.offset;
  result[SwapBlobMembers_Size].as_int = swap_blob.size;
  result[SwapBlobMembers_BucketId].as_int = swap_blob.bucket_id.as_int;

  return result;
}

/** vector to swap BLOB */
SwapBlob VecToSwapBlob(std::vector<BufferID> &vec) {
  SwapBlob result = {};

  if (vec.size() == SwapBlobMembers_Count) {
    result.node_id = (u32)vec[SwapBlobMembers_NodeId].as_int;
    result.offset = vec[SwapBlobMembers_Offset].as_int;
    result.size = vec[SwapBlobMembers_Size].as_int;
    result.bucket_id.as_int = vec[SwapBlobMembers_BucketId].as_int;
  } else {
    // TODO(chogan): @errorhandling
    HERMES_NOT_IMPLEMENTED_YET;
  }

  return result;
}

/** ID array to swap BLOB */
SwapBlob IdArrayToSwapBlob(BufferIdArray ids) {
  SwapBlob result = {};

  if (ids.length == SwapBlobMembers_Count) {
    result.node_id = (u32)ids.ids[SwapBlobMembers_NodeId].as_int;
    result.offset = ids.ids[SwapBlobMembers_Offset].as_int;
    result.size = ids.ids[SwapBlobMembers_Size].as_int;
    result.bucket_id.as_int = ids.ids[SwapBlobMembers_BucketId].as_int;
  } else {
    // TODO(chogan): @errorhandling
    HERMES_NOT_IMPLEMENTED_YET;
  }

  return result;
}

/** initialize metadata manager */
MetadataManager::MetadataManager(RpcContext *rpc, Config *config) :
    config_(config), rpc_(rpc) {
  // NOTE(chogan): All MetadataManager offsets are relative to the address of
  // the MDM itself.
  u32 node_id = rpc->node_id_;
  
  map_seed = 0x4E58E5DF;
  SeedHashForStorage(map_seed);

  system_view_state_update_interval_ms =
    config->system_view_state_update_interval_ms;

  // Initialize SystemViewState

  SystemViewState *sv_state = CreateSystemViewState(config);
  system_view_state_offset = GetOffsetFromMdm(sv_state);

  // Initialize Global SystemViewState

  if (node_id == 1) {
    // NOTE(chogan): Only Node 1 has the Global SystemViewState
    GlobalSystemViewState *global_state =
      CreateGlobalSystemViewState(rpc, config);
    global_system_view_state_offset = GetOffsetFromMdm(global_state);
  }
  global_system_view_state_node_id = 1;

  // Initialize BucketInfo array

  BucketInfo *buckets = PushArray<BucketInfo>(arena,
                                              config->max_buckets_per_node);
  bucket_info_offset = GetOffsetFromMdm(buckets);
  first_free_bucket.bits.node_id = (u32)node_id;
  first_free_bucket.bits.index = 0;
  num_buckets = 0;
  max_buckets = config->max_buckets_per_node;

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
  vbucket_info_offset = GetOffsetFromMdm(vbuckets);
  first_free_vbucket.bits.node_id = (u32)node_id;
  first_free_vbucket.bits.index = 0;
  num_vbuckets = 0;
  max_vbuckets = config->max_vbuckets_per_node;

  for (u32 i = 0; i < config->max_vbuckets_per_node; ++i) {
    VBucketInfo *info = vbuckets + i;
    info->active = false;
    info->async_flush_count.store(0);

    if (i == config->max_vbuckets_per_node - 1) {
      info->next_free.as_int = 0;
    } else {
      info->next_free.bits.node_id = (u32)node_id;
      info->next_free.bits.index = i + 1;
    }
  }
}

/** get virtual bucket information by index locally */
VBucketInfo* MetadataManager::LocalGetVBucketInfoByIndex(u32 index) {
  VBucketInfo *info_array =
      (VBucketInfo *)((u8 *)mdm + vbucket_info_offset);
  VBucketInfo *result = info_array + index;
  return result;
}

/** get virtual bucket information by id locally */
VBucketInfo* MetadataManager::LocalGetVBucketInfoById(VBucketID id) {
  VBucketInfo *result = LocalGetVBucketInfoByIndex(id.bits.index);
  return result;
}

/** increment reference counter locally */
void MetadataManager::LocalIncrementRefcount(VBucketID id) {
  mdm = GetMetadataManagerFromContext(context);
  VBucketInfo *info = LocalGetVBucketInfoById(id);
  info->ref_count.fetch_add(1);
}

/** decrement reference counter locally */
void MetadataManager::LocalDecrementRefcount(VBucketID id) {
  mdm = GetMetadataManagerFromContext(context);
  VBucketInfo *info = LocalGetVBucketInfoById(id);
  info->ref_count.fetch_sub(1);
  assert(info->ref_count.load() >= 0);
}

/** decrement reference counter */
void MetadataManager::DecrementRefcount(VBucketID id) {
  u32 target_node = id.bits.node_id;
  if (target_node == rpc->node_id) {
    LocalDecrementRefcount(context, id);
  } else {
    RpcCall<bool>(rpc, target_node, "RemoteDecrementRefcountVBucket", id);
  }
}

/** get relative node ID */
u32 MetadataManager::GetRelativeNodeId(int offset) {
  int result = rpc->node_id + offset;
  assert(result >= 0);
  assert(result <= (int)(rpc->num_nodes + 1));

  if (result > (int)rpc->num_nodes) {
    result = 1;
  } else if (result == 0) {
    result = rpc->num_nodes;
  }

  return (u32)result;
}

/** get next node */
u32 MetadataManager::GetNextNode() {
  u32 result = GetRelativeNodeId(1);

  return result;
}

/** get previous node */
u32 MetadataManager::GetPreviousNode() {
  u32 result = GetRelativeNodeId(-1);

  return result;
}

/** get node targets */
std::vector<TargetID> MetadataManager::GetNodeTargets(u32 target_node) {
  std::vector<TargetID> result;

  if (target_node == rpc->node_id) {
    result = LocalGetNodeTargets(context);
  } else {
    result = RpcCall<std::vector<TargetID>>(rpc, target_node,
                                            "RemoteGetNodeTargets");
  }

  return result;
}

/** get neighborhood node targets */
std::vector<TargetID> MetadataManager::GetNeighborhoodTargets() {
  // TODO(chogan): Inform the concept of "neighborhood" with a network topology
  // NOTE(chogan): For now, each node has 2 neighbors, NodeID-1 and NodeID+1
  // (wrapping around for nodes 1 and N).
  std::vector<TargetID> result;

  switch (rpc->num_nodes) {
    case 1: {
      // No neighbors
      break;
    }
    case 2: {
      // One neighbor
      u32 next_node = GetNextNode(rpc);
      result = GetNodeTargets(context, rpc, next_node);
      break;
    }
    default: {
      // Two neighbors
      u32 next_node = GetNextNode(rpc);
      std::vector<TargetID> next_targets = GetNodeTargets(context, rpc,
                                                          next_node);
      u32 prev_node = GetPreviousNode(rpc);
      std::vector<TargetID> prev_targets = GetNodeTargets(context, rpc,
                                                          prev_node);

      result.reserve(next_targets.size() + prev_targets.size());
      result.insert(result.end(), next_targets.begin(), next_targets.end());
      result.insert(result.end(), prev_targets.begin(), prev_targets.end());
    }
  }

  return result;
}

/** get remaining target capacity */
u64 MetadataManager::GetRemainingTargetCapacity(TargetID target_id) {
  u64 result = 0;
  u32 target_node = target_id.bits.node_id;

  if (target_node == rpc->node_id) {
    result = LocalGetRemainingTargetCapacity(context, target_id);
  } else {
    result = RpcCall<u64>(rpc, target_node, "RemoteGetRemainingTargetCapacity",
                          target_id);
  }

  return result;
}

/** get remaining target capacities */
std::vector<u64> MetadataManager::GetRemainingTargetCapacities(
    const std::vector<TargetID> &targets) {
  std::vector<u64> result(targets.size());

  for (size_t i = 0; i < targets.size(); ++i) {
    result[i] = GetRemainingTargetCapacity(context, rpc, targets[i]);
  }

  return result;
}

void MetadataManager::AttachBlobToVBucket(const char *blob_name,
                                          const char *bucket_name,
                                          VBucketID vbucket_id) {
  mdm = GetMetadataManagerFromContext(context);
  BucketID bucket_id = GetBucketId(context, rpc, bucket_name);
  BlobID blob_id = GetBlobId(context, rpc, blob_name, bucket_id);
  AddBlobIdToVBucket(rpc, blob_id, vbucket_id);
}

/** get bucket name by ID locally */
std::string
MetadataManager::LocalGetBucketNameById(BucketID blob_id) {
  mdm = GetMetadataManagerFromContext(context);
  std::string bucket_name =
      ReverseGetFromStorage(blob_id.as_int, kMapType_Bucket);
  return bucket_name;
}

std::vector<BlobID>
MetadataManager::GetBlobsFromVBucketInfo(VBucketID vbucket_id) {
  u32 target_node = vbucket_id.bits.node_id;
  if (target_node == rpc->node_id) {
    return LocalGetBlobsFromVBucketInfo(context, vbucket_id);
  } else {
    return RpcCall<std::vector<BlobID>>(
        rpc, target_node, "RemoteGetBlobsFromVBucketInfo", vbucket_id);
  }
}

void MetadataManager::RemoveBlobFromVBucketInfo(VBucketID vbucket_id,
                                                const char *blob_name,
                                                const char *bucket_name) {
  BucketID bucket_id = GetBucketId(context, rpc, bucket_name);
  BlobID blob_id = GetBlobId(context, rpc, blob_name, bucket_id);
  u32 target_node = vbucket_id.bits.node_id;
  if (target_node == rpc->node_id) {
    LocalRemoveBlobFromVBucketInfo(context, vbucket_id, blob_id);
  } else {
    RpcCall<bool>(rpc, target_node, "RemoteRemoveBlobFromVBucketInfo",
                  vbucket_id, blob_id);
  }
}

std::string MetadataManager::GetBucketNameById(BucketID id) {
  auto target_node = id.bits.node_id;
  if (target_node == rpc->node_id) {
    return LocalGetBucketNameById(context, id);
  } else {
    return RpcCall<std::string>(rpc, target_node, "RemoteGetBucketNameById",
                                id);
  }
}

/**
 * Returns the score calculated from @p stats.
 *
 * A high score indicates a "cold" Blob, or one that has not been accessed very
 * recently or frequently.
 *
 */
f32 MetadataManager::ScoringFunction(Stats *stats) {
  f32 recency_weight = 0.5;
  f32 frequency_weight = 0.5;
  // NOTE(chogan): A high relative_recency corresponds to a "cold" Blob.
  f32 relative_recency = clock - stats->recency;
  f32 result = (relative_recency * recency_weight -
                stats->frequency * frequency_weight);

  return result;
}

/** get number of outstanding flushing tasks locally */
int MetadataManager::LocalGetNumOutstandingFlushingTasks(VBucketID id) {
  mdm = GetMetadataManagerFromContext(context);
  vbucket_mutex.Lock();
  VBucketInfo *info = LocalGetVBucketInfoById(id);
  int result = 0;
  if (info) {
    result = info->async_flush_count;
  }
  vbucket_mutex.Unlock();

  return result;
}

/** get number of outstanding flushing tasks */
int MetadataManager::GetNumOutstandingFlushingTasks(VBucketID id) {
  u32 target_node = id.bits.node_id;
  int result = 0;

  if (target_node == rpc->node_id) {
    result = LocalGetNumOutstandingFlushingTasks(context, id);
  } else {
    result = RpcCall<int>(rpc, target_node,
                          "RemoteGetNumOutstandingFlushingTasks", id);
  }

  return result;
}

bool MetadataManager::LocalLockBlob(BlobID blob_id) {
  mdm = GetMetadataManagerFromContext(context);
  BlobInfo *blob_info = GetBlobInfoPtr(blob_id);
  if (blob_info) {
    blob_info->lock.Lock();
  } else {
    ReleaseBlobInfoPtr(mdm);
    return false;
  }
  if (blob_info->stop) {
    blob_info->lock.Unlock();
    LocalDelete(blob_id);
    LocalFreeBufferIdList(context, blob_id);
    ReleaseBlobInfoPtr(mdm);
    return false;
  }
  ReleaseBlobInfoPtr(mdm);
  return true;
}

bool MetadataManager::LocalUnlockBlob(BlobID blob_id) {
  mdm = GetMetadataManagerFromContext(context);
  BlobInfo *blob_info = GetBlobInfoPtr(blob_id);
  bool result = false;
  if (blob_info) {
    blob_info->lock.Unlock();
    result = true;
  }
  ReleaseBlobInfoPtr(mdm);
  return result;
}

/** lock BLOB */
bool MetadataManager::LockBlob(BlobID blob_id) {
  u32 target_node = GetBlobNodeId(blob_id);
  bool result;
  if (target_node == rpc->node_id) {
    result = LocalLockBlob(context, blob_id);
  } else {
    result = RpcCall<bool>(rpc, target_node, "RemoteLockBlob", blob_id);
  }
  return result;
}

/** unlock BLOB */
bool MetadataManager::UnlockBlob(BlobID blob_id) {
  u32 target_node = GetBlobNodeId(blob_id);
  bool result;
  if (target_node == rpc->node_id) {
    result = LocalUnlockBlob(context, blob_id);
  } else {
    result = RpcCall<bool>(rpc, target_node, "RemoteUnlockBlob", blob_id);
  }

  return result;
}

}  // namespace hermes
