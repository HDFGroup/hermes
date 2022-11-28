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

#ifndef HERMES_METADATA_MANAGEMENT_INTERNAL_H_
#define HERMES_METADATA_MANAGEMENT_INTERNAL_H_

#include "hermes_types.h"

namespace hermes {

bool IsNullBucketId(BucketID id);
bool IsNullVBucketId(VBucketID id);
bool IsNullBlobId(BlobID id);
bool IsNullTargetId(TargetID id);
TicketMutex *GetMapMutex(MetadataManager *mdm, MapType map_type);
VBucketID GetVBucketId(SharedMemoryContext *context, RpcContext *rpc,
                       const char *name);
u32 HashString(MetadataManager *mdm, RpcContext *rpc, const char *str);
MetadataManager *GetMetadataManagerFromContext(SharedMemoryContext *context);
BucketInfo *LocalGetBucketInfoByIndex(MetadataManager *mdm, u32 index);
VBucketInfo *GetVBucketInfoByIndex(MetadataManager *mdm, u32 index);
u32 AllocateBufferIdList(SharedMemoryContext *context, RpcContext *rpc,
                         u32 target_node,
                         const std::vector<BufferID> &buffer_ids);
std::vector<BufferID> GetBufferIdList(SharedMemoryContext *context,
                                      RpcContext *rpc, BlobID blob_id);
void FreeBufferIdList(SharedMemoryContext *context, RpcContext *rpc,
                      BlobID blob_id);

void LocalAddBlobIdToBucket(MetadataManager *mdm, BucketID bucket_id,
                            BlobID blob_id, bool track_stats = true);
void LocalAddBlobIdToVBucket(MetadataManager *mdm, VBucketID vbucket_id,
                             BlobID blob_id);
std::vector<BufferID> LocalGetBufferIdList(MetadataManager *mdm,
                                           BlobID blob_id);
void LocalGetBufferIdList(Arena *arena, MetadataManager *mdm, BlobID blob_id,
                          BufferIdArray *buffer_ids);
void LocalFreeBufferIdList(SharedMemoryContext *context, BlobID blob_id);
bool LocalDestroyBucket(SharedMemoryContext *context, RpcContext *rpc,
                        const char *bucket_name, BucketID bucket_id);
bool LocalDestroyVBucket(SharedMemoryContext *context, const char *vbucket_name,
                         VBucketID vbucket_id);
void LocalDestroyBlobById(SharedMemoryContext *context, RpcContext *rpc,
                          BlobID blob_id, BucketID bucket_id);
void LocalDestroyBlobByName(SharedMemoryContext *context, RpcContext *rpc,
                            const char *blob_name, BlobID blob_id,
                            BucketID bucket_id);
BucketID LocalGetNextFreeBucketId(SharedMemoryContext *context,
                                  const std::string &name);
u32 LocalAllocateBufferIdList(MetadataManager *mdm,
                              const std::vector<BufferID> &buffer_ids);
void LocalRenameBucket(SharedMemoryContext *context, RpcContext *rpc,
                       BucketID id, const std::string &old_name,
                       const std::string &new_name);
bool LocalContainsBlob(SharedMemoryContext *context, BucketID bucket_id,
                       BlobID blob_id);
void LocalRemoveBlobFromBucketInfo(SharedMemoryContext *context,
                                   BucketID bucket_id, BlobID blob_id);
void LocalIncrementRefcount(SharedMemoryContext *context, BucketID id);
void LocalDecrementRefcount(SharedMemoryContext *context, BucketID id);
void LocalIncrementRefcount(SharedMemoryContext *context, VBucketID id);
void LocalDecrementRefcount(SharedMemoryContext *context, VBucketID id);

u64 LocalGet(MetadataManager *mdm, const char *key, MapType map_type);
void LocalPut(MetadataManager *mdm, const char *key, u64 val, MapType map_type);
void LocalDelete(MetadataManager *mdm, const char *key, MapType map_type);

u64 LocalGetRemainingTargetCapacity(SharedMemoryContext *context, TargetID id);
std::vector<ViolationInfo> LocalUpdateGlobalSystemViewState(
    SharedMemoryContext *context, u32 node_id, std::vector<i64> adjustments);
GlobalSystemViewState *GetGlobalSystemViewState(SharedMemoryContext *context);
std::vector<u64> LocalGetGlobalDeviceCapacities(SharedMemoryContext *context);
std::vector<u64> GetGlobalDeviceCapacities(SharedMemoryContext *context,
                                           RpcContext *rpc);
void UpdateGlobalSystemViewState(SharedMemoryContext *context, RpcContext *rpc);

void StartGlobalSystemViewStateUpdateThread(SharedMemoryContext *context,
                                            RpcContext *rpc, Arena *arena,
                                            double sleep_ms);

void InitMetadataStorage(SharedMemoryContext *context, MetadataManager *mdm,
                         Arena *arena, Config *config);

std::string GetSwapFilename(MetadataManager *mdm, u32 node_id);
std::vector<BlobID> LocalGetBlobIds(SharedMemoryContext *context,
                                    BucketID bucket_id);
std::vector<TargetID> LocalGetNodeTargets(SharedMemoryContext *context);
u32 GetNextNode(RpcContext *rpc);
u32 GetPreviousNode(RpcContext *rpc);
BucketID LocalGetBucketIdFromBlobId(SharedMemoryContext *context, BlobID id);
std::string LocalGetBlobNameFromId(SharedMemoryContext *context,
                                   BlobID blob_id);
BucketID LocalGetOrCreateBucketId(SharedMemoryContext *context,
                                  const std::string &name);
VBucketID LocalGetOrCreateVBucketId(SharedMemoryContext *context,
                                    const std::string &name);
f32 LocalGetBlobImportanceScore(SharedMemoryContext *context, BlobID blob_id);
f32 GetBlobImportanceScore(SharedMemoryContext *context, RpcContext *rpc,
                           BlobID blob_id);

/**
 * Faster version of std::stoull.
 *
 * This is 4.1x faster than std::stoull. Since we generate all the numbers that
 * we use this function on, we can guarantee the following:
 *   - The size will always be kBucketIdStringSize.
 *   - The number will always be unsigned and within the range of a u64.
 *   - There will never be invalid characters passed in (only 0-9 and a-f).
 *
 * Avoiding all this input sanitization and error checking is how we can get a
 * 4.1x speedup.
 *
 * \param s A string with size at least kBucketIdStringSize, where the first
 *          kBucketIdStringSize characters consist only of 0-9 and a-f.
 *
 * \return The u64 representation of the first kBucketIdStringSize characters of
 *         \p s.
 */
u64 HexStringToU64(const std::string &s);
std::string MakeInternalBlobName(const std::string &name, BucketID id);

void LocalRemoveBlobFromVBucketInfo(SharedMemoryContext *context,
                                    VBucketID vbucket_id, BlobID blob_id);
std::vector<BlobID> LocalGetBlobsFromVBucketInfo(SharedMemoryContext *context,
                                                 VBucketID vbucket_id);
std::string LocalGetBucketNameById(SharedMemoryContext *context,
                                   BucketID blob_id);

void WaitForOutstandingBlobOps(MetadataManager *mdm, BlobID blob_id);
int LocalGetNumOutstandingFlushingTasks(SharedMemoryContext *context,
                                        VBucketID id);
int GetNumOutstandingFlushingTasks(SharedMemoryContext *context,
                                   RpcContext *rpc, VBucketID id);
void LocalCreateBlobMetadata(SharedMemoryContext *context, MetadataManager *mdm,
                             const std::string &blob_name, BlobID blob_id,
                             TargetID effective_target);
Heap *GetIdHeap(MetadataManager *mdm);
Heap *GetMapHeap(MetadataManager *mdm);
IdList AllocateIdList(MetadataManager *mdm, u32 length);
void FreeIdList(MetadataManager *mdm, IdList id_list);
u32 AppendToChunkedIdList(MetadataManager *mdm, ChunkedIdList *id_list, u64 id);
}  // namespace hermes
#endif  // HERMES_METADATA_MANAGEMENT_INTERNAL_H_
