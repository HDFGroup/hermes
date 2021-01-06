#ifndef HERMES_METADATA_MANAGEMENT_INTERNAL_H_
#define HERMES_METADATA_MANAGEMENT_INTERNAL_H_

#include "hermes_types.h"

namespace hermes {

bool IsNullBucketId(BucketID id);
bool IsNullVBucketId(VBucketID id);
bool IsNullBlobId(BlobID id);
bool IsNullTargetId(TargetID id);
TicketMutex *GetMapMutex(MetadataManager *mdm, MapType map_type);
VBucketID GetVBucketIdByName(SharedMemoryContext *context, RpcContext *rpc,
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
                            BlobID blob_id);
std::vector<BufferID> LocalGetBufferIdList(MetadataManager *mdm,
                                           BlobID blob_id);
void LocalGetBufferIdList(Arena *arena, MetadataManager *mdm, BlobID blob_id,
                          BufferIdArray *buffer_ids);
void LocalFreeBufferIdList(SharedMemoryContext *context, BlobID blob_id);
bool LocalDestroyBucket(SharedMemoryContext *context, RpcContext *rpc,
                               const char *bucket_name, BucketID bucket_id);
void LocalDestroyBlobById(SharedMemoryContext *context, RpcContext *rpc,
                          BlobID blob_id);
void LocalDestroyBlobByName(SharedMemoryContext *context, RpcContext *rpc,
                            const char *blob_name, BlobID blob_id);
BucketID LocalGetNextFreeBucketId(SharedMemoryContext *context, RpcContext *rpc,
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

u64 LocalGet(MetadataManager *mdm, const char *key, MapType map_type);
void LocalPut(MetadataManager *mdm, const char *key, u64 val, MapType map_type);
void LocalDelete(MetadataManager *mdm, const char *key, MapType map_type);

u64 LocalGetRemainingCapacity(SharedMemoryContext *context, TargetID id);
void LocalUpdateGlobalSystemViewState(SharedMemoryContext *context,
                                      std::vector<i64> adjustments);
SystemViewState *GetLocalSystemViewState(SharedMemoryContext *context);
SystemViewState *GetGlobalSystemViewState(SharedMemoryContext *context);
std::vector<u64> LocalGetGlobalDeviceCapacities(SharedMemoryContext *context);
std::vector<u64> GetGlobalDeviceCapacities(SharedMemoryContext *context,
                                           RpcContext *rpc);
void UpdateGlobalSystemViewState(SharedMemoryContext *context,
                                 RpcContext *rpc);

void StartGlobalSystemViewStateUpdateThread(SharedMemoryContext *context,
                                            RpcContext *rpc, Arena *arena,
                                            double slepp_ms);

void InitMetadataStorage(SharedMemoryContext *context, MetadataManager *mdm,
                         Arena *arena, Config *config);

std::vector<u64>
GetRemainingNodeCapacities(SharedMemoryContext *context,
                           const std::vector<TargetID> &targets);
std::string GetSwapFilename(MetadataManager *mdm, u32 node_id);

}  // namespace hermes
#endif  // HERMES_METADATA_MANAGEMENT_INTERNAL_H_
