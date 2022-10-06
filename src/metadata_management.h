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

#ifndef HERMES_METADATA_MANAGEMENT_H_
#define HERMES_METADATA_MANAGEMENT_H_

#include <string.h>

#include <atomic>
#include <string>

#include "memory_management.h"
#include "buffer_pool.h"

namespace hermes {

static const u32 kGlobalMutexNodeId = 1;

struct RpcContext;

/**
 * Representation of a non-NULL-terminated string in shared memory.
 *
 * Both the ShmemString iteself and the memory for the string it represents must
 * reside in the same shared memory segement, and the ShmemString must be stored
 * at a lower address than the string memory because the offset is from the
 * ShmemString instance itself. Here is a diagram:
 *
 * |-----------8 bytes offset ------------|
 * [ ShmemString | offset: 8 | size: 16 ] [ "string in memory" ]
 *
 * @see MakeShmemString()
 * @see GetShmemString()
 *
 */
struct ShmemString {
  /** offset is from the address of this ShmemString instance iteself */
  u32 offset;
  /** The size of the string (not NULL terminated) */
  u32 size;

  ShmemString(const ShmemString &) = delete;
  ShmemString(const ShmemString &&) = delete;
  ShmemString& operator=(const ShmemString &) = delete;
  ShmemString& operator=(const ShmemString &&) = delete;
};

enum MapType {
  kMapType_Bucket,
  kMapType_VBucket,
  kMapType_BlobId,
  kMapType_BlobInfo,

  kMapType_Count
};

enum class ThresholdViolation {
  kMin,
  kMax
};

struct ViolationInfo {
  TargetID target_id;
  ThresholdViolation violation;
  size_t violation_size;
};

struct Stats {
  u32 recency;
  u32 frequency;
};

const int kIdListChunkSize = 10;

struct IdList {
  u32 head_offset;
  u32 length;
};

struct BufferIdArray {
  BufferID *ids;
  u32 length;
};

struct BlobInfo {
  Stats stats;
  TicketMutex lock;
  TargetID effective_target;
  u32 last;
  bool stop;

  BlobInfo() : last(0), stop(false) {
    stats.recency = 0;
    stats.frequency = 0;
    lock.ticket.store(0);
    lock.serving.store(0);
    effective_target.as_int = 0;
  }

  BlobInfo& operator=(const BlobInfo &other) {
    stats = other.stats;
    lock.ticket.store(other.lock.ticket.load());
    lock.serving.store(other.lock.serving.load());
    effective_target = other.effective_target;
    last = other.last;
    stop = other.stop;

    return *this;
  }
};

struct BucketInfo {
  BucketID next_free;
  ChunkedIdList blobs;
  std::atomic<int> ref_count;
  bool active;
};

static constexpr int kMaxTraitsPerVBucket = 8;

struct VBucketInfo {
  VBucketID next_free;
  ChunkedIdList blobs;
  std::atomic<int> ref_count;
  std::atomic<int> async_flush_count;
  /** Not currently used since Traits are process local. */
  TraitID traits[kMaxTraitsPerVBucket];
  bool active;
};

struct SystemViewState {
  /** Total capacities of each device. */
  u64 capacities[kMaxDevices];
  /** The remaining bytes available for buffering in each device. */
  std::atomic<u64> bytes_available[kMaxDevices];
  /** The min and max threshold (percentage) for each device at which the
   * BufferOrganizer will trigger. */
  Thresholds bo_capacity_thresholds[kMaxDevices];
  /** The total number of buffering devices. */
  int num_devices;
};

// TODO(chogan):
/**
 * A snapshot view of the entire system's Targets' available capacities.
 *
 * This information is only stored on 1 node, designated by
 * MetadataManager::global_system_view_state_node_id, and is only updated by 1
 * rank (the Hermes process on that node). Hence, it does not need to be stored
 * in shared memory and we are able to use normal std containers. However, since
 * multiple RPC handler threads can potentially update the `bytes_available`
 * field concurrently, we must not do any operations on the vector itself. We
 * can only do operations on the atomics within. The vector is created in
 * StartGlobalSystemViewStateUpdateThread, and thereafter we can only call
 * functions on the individual atomics (e.g., bytes_available[i].fetch_add),
 * which is thread safe.
 */
struct GlobalSystemViewState {
  /** The total number of buffering Targets in the system */
  u64 num_targets;
  /** The number of devices per node */
  int num_devices;
  u64 capacities[kMaxDevices];
  /** The remaining capacity of each Target in the system */
  std::atomic<u64> *bytes_available;
  /** The min and max capacity thresholds (percentage) for each Target in the
   * system */
  Thresholds bo_capacity_thresholds[kMaxDevices];
};

struct MetadataManager {
  // All offsets are relative to the beginning of the MDM
  ptrdiff_t bucket_info_offset;
  BucketID first_free_bucket;

  ptrdiff_t vbucket_info_offset;
  VBucketID first_free_vbucket;

  ptrdiff_t rpc_state_offset;
  ptrdiff_t host_names_offset;
  ptrdiff_t host_numbers_offset;
  ptrdiff_t system_view_state_offset;
  ptrdiff_t global_system_view_state_offset;

  ptrdiff_t id_heap_offset;
  ptrdiff_t map_heap_offset;

  ptrdiff_t bucket_map_offset;
  ptrdiff_t vbucket_map_offset;
  ptrdiff_t blob_id_map_offset;
  ptrdiff_t blob_info_map_offset;

  ptrdiff_t swap_filename_prefix_offset;
  ptrdiff_t swap_filename_suffix_offset;

  // TODO(chogan): @optimization Should the TicketMutexes here be reader/writer
  // locks?

  /** Lock for accessing `BucketInfo` structures located at
   * `bucket_info_offset` */
  TicketMutex bucket_mutex;
  RwLock bucket_delete_lock;

  /** Lock for accessing `VBucketInfo` structures located at
   * `vbucket_info_offset` */
  TicketMutex vbucket_mutex;

  /** Lock for accessing the `IdMap` located at `bucket_map_offset` */
  TicketMutex bucket_map_mutex;
  /** Lock for accessing the `IdMap` located at `vbucket_map_offset` */
  TicketMutex vbucket_map_mutex;
  /** Lock for accessing the `IdMap` located at `blob_id_map_offset` */
  TicketMutex blob_id_map_mutex;
  /** Lock for accessing the `BlobInfoMap` located at `blob_info_map_offset` */
  TicketMutex blob_info_map_mutex;
  /** Lock for accessing `IdList`s and `ChunkedIdList`s */
  TicketMutex id_mutex;

  size_t map_seed;

  IdList node_targets;
  IdList neighborhood_targets;

  u32 system_view_state_update_interval_ms;
  u32 global_system_view_state_node_id;
  u32 num_buckets;
  u32 max_buckets;
  u32 num_vbuckets;
  u32 max_vbuckets;
  std::atomic<u32> clock;
};

struct RpcContext;

/**
 *
 */
void InitMetadataManager(MetadataManager *mdm, RpcContext *rpc, Arena *arena,
                         Config *config);

/**
 *
 */
void InitNeighborhoodTargets(SharedMemoryContext *context, RpcContext *rpc);

/**
 *
 */
bool DestroyBucket(SharedMemoryContext *context, RpcContext *rpc,
                   const char *name, BucketID bucket_id);

/**
 *
 */
bool DestroyVBucket(SharedMemoryContext *context, RpcContext *rpc,
                    const char *name, VBucketID vbucket_id);

/**
 *
 */
void DestroyBlobByName(SharedMemoryContext *context, RpcContext *rpc,
                       BucketID bucket_id, const std::string &blob_name);

/**
 *
 */
void RenameBlob(SharedMemoryContext *context, RpcContext *rpc,
                const std::string &old_name, const std::string &new_name,
                BucketID bucket_id);

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
BufferIdArray GetBufferIdsFromBlobId(Arena *arena,
                                     SharedMemoryContext *context,
                                     RpcContext *rpc, BlobID blob_id,
                                     u32 **sizes);

/**
 *
 */
BlobID GetBlobId(SharedMemoryContext *context, RpcContext *rpc,
                 const std::string &name, BucketID bucket_id,
                 bool track_stats = true);



/**
 *
 */
std::string GetBlobNameFromId(SharedMemoryContext *context, RpcContext *rpc,
                              BlobID blob_id);

/**
 *
 */
bool BlobIsInSwap(BlobID id);

/**
 *
 */
BucketID GetOrCreateBucketId(SharedMemoryContext *context, RpcContext *rpc,
                             const std::string &name);

/**
 *
 */
VBucketID GetOrCreateVBucketId(SharedMemoryContext *context, RpcContext *rpc,
                               const std::string &name);

/**
 *
 */
void AttachBlobToBucket(SharedMemoryContext *context, RpcContext *rpc,
                        const char *blob_name, BucketID bucket_id,
                        const std::vector<BufferID> &buffer_ids,
                        TargetID effective_target, bool is_swap_blob = false,
                        bool called_from_buffer_organizer = false);

/**
 *
 */
void DecrementRefcount(SharedMemoryContext *context, RpcContext *rpc,
                       BucketID id);

/**
 *
 */
std::vector<BufferID> SwapBlobToVec(SwapBlob swap_blob);

/**
 *
 */
SwapBlob VecToSwapBlob(std::vector<BufferID> &vec);

/**
 *
 */
SwapBlob IdArrayToSwapBlob(BufferIdArray ids);

/**
 *
 */
bool IsBlobNameTooLong(const std::string &name);

/**
 *
 */
bool IsBucketNameTooLong(const std::string &name);

/**
 *
 */
bool IsVBucketNameTooLong(const std::string &name);

/**
 *
 */
TargetID FindTargetIdFromDeviceId(const std::vector<TargetID> &targets,
                                  DeviceID device_id);

/**
 *
 */
std::vector<TargetID> GetNeighborhoodTargets(SharedMemoryContext *context,
                                             RpcContext *rpc);
/**
 *
 */
std::vector<u64>
GetRemainingTargetCapacities(SharedMemoryContext *context, RpcContext *rpc,
                             const std::vector<TargetID> &targets);
/**
 *
 */
std::vector<BlobID> GetBlobIds(SharedMemoryContext *context, RpcContext *rpc,
                               BucketID bucket_id);

/**
 *
 */
BucketID GetBucketId(SharedMemoryContext *context, RpcContext *rpc,
                           const char *name);

/**
 *
 */
BucketID GetBucketIdFromBlobId(SharedMemoryContext *context, RpcContext *rpc,
                               BlobID blob_id);

/**
 *
 */
void DecrementRefcount(SharedMemoryContext *context, RpcContext *rpc,
                       VBucketID id);
/**
 *
 */
bool IsNullBucketId(BucketID id);

/**
 *
 */
bool IsNullVBucketId(VBucketID id);

/**
 *
 */
bool IsNullBlobId(BlobID id);

/**
 *
 */
void BeginGlobalTicketMutex(SharedMemoryContext *context, RpcContext *rpc);

/**
 *
 */
void EndGlobalTicketMutex(SharedMemoryContext *context, RpcContext *rpc);

/**
 *
 */
void LocalBeginGlobalTicketMutex(MetadataManager *mdm);

/**
 *
 */
void LocalEndGlobalTicketMutex(MetadataManager *mdm);

/**
 *
 */
void AttachBlobToVBucket(SharedMemoryContext *context, RpcContext *rpc,
                         const char *blob_name, const char *bucket_name,
                         VBucketID vbucket_id);

/**
 *
 */
void RemoveBlobFromBucketInfo(SharedMemoryContext *context, RpcContext *rpc,
                              BucketID bucket_id, BlobID blob_id);

/**
 *
 */
void RemoveBlobFromVBucketInfo(SharedMemoryContext *context, RpcContext *rpc,
                               VBucketID vbucket_id, const char *blob_name,
                               const char *bucket_name);

/**
 *
 */
std::vector<BlobID> GetBlobsFromVBucketInfo(SharedMemoryContext *context,
                                            RpcContext *rpc,
                                            VBucketID vbucket_id);

/**
 *
 */
std::string GetBucketNameById(SharedMemoryContext *context, RpcContext *rpc,
                              BucketID id);
/**
 *
 */
void IncrementBlobStats(SharedMemoryContext *context, RpcContext *rpc,
                        BlobID blob_id);

/**
 *
 */
void LocalIncrementBlobStats(MetadataManager *mdm, BlobID blob_id);

/**
 *
 */
bool LockBlob(SharedMemoryContext *context, RpcContext *rpc, BlobID blob_id);

/**
 *
 */
bool UnlockBlob(SharedMemoryContext *context, RpcContext *rpc, BlobID blob_id);

/**
 *
 */
bool LocalLockBlob(SharedMemoryContext *context, BlobID blob_id);

/**
 *
 */
bool LocalUnlockBlob(SharedMemoryContext *context, BlobID blob_id);

/**
 *
 */
SystemViewState *GetLocalSystemViewState(SharedMemoryContext *context);

/**
 *
 */
void LocalReplaceBlobIdInBucket(SharedMemoryContext *context,
                                BucketID bucket_id, BlobID old_blob_id,
                                BlobID new_blob_id);
/**
 * Deletes @p old_blob_id from @p bucket_id and adds @p new_blob_id. It combines
 * the delete and the add into one call in order to avoid multiple RPCs.
 */
void ReplaceBlobIdInBucket(SharedMemoryContext *context, RpcContext *rpc,
                           BucketID bucket_id, BlobID old_blob_id,
                           BlobID new_blob_id);

/**
 * Creates a ShmemString with the value @p val at location @p memory.
 *
 * @pre The address of @p sms must be lower than @p memory because the @p offset
 *      is from the beginning of the @sms.
 *
 * @param[out] sms The ShmemString instance to be filled out.
 * @param memory The location in shared memory to store the @p val.
 * @param val The string to store.
 */
void MakeShmemString(ShmemString *sms, u8 *memory, const std::string &val);

/**
 * Retrieves a ShmemString into a std::string
 *
 * @param sms The ShmemString that represents the internal string
 *
 * @return A newly allocated std::string containing a copy of the string from
 *         shared memory, or an empty std::string if the ShmemString is invalid.
 */
std::string GetShmemString(ShmemString *sms);

}  // namespace hermes

#endif  // HERMES_METADATA_MANAGEMENT_H_
