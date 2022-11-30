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

#include "buffer_pool.h"

namespace hermes {

static const u32 kGlobalMutexNodeId = 1; /**< global mutex node ID */

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
  ShmemString &operator=(const ShmemString &) = delete;
  ShmemString &operator=(const ShmemString &&) = delete;
};

/** map type */
enum MapType {
  kMapType_Bucket,
  kMapType_VBucket,
  kMapType_BlobId,
  kMapType_BlobInfo,
  kMapType_Count
};

/** min/max threshold violation */
enum class ThresholdViolation { kMin, kMax };

/**
   A structure to represent violation information
*/
struct ViolationInfo {
  TargetID target_id;           /**< target node ID */
  ThresholdViolation violation; /**< threshold violation */
  size_t violation_size;        /**< size of violation */
};

/**
   A structure to represent statistics
 */
struct Stats {
  u32 recency;   /**< recency */
  u32 frequency; /**< frequency */
};

const int kIdListChunkSize = 10; /**< chunk size of ID list */
/**
   A structure to represent ID list
*/
struct IdList {
  u32 head_offset; /**< the offset of head in the list */
  u32 length;      /**< length of list */
};

/**
   A structure to represent the array of buffer IDs
 */
struct BufferIdArray {
  BufferID *ids; /**< pointer to IDs */
  u32 length;    /**< length of array */
};

/**
   A structure to represent Blob information
 */
struct BlobInfo {
  Stats stats;               /**< BLOB statistics */
  labstor::Mutex lock;          /**< lock */
  TargetID effective_target; /**< target ID */
  u32 last;                  /**< last */
  bool stop;                 /**< stop */

  BlobInfo() : last(0), stop(false) {
    stats.recency = 0;
    stats.frequency = 0;
    lock.ticket.store(0);
    lock.serving.store(0);
    effective_target.as_int = 0;
  }

  /** Copy \a other BlobInfo sturcture. */
  BlobInfo &operator=(const BlobInfo &other) {
    stats = other.stats;
    lock.ticket.store(other.lock.ticket.load());
    lock.serving.store(other.lock.serving.load());
    effective_target = other.effective_target;
    last = other.last;
    stop = other.stop;

    return *this;
  }
};

/**
   A structure to represent bucket information
*/
struct BucketInfo {
  BucketID next_free;         /**< id of next free bucket */
  ChunkedIdList blobs;        /**< list of BLOB IDs */
  std::atomic<int> ref_count; /**< reference count */
  bool active;                /**< is bucket active? */
};

/** maxinum number of traits per bucket */
static constexpr int kMaxTraitsPerVBucket = 8;

/**
   A structure to represent virtual bucket information
 */
struct VBucketInfo {
  VBucketID next_free;                /**< id of next free virtual bucket */
  ChunkedIdList blobs;                /**< list of BLOB IDs */
  std::atomic<int> ref_count;         /**< reference count */
  std::atomic<int> async_flush_count; /**< asynchrnous flush count */
  /** Not currently used since Traits are process local. */
  TraitID traits[kMaxTraitsPerVBucket];
  bool active; /**< is virtual bucket active? */
};

/**
   A sntructure to view the current state of system
 */
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
  u64 capacities[kMaxDevices]; /**< capacities of devices */
  /** The remaining capacity of each Target in the system */
  std::atomic<u64> *bytes_available;
  /** The min and max capacity thresholds (percentage) for each Target in the
   * system */
  Thresholds bo_capacity_thresholds[kMaxDevices];
};

/**
   A structure to represent metadata manager
*/
class MetadataManager {
 public:
  Config *config_;
  SharedMemoryContext *context_;
  RpcContext *rpc_;

  // All offsets are relative to the beginning of the MDM
  ptrdiff_t bucket_info_offset; /**< bucket information */
  BucketID first_free_bucket;   /**< ID of first free bucket */

  ptrdiff_t vbucket_info_offset; /**< virtual bucket information */
  VBucketID first_free_vbucket;  /**< ID of first free virtual bucket */

  ptrdiff_t rpc_state_offset;                /**< RPC state */
  ptrdiff_t host_names_offset;               /**< host names  */
  ptrdiff_t host_numbers_offset;             /**< host numbers */
  ptrdiff_t system_view_state_offset;        /**< system view state */
  ptrdiff_t global_system_view_state_offset; /**< global system view state */

  ptrdiff_t id_heap_offset;  /**< ID heap */
  ptrdiff_t map_heap_offset; /**< map heap */

  ptrdiff_t bucket_map_offset;    /**< bucket map */
  ptrdiff_t vbucket_map_offset;   /**< virtual bucket map */
  ptrdiff_t blob_id_map_offset;   /**< BLOB ID map */
  ptrdiff_t blob_info_map_offset; /**< BLOB information map */

  ptrdiff_t swap_filename_prefix_offset; /**< swap file name prefix */
  ptrdiff_t swap_filename_suffix_offset; /**< swap file name suffix */

  // TODO(chogan): @optimization Should the mutexes here be reader/writer
  // locks?

  /** Lock for accessing `BucketInfo` structures located at
   * `bucket_info_offset` */
  labstor::Mutex bucket_mutex;
  labstor::RwLock bucket_delete_lock; /**< lock for bucket deletion */

  /** Lock for accessing `VBucketInfo` structures located at
   * `vbucket_info_offset` */
  labstor::Mutex vbucket_mutex;

  /** Lock for accessing the `IdMap` located at `bucket_map_offset` */
  labstor::Mutex bucket_map_mutex;
  /** Lock for accessing the `IdMap` located at `vbucket_map_offset` */
  labstor::Mutex vbucket_map_mutex;
  /** Lock for accessing the `IdMap` located at `blob_id_map_offset` */
  labstor::Mutex blob_id_map_mutex;
  /** Lock for accessing the `BlobInfoMap` located at `blob_info_map_offset` */
  labstor::Mutex blob_info_map_mutex;
  /** Lock for accessing `IdList`s and `ChunkedIdList`s */
  labstor::Mutex id_mutex;

  size_t map_seed; /**<  map seed */

  IdList node_targets;         /**<  ID list of node targets  */
  IdList neighborhood_targets; /**<  ID list of neighborhood targets */

  u32 system_view_state_update_interval_ms; /**< sys. view update interval */
  u32 global_system_view_state_node_id;     /**< node ID fo global sys. view */
  u32 num_buckets;                          /**< number of buckets */
  u32 max_buckets;                          /**< maximum number of buckets */
  u32 num_vbuckets;                         /**< number of virtual buckets */
  u32 max_vbuckets;       /**< maximum number of virtual buckets */
  std::atomic<u32> clock; /**< clock */

 public:
  MetadataManager(RpcContext *context, Config *config);
};

/**
 *
 */
void InitMetadataManager(MetadataManager *mdm, RpcContext *rpc, Config *config);

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
BufferIdArray GetBufferIdsFromBlobId(SharedMemoryContext *context,
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
std::vector<u64> GetRemainingTargetCapacities(
    SharedMemoryContext *context, RpcContext *rpc,
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
 * begin global ticket mutex
 */
void BeginGloballabstor::Mutex(SharedMemoryContext *context, RpcContext *rpc);

/**
 * end global ticket mutex
 */
void EndGloballabstor::Mutex(SharedMemoryContext *context, RpcContext *rpc);

/**
 * begin global ticket mutex locally
 */
void LocalBeginGloballabstor::Mutex(MetadataManager *mdm);

/**
 * end global ticket mutex locally
 */
void LocalEndGloballabstor::Mutex(MetadataManager *mdm);

/**
 * attach BLOB to VBucket
 */
void AttachBlobToVBucket(SharedMemoryContext *context, RpcContext *rpc,
                         const char *blob_name, const char *bucket_name,
                         VBucketID vbucket_id);

/**
 * remove BLOB from Bucket
 */
void RemoveBlobFromBucketInfo(SharedMemoryContext *context, RpcContext *rpc,
                              BucketID bucket_id, BlobID blob_id);

/**
 * remove BLOB from VBucket
 */
void RemoveBlobFromVBucketInfo(SharedMemoryContext *context, RpcContext *rpc,
                               VBucketID vbucket_id, const char *blob_name,
                               const char *bucket_name);

/**
 * get BLOB from VBucket
 */
std::vector<BlobID> GetBlobsFromVBucketInfo(SharedMemoryContext *context,
                                            RpcContext *rpc,
                                            VBucketID vbucket_id);

/**
 * get bucket name by bucket ID
 */
std::string GetBucketNameById(SharedMemoryContext *context, RpcContext *rpc,
                              BucketID id);
/**
 * increment BLOB stats
 */
void IncrementBlobStats(SharedMemoryContext *context, RpcContext *rpc,
                        BlobID blob_id);

/**
 * increment BLOB stats locally
 */
void LocalIncrementBlobStats(MetadataManager *mdm, BlobID blob_id);

/**
 * lock BLOB
 */
bool LockBlob(SharedMemoryContext *context, RpcContext *rpc, BlobID blob_id);

/**
 * unlock BLOB
 */
bool UnlockBlob(SharedMemoryContext *context, RpcContext *rpc, BlobID blob_id);

/**
 * lock BLOB locally
 */
bool LocalLockBlob(SharedMemoryContext *context, BlobID blob_id);

/**
 * unlock BLOB locally
 */
bool LocalUnlockBlob(SharedMemoryContext *context, BlobID blob_id);

/**
 * get local system view state
 */
SystemViewState *GetLocalSystemViewState(SharedMemoryContext *context);

/**
 * replace BLOB ID in bucket locally
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
 *      is from the beginning of the \a sms.
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
