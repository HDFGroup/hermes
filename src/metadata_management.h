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
#include "rpc_thallium.h"
#include <labstor/data_structures/lockless/string.h>
#include <labstor/data_structures/unordered_map.h>

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
  labstor::Mutex lock;       /**< lock */
  TargetID effective_target; /**< target ID */
  u32 last;                  /**< last */
  bool stop;                 /**< stop */

  BlobInfo() : last(0), stop(false) {
    stats.recency = 0;
    stats.frequency = 0;
    effective_target.as_int = 0;
  }

  /** Copy \a other BlobInfo sturcture. */
  BlobInfo &operator=(const BlobInfo &other) {
    stats = other.stats;
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

namespace lipc = labstor::ipc;
namespace lipcl = labstor::ipc::lockless;

class MetadataManager {
 public:
  Config *config_;
  SharedMemoryContext *context_;
  ThalliumRpc *rpc_;
  SystemViewState local_state_;
  GlobalSystemViewState global_state_;

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

  /**
   * initialize metadata manager
   * */
  MetadataManager(SharedMemoryContext *context, RpcContext *rpc, Config *config);

  /** get virtual bucket information by index locally */
  VBucketInfo* LocalGetVBucketInfoByIndex(u32 index);

  /** get virtual bucket information by id locally */
  VBucketInfo* LocalGetVBucketInfoById(VBucketID id);

  /** increment reference counter locally */
  void LocalIncrementRefcount(VBucketID id);

  /** decrement reference counter locally */
  void LocalDecrementRefcount(VBucketID id);

  /** decrement reference counter */
  void DecrementRefcount(VBucketID id);

  /** get relative node ID */
  u32 GetRelativeNodeId(int offset);

  /** get next node */
  u32 GetNextNode();

  /** get previous node */
  u32 GetPreviousNode();

  /** get node targets */
  std::vector<TargetID> GetNodeTargets(u32 target_node);

  /** get neighborhood node targets */
  std::vector<TargetID> GetNeighborhoodTargets();

  /** get remaining target capacity */
  u64 GetRemainingTargetCapacity(TargetID target_id);

  /** get remaining target capacities */
  std::vector<u64>
  GetRemainingTargetCapacities(const std::vector<TargetID> &targets);

  void AttachBlobToVBucket(const char *blob_name, const char *bucket_name,
                           VBucketID vbucket_id);

  /** get bucket name by ID locally */
  std::string LocalGetBucketNameById(BucketID blob_id);

  std::vector<BlobID> GetBlobsFromVBucketInfo(VBucketID vbucket_id);

  void RemoveBlobFromVBucketInfo(VBucketID vbucket_id, const char *blob_name,
                                 const char *bucket_name);

  std::string GetBucketNameById(BucketID id);

  f32 ScoringFunction(Stats *stats);

  int LocalGetNumOutstandingFlushingTasks(VBucketID id);

  int GetNumOutstandingFlushingTasks(VBucketID id);

  bool LocalLockBlob(BlobID blob_id);

  bool LocalUnlockBlob(BlobID blob_id);

  /** lock BLOB */
  bool LockBlob(BlobID blob_id);

  /** unlock BLOB */
  bool UnlockBlob(BlobID blob_id);

 private:

  /** is BLOB's \a name too long for kMaxBlobNameSize? */
  bool IsBlobNameTooLong(const std::string &name);

  /** is Bucket's \a name too long for kMaxBucketNameSize? */
  bool IsBucketNameTooLong(const std::string &name);

  /** is vBucket's \a name too long for kMaxVBucketNameSize? */
  bool IsVBucketNameTooLong(const std::string &name);

  /** put \a key, \a value, and \a map_type locally */
  void LocalPut(const char *key, u64 val, MapType map_type);

  /** put \a key and \a value locally */
  void LocalPut(BlobID key, const BlobInfo &value);

  /** get the value of \a key and \a map_type locally */
  u64 LocalGet(const char *key, MapType map_type);

  /** delete \a key locally */
  void LocalDelete(BlobID key);

  /** delete \a map_type locally */
  void LocalDelete(const char *key, MapType map_type);

  /** log error when metadata arena capacity is full */
  static void MetadataArenaErrorHandler();

  /** get hash string for metadata storage */
  u32 HashString(const char *str);

  /** get id */
  u64 GetId(const char *name, MapType map_type);

  /** get bucket id */
  BucketID GetBucketId(const char *name);

  /** get local bucket id */
  BucketID LocalGetBucketId(const char *name);

  /** get virtual bucket id */
  VBucketID GetVBucketId(const char *name);

  /** get local virtual bucket id */
  VBucketID LocalGetVBucketId(const char *name);

  /** make an internal BLOB name */
  std::string MakeInternalBlobName(const std::string &name, BucketID id);

  /** get BLOB id */
  BlobID GetBlobId(const std::string &name, BucketID bucket_id,
                   bool track_stats);

  /** put BLOB id */
  void PutId(const std::string &name, u64 id, MapType map_type);

  /** put bucket id */
  void PutBucketId(const std::string &name, BucketID id);

  /** put bucket id locally */
  void LocalPutBucketId(const std::string &name,BucketID id);

  /** put virtual bucket id */
  void PutVBucketId(const std::string &name, VBucketID id);

  /** put virtual bucket id locally */
  void LocalPutVBucketId(const std::string &name, VBucketID id);

  /** put BLOB id */
  void PutBlobId(const std::string &name, BlobID id, BucketID bucket_id);

  /** delete id */
  void DeleteId(const std::string &name,MapType map_type);

  /** delete bucket id */
  void DeleteBucketId(const std::string &name);

  /** delete virtual bucket id */
  void DeleteVBucketId(const std::string &name);

  /** delete BLOB information locally */
  void LocalDeleteBlobInfo(BlobID blob_id);

  /** delete BLOB id locally */
  void LocalDeleteBlobId(const std::string &name,
                         BucketID bucket_id);

  /** delete BLOB id */
  void DeleteBlobId(const std::string &name, BucketID bucket_id);

  /** get bucket information by \a index index locally */
  BucketInfo *LocalGetBucketInfoByIndex(u32 index);

  /** get BLOB name from \a blob_id locally */
  std::string LocalGetBlobNameFromId(BlobID blob_id);

  /** get BLOB name from \a blob_id */
  std::string GetBlobNameFromId(BlobID blob_id);

  u64 HexStringToU64(const std::string &s);

  /** get bucket ID from \a blob_id locally */
  BucketID LocalGetBucketIdFromBlobId(BlobID id);

  /** get bucket ID from \a blob_id */
  BucketID GetBucketIdFromBlobId(BlobID id);

  /** get bucket information from \a bucket_id */
  BucketInfo *LocalGetBucketInfoById(BucketID id);

  /** get BLOB IDs from \a bucket_id */
  std::vector<BlobID> GetBlobIds(BucketID bucket_id);

  /** get virtual bucket information by \a index */
  VBucketInfo *GetVBucketInfoByIndex(u32 index);

  /**
 * Returns an available BucketID and marks it as in use in the MDM.
 *
 * Assumes bucket_mutex is already held by the caller.
   */
  BucketID LocalGetNextFreeBucketId(const std::string &name);

  /** get or create a bucket ID locally */
  BucketID LocalGetOrCreateBucketId(const std::string &name);

  /** get or create a bucket ID */
  BucketID GetOrCreateBucketId(const std::string &name);

  /**
 * Returns an available VBucketID and marks it as in use in the MDM.
 *
 * Assumes MetadataManager::vbucket_mutex is already held by the caller.
   */
  VBucketID LocalGetNextFreeVBucketId(const std::string &name);

  /** get or create a virtual bucket ID locally */
  VBucketID LocalGetOrCreateVBucketId(const std::string &name);

  /** get or create a virtual bucket ID */
  VBucketID GetOrCreateVBucketId(const std::string &name);

  /** copy IDs */
  void CopyIds(u64 *dest, u64 *src, u32 count);

  void ReplaceBlobIdInBucket(BucketID bucket_id,
                             BlobID old_blob_id,
                             BlobID new_blob_id);

  /** add BLOB ID to bucket */
  void AddBlobIdToBucket(BlobID blob_id,
                         BucketID bucket_id);

  /** add BLOB ID to virtual bucket */
  void AddBlobIdToVBucket(BlobID blob_id,
                          VBucketID vbucket_id);

  /** allocate buffer ID list */
  u32 AllocateBufferIdList(u32 target_node,
                           const std::vector<BufferID> &buffer_ids);

  /** get buffer ID list */
  void GetBufferIdList(
      BlobID blob_id,
      BufferIdArray *buffer_ids);

  /** get buffer ID list as vector */
  std::vector<BufferID> GetBufferIdList(BlobID blob_id);

  /** get buffer IDs from BLOB id */
  BufferIdArray GetBufferIdsFromBlobId(BlobID blob_id,
                                       u32 **sizes);

  /** create BLOB metadata locally */
  void LocalCreateBlobMetadata(const std::string &blob_name, BlobID blob_id,
                               TargetID effective_target);

  /** create BLOB metadata */
  void CreateBlobMetadata(
      const std::string &blob_name, BlobID blob_id,
      TargetID effective_target);

  /** attach BLOB to bucket */
  void AttachBlobToBucket(
      const char *blob_name, BucketID bucket_id,
      const std::vector<BufferID> &buffer_ids,
      TargetID effective_target, bool is_swap_blob,
      bool called_from_buffer_organizer);

  /** free buffer ID list */
  void FreeBufferIdList(
      BlobID blob_id);

  /** delete BLOB metadata locally */
  void LocalDeleteBlobMetadata(const char *blob_name,
                               BlobID blob_id, BucketID bucket_id);

  /** wait for outstanding BLOB operations */
  void WaitForOutstandingBlobOps(BlobID blob_id);

  /** destroy BLOB by name locally */
  void LocalDestroyBlobByName(const char *blob_name,
                              BlobID blob_id,
                              BucketID bucket_id);

  /** destroy BLOB by ID locally */
  void LocalDestroyBlobById(BlobID blob_id, BucketID bucket_id);

  void RemoveBlobFromBucketInfo(BucketID bucket_id, BlobID blob_id);

  /** destroy BLOB by name */
  void DestroyBlobByName(BucketID bucket_id, const std::string &blob_name);

  /** rename BLOB */
  void RenameBlob(const std::string &old_name,
                  const std::string &new_name,
                  BucketID bucket_id);

  /** does \a bucket_id bucket contain \a blob_name BLOB? */
  bool ContainsBlob(BucketID bucket_id, const std::string &blob_name);

  /** destroy BLOB by ID */
  void DestroyBlobById(BlobID id,
                       BucketID bucket_id);

  /** destroy bucket */
  bool DestroyBucket(
      const char *name, BucketID bucket_id);

  /** destroy virtual bucket */
  bool DestroyVBucket(
      const char *name, VBucketID vbucket_id);

  /** rename bucket locally */
  void LocalRenameBucket(
      BucketID id, const std::string &old_name,
      const std::string &new_name);

  /** rename bucket */
  void RenameBucket(BucketID id,
                    const std::string &old_name,
                    const std::string &new_name);

  /** increment reference count locally */
  void LocalIncrementRefcount(BucketID id);

  /** decrement reference count locally */
  void LocalDecrementRefcount(BucketID id);

  /** decrement reference count  */
  void DecrementRefcount(BucketID id);

  /** get remaning target capacity locally */
  u64 LocalGetRemainingTargetCapacity(TargetID id);

  /** get local system view state */
  SystemViewState *GetLocalSystemViewState();

  /** get global device capacities locally */
  std::vector<u64> LocalGetGlobalDeviceCapacities();

  /** get global device capacities */
  std::vector<u64> GetGlobalDeviceCapacities();

  /** update global system view state locally */
  std::vector<ViolationInfo>
  LocalUpdateGlobalSystemViewState(u32 node_id,
                                   std::vector<i64> adjustments);

  /** update global system view state */
  void UpdateGlobalSystemViewState();

  /** find target ID from device ID */
  TargetID FindTargetIdFromDeviceId(const std::vector<TargetID> &targets,
                                    DeviceID device_id);

  /** create system view state */
  SystemViewState* CreateSystemViewState(Config *config);

  /** create global system view state */
  GlobalSystemViewState* CreateGlobalSystemViewState(Config *config);

  /** get swap file name */
  std::string GetSwapFilename(u32 node_id);

  /** swap BLOB to vector */
  std::vector<BufferID> SwapBlobToVec(SwapBlob swap_blob)l

  /** vector to swap BLOB */
  SwapBlob VecToSwapBlob(std::vector<BufferID> &vec)l

  /** ID array to swap BLOB */
  SwapBlob IdArrayToSwapBlob(BufferIdArray ids)l
};

/** log error when metadata arena capacity is full */
static void MetadataArenaErrorHandler() {
  LOG(FATAL) << "Metadata arena capacity exceeded. Consider increasing the "
             << "value of metadata_arena_percentage in the Hermes configuration"
             << std::endl;
}

}  // namespace hermes

#endif  // HERMES_METADATA_MANAGEMENT_H_
