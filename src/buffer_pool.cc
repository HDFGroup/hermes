#include "buffer_pool.h"
#include "buffer_pool_internal.h"

#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <assert.h>
#include <fcntl.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <cmath>
#include <iostream>
#include <set>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "mpi.h"

#include "metadata_management.h"
#include "rpc.h"

#include "debug_state.cc"
#include "memory_management.cc"
#include "config_parser.cc"
#include "utils.cc"
#include "traits.cc"

#if defined(HERMES_COMMUNICATION_MPI)
#include "communication_mpi.cc"
#elif defined(HERMES_COMMUNICATION_ZMQ)
#include "communication_zmq.cc"
#else
#error "Communication implementation required " \
  "(e.g., -DHERMES_COMMUNICATION_MPI)."
#endif

#if defined(HERMES_RPC_THALLIUM)
#include "rpc_thallium.cc"
#else
#error "RPC implementation required (e.g., -DHERMES_RPC_THALLIUM)."
#endif

#include "metadata_management.cc"
#include "buffer_organizer.cc"

#if defined(HERMES_MDM_STORAGE_STBDS)
#include "metadata_storage_stb_ds.cc"
#else
#error "Metadata storage implementation required" \
  "(e.g., -DHERMES_MDM_STORAGE_STBDS)."
#endif

/**
 * @file buffer_pool.cc
 *
 * Implementation of a BufferPool that lives in shared memory. Other processes
 * interact with the BufferPool by requesting buffers through the `GetBuffers`
 * call to reserve a set of `BufferID`s and then using those IDs for I/O. Remote
 * processes can request remote buffers via the `GetBuffers` RPC call.
 */

namespace hermes {

void Finalize(SharedMemoryContext *context, CommunicationContext *comm,
              RpcContext *rpc, const char *shmem_name, Arena *trans_arena,
              bool is_application_core, bool force_rpc_shutdown) {
  WorldBarrier(comm);
  if (is_application_core) {
    ShutdownRpcClients(rpc);
    ReleaseSharedMemoryContext(context);
    HERMES_DEBUG_CLIENT_CLOSE();
  }
  WorldBarrier(comm);
  if (!is_application_core) {
    if (comm->first_on_node) {
      bool is_daemon =
        (comm->world_size == comm->num_nodes) && !force_rpc_shutdown;
      FinalizeRpcContext(rpc, is_daemon);
    }
    UnmapSharedMemory(context);
    shm_unlink(shmem_name);
    HERMES_DEBUG_SERVER_CLOSE();
  }
  DestroyArena(trans_arena);
}

void LockBuffer(BufferHeader *header) {
  bool expected = false;
  while (header->locked.compare_exchange_weak(expected, true)) {
    // NOTE(chogan): Spin until we get the lock
  }
}

void UnlockBuffer(BufferHeader *header) {
  header->locked.store(false);
}

BufferPool *GetBufferPoolFromContext(SharedMemoryContext *context) {
  BufferPool *result = (BufferPool *)(context->shm_base +
                                      context->buffer_pool_offset);

  return result;
}

Device *GetDeviceFromHeader(SharedMemoryContext *context,
                            BufferHeader *header) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  Device *devices_base = (Device *)(context->shm_base + pool->devices_offset);
  Device *result = devices_base + header->device_id;

  return result;
}

Target *GetTarget(SharedMemoryContext *context, int index) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  Target *targets_base = (Target *)(context->shm_base + pool->targets_offset);
  Target *result = targets_base + index;

  return result;
}

Target *GetTargetFromId(SharedMemoryContext *context, TargetID id) {
  Target *result = GetTarget(context, id.bits.index);

  return result;
}

std::vector<f32> GetBandwidths(SharedMemoryContext *context) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  std::vector<f32> result(pool->num_devices, 0);

  for (int i = 0; i < pool->num_devices; i++) {
    Device *device = GetDeviceById(context, i);
    result[i] = device->bandwidth_mbps;
  }

  return result;
}

Device *GetDeviceById(SharedMemoryContext *context, DeviceID device_id) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  Device *devices_base = (Device *)(context->shm_base + pool->devices_offset);
  Device *result = devices_base + device_id;

  return result;
}

DeviceID GetDeviceIdFromTargetId(TargetID target_id) {
  DeviceID result = target_id.bits.device_id;

  return result;
}

BufferHeader *GetHeadersBase(SharedMemoryContext *context) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  BufferHeader *result = (BufferHeader *)(context->shm_base +
                                          pool->headers_offset);

  return result;
}

inline BufferHeader *GetHeaderByIndex(SharedMemoryContext *context, u32 index) {
  [[maybe_unused]] BufferPool *pool = GetBufferPoolFromContext(context);
  BufferHeader *headers = GetHeadersBase(context);
  assert(index < pool->total_headers);
  BufferHeader *result = headers + index;

  return result;
}

BufferHeader *GetHeaderByBufferId(SharedMemoryContext *context,
                                         BufferID id) {
  BufferHeader *result = GetHeaderByIndex(context, id.bits.header_index);

  return result;
}

void ResetHeader(BufferHeader *header) {
  if (header) {
    // NOTE(chogan): Keep `id` the same. They should never change.
    header->next_free.as_int = 0;
    // NOTE(chogan): Keep `data_offset` because splitting/merging may reuse it
    header->used = 0;
    header->capacity = 0;
    header->device_id = 0;
    header->in_use = 0;
    header->locked = 0;
  }
}

static inline void MakeHeaderDormant(BufferHeader *header) {
  header->capacity = 0;
}

bool HeaderIsDormant(BufferHeader *header) {
  bool result = header->capacity == 0;

  return result;
}

#if 0
BufferHeader *GetFirstDormantHeader(SharedMemoryContext *context) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  BufferHeader *headers = GetHeadersBase(context);
  BufferHeader *result = 0;

  for (u32 i = 0; i < pool->num_headers; ++i) {
    BufferHeader *header = &headers[i];
    if (HeaderIsDormant(header)) {
      result = header;
      break;
    }
  }

  return result;
}
#endif

#if 0
f32 ComputeFragmentationScore(SharedMemoryContext *context) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  BufferHeader *headers = GetHeadersBase(context);

  u32 total_used_headers = 0;
  f32 usage_score = 0;
  for (u32 i = 0; i < pool->num_headers; ++i) {
    BufferHeader *header = &headers[i];

    if (header->in_use) {
      total_used_headers++;

      // TODO(chogan): Is a lock really necessary? Need to check where
      // `used` and `capacity` get mutated. We only read them here. Could
      // just make both variables atomics.
      LockBuffer(header);
      f32 percentage_used = header->used / header->capacity;
      usage_score += percentage_used;
      UnlockBuffer(header);
    }
  }

  f32 percentage_of_used_headers = ((f32)total_used_headers /
                                    (f32)pool->num_headers);
  f32 max_usage_score = 1.0f * pool->num_headers;
  f32 optimal_usage = percentage_of_used_headers * max_usage_score;
  f32 result = optimal_usage - usage_score;

  // TODO(chogan): Reorganize BufferPool if score is > 0.5 or 0.6
  return result;
}
#endif

i32 GetSlabUnitSize(SharedMemoryContext *context, DeviceID device_id,
                    int slab_index) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  i32 result = 0;
  i32 *slab_unit_sizes = nullptr;

  if (device_id < pool->num_devices) {
    slab_unit_sizes = (i32 *)(context->shm_base +
                              pool->slab_unit_sizes_offsets[device_id]);
    if (slab_index < pool->num_slabs[device_id]) {
      result = slab_unit_sizes[slab_index];
    } else {
      // TODO(chogan): @logging
    }
  } else {
    // TODO(chogan): @logging
  }

  return result;
}

i32 GetSlabBufferSize(SharedMemoryContext *context, DeviceID device_id,
                       int slab_index) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  i32 *slab_sizes = nullptr;
  i32 result = 0;

  if (device_id < pool->num_devices) {
    slab_sizes = (i32 *)(context->shm_base +
                         pool->slab_buffer_sizes_offsets[device_id]);
    if (slab_index < pool->num_slabs[device_id]) {
      result = slab_sizes[slab_index];
    } else {
      // TODO(chogan): @logging
    }
  } else {
    // TODO(chogan): @logging
  }

  return result;
}

BufferID *GetFreeListPtr(SharedMemoryContext *context, DeviceID device_id) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  BufferID *result = nullptr;

  if (device_id < pool->num_devices) {
    result =
      (BufferID *)(context->shm_base + pool->free_list_offsets[device_id]);
  }

  return result;
}

int GetSlabIndexFromHeader(SharedMemoryContext *context, BufferHeader *header) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  DeviceID device_id = header->device_id;
  i32 units = header->capacity / pool->block_sizes[device_id];
  int result = 0;

  for (int i = 0; i < pool->num_slabs[device_id]; ++i) {
    if (GetSlabUnitSize(context, device_id, i) == units) {
      result = i;
      break;
    }
  }

  return result;
}

bool BufferIsRemote(CommunicationContext *comm, BufferID buffer_id) {
  bool result = (u32)comm->node_id != buffer_id.bits.node_id;

  return result;
}

bool BufferIsRemote(RpcContext *rpc, BufferID buffer_id) {
  bool result = rpc->node_id != buffer_id.bits.node_id;

  return result;
}

bool IsNullBufferId(BufferID id) {
  bool result = id.as_int == 0;

  return result;
}

bool BufferIsByteAddressable(SharedMemoryContext *context, BufferID id) {
  BufferHeader *header = GetHeaderByBufferId(context, id);
  Device *device = GetDeviceFromHeader(context, header);
  bool result = device->is_byte_addressable;

  return result;
}

BufferID PeekFirstFreeBufferId(SharedMemoryContext *context, DeviceID device_id,
                               int slab_index) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  BufferID result = {};
  BufferID *free_list = GetFreeListPtr(context, device_id);
  if (slab_index < pool->num_slabs[device_id]) {
    result = free_list[slab_index];
  }

  return result;
}

void SetFirstFreeBufferId(SharedMemoryContext *context, DeviceID device_id,
                          int slab_index, BufferID new_id) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  BufferID *free_list = GetFreeListPtr(context, device_id);
  if (slab_index < pool->num_slabs[device_id]) {
    free_list[slab_index] = new_id;
  }
}

static std::atomic<u32> *GetAvailableBuffersArray(SharedMemoryContext *context,
                                                  DeviceID device_id) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  std::atomic<u32> *result =
    (std::atomic<u32> *)(context->shm_base +
                         pool->buffers_available_offsets[device_id]);

  return result;
}

#if 0
static u32 GetNumBuffersAvailable(SharedMemoryContext *context,
                                  DeviceID device_id, int slab_index) {
  std::atomic<u32> *buffers_available = GetAvailableBuffersArray(context,
                                                                 device_id);
  u32 result = 0;
  if (buffers_available) {
    result = buffers_available[slab_index].load();
  }

  return result;
}

static u64 GetNumBytesRemaining(SharedMemoryContext *context,
                                DeviceID device_id, int slab_index) {
  u32 num_free_buffers = GetNumBuffersAvailable(context, device_id, slab_index);
  u32 buffer_size = GetSlabBufferSize(context, device_id, slab_index);
  u64 result = num_free_buffers * buffer_size;

  return result;
}

static u64 GetNumBytesRemaining(SharedMemoryContext *context, DeviceID id) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  u64 result = 0;
  for (int i = 0; i < pool->num_slabs[id]; ++i) {
    result += GetNumBytesRemaining(context, id, i);
  }

  return result;
}
#endif

static void DecrementAvailableBuffers(SharedMemoryContext *context,
                                      DeviceID device_id, int slab_index) {
  std::atomic<u32> *buffers_available = GetAvailableBuffersArray(context,
                                                                 device_id);
  if (buffers_available) {
    buffers_available[slab_index].fetch_sub(1);
  }
}

static void IncrementAvailableBuffers(SharedMemoryContext *context,
                                      DeviceID device_id, int slab_index) {
  std::atomic<u32> *buffers_available = GetAvailableBuffersArray(context,
                                                                 device_id);
  if (buffers_available) {
    buffers_available[slab_index].fetch_add(1);
  }
}

void UpdateBufferingCapacities(SharedMemoryContext *context, i64 adjustment,
                               DeviceID device_id) {
  BufferPool *pool = GetBufferPoolFromContext(context);

  // NOTE(chogan): Update local capacities, which will eventually be reflected
  // in the global SystemViewState.
  // TODO(chogan): I think Target capacities will supercede the global system
  // view state once we have topologies. For now we track both node capacities
  // and global capacities.
  pool->capacity_adjustments[device_id].fetch_add(adjustment);

  // TODO(chogan): DeviceID is currently equal to TargetID, but that will change
  // once we have topologies. This function will need to support TargetIDs
  // instead of DeviceID.
  Target *target = GetTarget(context, device_id);
  target->remaining_space.fetch_add(adjustment);
}

void LocalReleaseBuffer(SharedMemoryContext *context, BufferID buffer_id) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  BufferHeader *header_to_free = GetHeaderByIndex(context,
                                                  buffer_id.bits.header_index);
  if (header_to_free) {
    BeginTicketMutex(&pool->ticket_mutex);
    header_to_free->used = 0;
    header_to_free->in_use = false;
    int slab_index = GetSlabIndexFromHeader(context, header_to_free);
    DeviceID device_id = header_to_free->device_id;
    header_to_free->next_free = PeekFirstFreeBufferId(context, device_id,
                                                     slab_index);
    SetFirstFreeBufferId(context, device_id, slab_index, buffer_id);
    IncrementAvailableBuffers(context, device_id, slab_index);

    i64 capacity_adjustment = header_to_free->capacity;
    UpdateBufferingCapacities(context, capacity_adjustment, device_id);

    EndTicketMutex(&pool->ticket_mutex);
  }
}

void ReleaseBuffer(SharedMemoryContext *context, RpcContext *rpc,
                   BufferID buffer_id) {
  u32 target_node = buffer_id.bits.node_id;
  if (target_node == rpc->node_id) {
    LocalReleaseBuffer(context, buffer_id);
  } else {
    RpcCall<void>(rpc, target_node, "RemoteReleaseBuffer", buffer_id);
  }
}

void ReleaseBuffers(SharedMemoryContext *context, RpcContext *rpc,
                    const std::vector<BufferID> &buffer_ids) {
  for (auto id : buffer_ids) {
    ReleaseBuffer(context, rpc, id);
  }
}

void LocalReleaseBuffers(SharedMemoryContext *context,
                         const std::vector<BufferID> &buffer_ids) {
  for (auto id : buffer_ids) {
    LocalReleaseBuffer(context, id);
  }
}

BufferID GetFreeBuffer(SharedMemoryContext *context, DeviceID device_id,
                       int slab_index) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  BufferID result = {};

  BeginTicketMutex(&pool->ticket_mutex);
  BufferID id = PeekFirstFreeBufferId(context, device_id, slab_index);
  if (!IsNullBufferId(id)) {
    u32 header_index = id.bits.header_index;
    BufferHeader *header = GetHeaderByIndex(context, header_index);
    header->in_use = true;
    result = header->id;
    SetFirstFreeBufferId(context, device_id, slab_index, header->next_free);
    DecrementAvailableBuffers(context, device_id, slab_index);

    i64 capacity_adjustment = -(i64)header->capacity;
    UpdateBufferingCapacities(context, capacity_adjustment, device_id);
  }
  EndTicketMutex(&pool->ticket_mutex);

  return result;
}

std::vector<BufferID> GetBuffers(SharedMemoryContext *context,
                                 const PlacementSchema &schema) {
  BufferPool *pool = GetBufferPoolFromContext(context);

  bool failed = false;
  std::vector<BufferID> result;
  for (auto [size_left, target] : schema) {
    DeviceID device_id = GetDeviceIdFromTargetId(target);
    std::vector<size_t> num_buffers(pool->num_slabs[device_id], 0);

    // NOTE(chogan): naive buffer selection algorithm: fill with largest
    // buffers first
    for (int i = pool->num_slabs[device_id] - 1; i >= 0; --i) {
      size_t buffer_size = GetSlabBufferSize(context, device_id, i);
      size_t num_buffers = buffer_size ? size_left / buffer_size : 0;

      while (num_buffers > 0) {
        BufferID id = GetFreeBuffer(context, device_id, i);
        if (id.as_int) {
          result.push_back(id);
          BufferHeader *header = GetHeaderByBufferId(context, id);
          header->used = buffer_size;
          size_left -= buffer_size;
          num_buffers--;
        } else {
          // NOTE(chogan): Out of buffers in this slab. Go to next slab.
          break;
        }
      }
    }

    if (size_left > 0) {
      size_t buffer_size = GetSlabBufferSize(context, device_id, 0);
      BufferID id = GetFreeBuffer(context, device_id, 0);
      size_t used = std::min(buffer_size, size_left);
      size_left -= used;
      if (id.as_int && size_left == 0) {
        result.push_back(id);
        BufferHeader *header = GetHeaderByBufferId(context, id);
        header->used = used;
      } else {
        failed = true;
        DLOG(INFO) << "Not enough buffers to fulfill request" << std::endl;
      }
    }
  }

  if (failed) {
    // NOTE(chogan): All or none operation. Must release the acquired buffers if
    // we didn't get all we asked for
    LocalReleaseBuffers(context, result);
    result.clear();
  }

  return result;
}

u32 LocalGetBufferSize(SharedMemoryContext *context, BufferID id) {
  BufferHeader *header = GetHeaderByBufferId(context, id);
  u32 result = header->used;

  return result;
}

u32 GetBufferSize(SharedMemoryContext *context, RpcContext *rpc, BufferID id) {
  u32 result = 0;
  if (BufferIsRemote(rpc, id)) {
    result = RpcCall<u32>(rpc, id.bits.node_id, "RemoteGetBufferSize", id);
  } else {
    result = LocalGetBufferSize(context, id);
  }

  return result;
}

size_t GetBlobSize(SharedMemoryContext *context, RpcContext *rpc,
                   BufferIdArray *buffer_ids) {
  size_t result = 0;
  // TODO(chogan): @optimization Combine all ids on same node into 1 RPC
  for (u32 i = 0; i < buffer_ids->length; ++i) {
    u32 size = GetBufferSize(context, rpc, buffer_ids->ids[i]);
    result += size;
  }

  return result;
}

size_t GetBlobSizeById(SharedMemoryContext *context, RpcContext *rpc,
                       Arena *arena, BlobID blob_id) {
  size_t result = 0;
  BufferIdArray buffer_ids =
    GetBufferIdsFromBlobId(arena, context, rpc, blob_id, NULL);

  if (BlobIsInSwap(blob_id)) {
    SwapBlob swap_blob = IdArrayToSwapBlob(buffer_ids);
    result = swap_blob.size;
  } else {
    result = GetBlobSize(context, rpc, &buffer_ids);
  }

  return result;
}

ptrdiff_t GetBufferOffset(SharedMemoryContext *context, BufferID id) {
  BufferHeader *header = GetHeaderByIndex(context, id.bits.header_index);
  ptrdiff_t result = header->data_offset;

  return result;
}

u8 *GetRamBufferPtr(SharedMemoryContext *context, BufferID buffer_id) {
  ptrdiff_t data_offset = GetBufferOffset(context, buffer_id);
  u8 *result = context->shm_base + data_offset;

  return result;
}

BufferID MakeBufferId(u32 node_id, u32 header_index) {
  BufferID result = {};
  result.bits.node_id = node_id;
  result.bits.header_index = header_index;

  return result;
}

void PartitionRamBuffers(Arena *arena, i32 buffer_size, i32 buffer_count,
                         int block_size) {
  for (int i = 0; i < buffer_count; ++i) {
    int num_blocks_needed = buffer_size / block_size;
    [[maybe_unused]] u8 *first_buffer = PushArray<u8>(arena, block_size);

    for (int block = 0; block < num_blocks_needed - 1; ++block) {
      // NOTE(chogan): @optimization Since we don't have to store these
      // pointers, the only effect of this loop is that the arena will end up
      // with the correct "next free" address. This function isn't really
      // necessary; it's mainly just testing that everything adds up correctly.
      [[maybe_unused]] u8 *buffer = PushArray<u8>(arena, block_size);
      // NOTE(chogan): Make sure the buffers are perfectly aligned (no holes or
      // padding is introduced)
      [[maybe_unused]] i32 buffer_block_offset = (block + 1) * block_size;
      assert((u8 *)first_buffer == ((u8 *)buffer) - buffer_block_offset);
    }
  }
}

BufferID MakeBufferHeaders(Arena *arena, int buffer_size, u32 start_index,
                           u32 end_index, int node_id, DeviceID device_id,
                           ptrdiff_t initial_offset, u8 **header_begin) {
  BufferHeader dummy = {};
  BufferHeader *previous = &dummy;

  for (u32 i = start_index, j = 0; i < end_index; ++i, ++j) {
    BufferHeader *header = PushClearedStruct<BufferHeader>(arena);
    header->id = MakeBufferId(node_id, i);
    header->capacity = buffer_size;
    header->device_id = device_id;

    // NOTE(chogan): Stored as offset from base address of shared memory
    header->data_offset = buffer_size * j + initial_offset;

    previous->next_free = header->id;
    previous = header;

    // NOTE(chogan): Store the address of the first header so we can later
    // compute the `headers_offset`
    if (i == 0 && header_begin) {
      *header_begin = (u8 *)header;
    }
  }

  return dummy.next_free;
}

Device *InitDevices(Arena *arena, Config *config) {
  Device *result = PushArray<Device>(arena, config->num_devices);

  for (int i = 0; i < config->num_devices; ++i) {
    Device *device = result + i;
    device->bandwidth_mbps = config->bandwidths[i];
    device->latency_ns = config->latencies[i];
    device->id = i;
    // TODO(chogan): @configuration Get this from cmake.
    device->has_fallocate = false;
    size_t path_length = config->mount_points[i].size();

    if (path_length == 0) {
      device->is_byte_addressable = true;
    } else {
      // TODO(chogan): @errorhandling
      assert(path_length < kMaxPathLength);
      snprintf(device->mount_point, path_length + 1, "%s",
               config->mount_points[i].c_str());
    }
  }

  return result;
}

Target *InitTargets(Arena *arena, Config *config, Device *devices,
                    int node_id) {
  Target *result = PushArray<Target>(arena, config->num_targets);

  if (config->num_targets != config->num_devices) {
    HERMES_NOT_IMPLEMENTED_YET;
  }

  for (int i = 0; i < config->num_targets; ++i) {
    TargetID id = {};
    id.bits.node_id = node_id;
    id.bits.device_id = (DeviceID)i;
    id.bits.index = i;
    Target *target = result + i;
    target->id = id;
    target->capacity = config->capacities[i];
    target->remaining_space.store(config->capacities[i]);
    target->speed.store(devices[i].bandwidth_mbps);
  }

  return result;
}

// TODO(chogan): @testing Needs more testing for the case when the free list has
// been jumbled for a while. Currently we just test a nice linear free list.
void MergeRamBufferFreeList(SharedMemoryContext *context, int slab_index) {
  BufferPool *pool = GetBufferPoolFromContext(context);

  if (slab_index < 0 || slab_index >= pool->num_slabs[0] - 1) {
    // TODO(chogan): @logging
    return;
  }

  // TODO(chogan): @configuration Assumes RAM is first Device
  int this_slab_unit_size = GetSlabUnitSize(context, 0, slab_index);
  int bigger_slab_unit_size = GetSlabUnitSize(context, 0, slab_index + 1);

  // TODO(chogan): Currently just handling the case where the next slab size is
  // perfectly divisible by this slab's size
  if (this_slab_unit_size == 0 ||
      bigger_slab_unit_size % this_slab_unit_size != 0) {
    // TODO(chogan): @logging
    return;
  }

  int merge_factor = bigger_slab_unit_size / this_slab_unit_size;
  int new_slab_size_in_bytes = bigger_slab_unit_size * pool->block_sizes[0];
  int old_slab_size_in_bytes = this_slab_unit_size * pool->block_sizes[0];

  BeginTicketMutex(&pool->ticket_mutex);
  // TODO(chogan): Assuming first Device is RAM
  DeviceID device_id = 0;
  BufferID id = PeekFirstFreeBufferId(context, device_id, slab_index);

  while (id.as_int != 0) {
    BufferHeader *header_to_merge = GetHeaderByIndex(context,
                                                     id.bits.header_index);
    ptrdiff_t initial_offset = header_to_merge->data_offset;

    // NOTE(chogan): First go through the next `merge_factor` buffers and see if
    // a merge is possible.
    bool have_enough_buffers = false;
    bool buffers_are_contiguous = true;
    bool buffer_offsets_are_ascending = true;
    BufferID id_copy = id;
    for (int i = 0;
         i < merge_factor && id_copy.as_int != 0 && buffers_are_contiguous;
         ++i) {
      BufferHeader *header = GetHeaderByBufferId(context, id_copy);
      if (i != 0) {
        // NOTE(chogan): Contiguous buffers may be in either ascending or
        // descending order
        int buffer_offset = i * old_slab_size_in_bytes;
        buffer_offsets_are_ascending = header->data_offset > initial_offset;
        buffer_offset *= buffer_offsets_are_ascending ? 1 : -1;
        if (initial_offset + buffer_offset != header->data_offset) {
          buffers_are_contiguous = false;
        }
      }
      if (i == merge_factor - 1) {
        have_enough_buffers = true;
      }
      id_copy = header->next_free;
    }

    if (have_enough_buffers && buffers_are_contiguous) {
      int saved_free_list_count = 0;
      // TODO(chogan): What's the max this number could be? Could save the tail
      // and attach to the end of the free list rather than doing all this
      // popping, saving, and pushing
      const int max_saved_entries = 64;
      BufferID saved_free_list_entries[max_saved_entries] = {};
      // NOTE(chogan): Pop `merge_factor` entries off the free list for
      // `slab_index`
      id_copy = id;
      for (int i = 0;
           i < merge_factor;
           ++i) {
        BufferHeader *header = GetHeaderByBufferId(context, id_copy);

        while (header->id.as_int !=
               PeekFirstFreeBufferId(context, device_id, slab_index).as_int) {
          // NOTE(chogan): It's possible that the buffer we're trying to pop
          // from the free list is not at the beginning of the list. In that
          // case, we have to pop and save all the free buffers before the one
          // we're interested in, and then restore them to the free list later.
          assert(saved_free_list_count < max_saved_entries);
          saved_free_list_entries[saved_free_list_count++] =
            PeekFirstFreeBufferId(context, device_id, slab_index);

          BufferID first_id = PeekFirstFreeBufferId(context, device_id,
                                                   slab_index);
          BufferHeader *first_free = GetHeaderByBufferId(context, first_id);
          SetFirstFreeBufferId(context, device_id, slab_index,
                               first_free->next_free);
        }

        SetFirstFreeBufferId(context, device_id, slab_index, header->next_free);
        id_copy = header->next_free;
        MakeHeaderDormant(header);
      }

      ResetHeader(header_to_merge);
      if (buffer_offsets_are_ascending) {
        header_to_merge->data_offset = initial_offset;
      } else {
        // NOTE(chogan): In this case `initial_offset` points at the beginning
        // of the last of `this_slab_unit_size` buffers. We need to back it up
        // to the correct position.
        ptrdiff_t back_up_count = old_slab_size_in_bytes * (merge_factor - 1);
        header_to_merge->data_offset = initial_offset - back_up_count;
      }
      header_to_merge->capacity = new_slab_size_in_bytes;

      // NOTE(chogan): Add the new header to the next size up's free list
      header_to_merge->next_free = PeekFirstFreeBufferId(context, device_id,
                                                         slab_index + 1);
      SetFirstFreeBufferId(context, device_id, slab_index + 1,
                           header_to_merge->id);

      while (saved_free_list_count > 0) {
        // NOTE(chogan): Restore headers that we popped and saved.
        BufferID saved_id = saved_free_list_entries[--saved_free_list_count];
        BufferHeader *saved_header = GetHeaderByBufferId(context, saved_id);
        saved_header->next_free = PeekFirstFreeBufferId(context, device_id,
                                                        slab_index);
        SetFirstFreeBufferId(context, device_id, slab_index, saved_header->id);
      }

      id = id_copy;
    } else {
      // NOTE(chogan): Didn't have enough contiguous buffers for a merge. Try
      // the next free buffer.
      id = header_to_merge->next_free;
    }
  }
  EndTicketMutex(&pool->ticket_mutex);
}

// TODO(chogan): @testing Needs more testing for the case when the free list has
// been jumbled for a while. Currently we just test a nice linear free list.
void SplitRamBufferFreeList(SharedMemoryContext *context, int slab_index) {
  BufferPool *pool = GetBufferPoolFromContext(context);

  if (slab_index < 1 || slab_index >= pool->num_slabs[0]) {
    // TODO(chogan): @logging
    return;
  }

  int this_slab_unit_size = GetSlabUnitSize(context, 0, slab_index);
  int smaller_slab_unit_size = GetSlabUnitSize(context, 0, slab_index - 1);

  // TODO(chogan): Currently just handling the case where this slab size is
  // perfectly divisible by the next size down
  assert(smaller_slab_unit_size &&
         this_slab_unit_size % smaller_slab_unit_size == 0);

  int split_factor = this_slab_unit_size / smaller_slab_unit_size;
  int new_slab_size_in_bytes = smaller_slab_unit_size * pool->block_sizes[0];

  // TODO(chogan): @optimization We don't really want to wait for a long queue
  // on the ticket mutex. If we need to split, we want to stop the world and do
  // it immediately.
  BeginTicketMutex(&pool->ticket_mutex);
  // TODO(chogan): Assuming first Device is RAM
  DeviceID device_id = 0;
  BufferID id = PeekFirstFreeBufferId(context, device_id, slab_index);
  u32 unused_header_index = 0;
  BufferHeader *headers = GetHeadersBase(context);
  BufferHeader *next_unused_header = &headers[unused_header_index];

  while (id.as_int != 0) {
    BufferHeader *header_to_split = GetHeaderByIndex(context,
                                                     id.bits.header_index);
    ptrdiff_t old_data_offset = header_to_split->data_offset;
    SetFirstFreeBufferId(context, device_id, slab_index,
                         header_to_split->next_free);
    id = header_to_split->next_free;

    for (int i = 0; i < split_factor; ++i) {
      // NOTE(chogan): Find the next dormant header. This is easy to optimize
      // when splitting since we can keep the live and dormant headers separate
      // and store `first_dormant_header`, but this isn't possible when merging
      // (because we can't move headers that are in use). So, we have to scan
      // the array.
      if (i == 0) {
        // NOTE(chogan): Reuse this header as the first unused one
        next_unused_header = header_to_split;
      } else {
        while (!HeaderIsDormant(next_unused_header)) {
          // NOTE(chogan): Assumes first Device is RAM
          if (++unused_header_index >= pool->num_headers[0]) {
            unused_header_index = 0;
          }
          next_unused_header = &headers[unused_header_index];
        }
      }

      ResetHeader(next_unused_header);
      next_unused_header->data_offset = old_data_offset;
      next_unused_header->capacity = new_slab_size_in_bytes;

      next_unused_header->next_free = PeekFirstFreeBufferId(context, device_id,
                                                            slab_index - 1);
      SetFirstFreeBufferId(context, device_id, slab_index - 1,
                           next_unused_header->id);

      old_data_offset += new_slab_size_in_bytes;
    }
  }
  EndTicketMutex(&pool->ticket_mutex);
}

ptrdiff_t InitBufferPool(u8 *shmem_base, Arena *buffer_pool_arena,
                         Arena *scratch_arena, i32 node_id, Config *config) {
  ScopedTemporaryMemory scratch(scratch_arena);

  i32 **buffer_counts = PushArray<i32*>(scratch, config->num_devices);
  i32 **slab_buffer_sizes = PushArray<i32*>(scratch, config->num_devices);
  i32 *header_counts = PushArray<i32>(scratch, config->num_devices);

  for (int device = 0; device < config->num_devices; ++device) {
    slab_buffer_sizes[device] = PushArray<i32>(scratch,
                                               config->num_slabs[device]);
    buffer_counts[device] = PushArray<i32>(scratch, config->num_slabs[device]);

    for (int slab = 0; slab < config->num_slabs[device]; ++slab) {
      slab_buffer_sizes[device][slab] = (config->block_sizes[device] *
                                       config->slab_unit_sizes[device][slab]);
      f32 slab_percentage = config->desired_slab_percentages[device][slab];
      size_t bytes_for_slab = (size_t)((f32)config->capacities[device] *
                                       slab_percentage);
      buffer_counts[device][slab] = (bytes_for_slab /
                                   slab_buffer_sizes[device][slab]);
    }
  }

  // NOTE(chogan): One header per RAM block to allow for splitting and merging
  // TODO(chogan): @configuration Assumes first Device is RAM
  // TODO(chogan): Allow splitting and merging for every Device
  assert(config->capacities[0] % config->block_sizes[0] == 0);
  header_counts[0] = config->capacities[0] / config->block_sizes[0];

  for (int device = 1; device < config->num_devices; ++device) {
    header_counts[device] = 0;
    for (int slab = 0; slab < config->num_slabs[device]; ++slab) {
      header_counts[device] += buffer_counts[device][slab];
    }
  }

  // NOTE(chogan): Anything stored in the buffer_pool_arena (besides buffers)
  // needs to be accounted for here. It would be nice to have a compile time
  // check that makes sure anything we allocate for the buffer pool but outside
  // of it gets accounted for here.

  i32 max_headers_needed = 0;
  size_t free_lists_size = 0;
  size_t slab_metadata_size = 0;
  for (int device = 0; device < config->num_devices; ++device) {
    max_headers_needed += header_counts[device];
    free_lists_size += config->num_slabs[device] * sizeof(BufferID);
    // NOTE(chogan): The '* 2' is because we have an i32 for both slab unit size
    // and slab buffer size
    slab_metadata_size += config->num_slabs[device] * sizeof(i32) * 2;
    // NOTE(chogan): buffers_available array
    slab_metadata_size += config->num_slabs[device] * sizeof(u32);
  }

  size_t headers_size = max_headers_needed * sizeof(BufferHeader);
  size_t devices_size = config->num_devices * sizeof(Device);
  size_t buffer_pool_size = (sizeof(BufferPool) + free_lists_size +
                             slab_metadata_size);

  // IMPORTANT(chogan): Currently, no additional bytes are added for alignment.
  // However, if we add more metadata to the BufferPool in the future, automatic
  // alignment could make this number larger than we think. `PushSize` will
  // print out when it adds alignment padding, so for now we can monitor that.
  // In the future it would be nice to have a programatic way to account for
  // alignment padding.
  size_t required_bytes_for_metadata = (headers_size + buffer_pool_size +
                                        devices_size);

  size_t required_bytes_for_metadata_rounded =
    RoundUpToMultiple(required_bytes_for_metadata, config->block_sizes[0]);
  i32 num_blocks_reserved_for_metadata = (required_bytes_for_metadata_rounded /
                                          config->block_sizes[0]);
  // NOTE(chogan): Remove some of the smallest RAM buffers to make room for
  // metadata
  buffer_counts[0][0] -= num_blocks_reserved_for_metadata;
  // NOTE(chogan): We need fewer headers because we have fewer buffers now
  header_counts[0] -= num_blocks_reserved_for_metadata;
  // NOTE(chogan): Adjust the config capacity for RAM to reflect the actual
  // capacity for buffering (excluding BufferPool metadata).
  assert(config->capacities[0] > required_bytes_for_metadata_rounded);
  config->capacities[0] -= required_bytes_for_metadata_rounded;

  u32 total_headers = max_headers_needed - num_blocks_reserved_for_metadata;

  int *num_buffers = PushArray<int>(scratch_arena, config->num_devices);
  int total_buffers = 0;
  for (int device = 0; device < config->num_devices; ++device) {
    DLOG(INFO) << "Device: " << device << std::endl;
    num_buffers[device] = 0;
    for (int slab = 0; slab < config->num_slabs[device]; ++slab) {
      DLOG(INFO) << "    " << slab << "-Buffers: "
                 << buffer_counts[device][slab] << std::endl;
      num_buffers[device] += buffer_counts[device][slab];
    }
    total_buffers += num_buffers[device];
    DLOG(INFO) << "    Num Headers: " << header_counts[device] << std::endl;
    DLOG(INFO) << "    Num Buffers: " << num_buffers[device] << std::endl;
  }
  DLOG(INFO) << "Total Buffers: " << total_buffers << std::endl;;

  // Build RAM buffers.

  // TODO(chogan): @configuration Assumes the first Device is RAM
  for (int slab = 0; slab < config->num_slabs[0]; ++slab) {
    PartitionRamBuffers(buffer_pool_arena, slab_buffer_sizes[0][slab],
                        buffer_counts[0][slab], config->block_sizes[0]);
  }

  // Init Devices and Targets

  Device *devices = InitDevices(buffer_pool_arena, config);

  Target *targets = InitTargets(buffer_pool_arena, config, devices, node_id);

  // Create Free Lists

  BufferID **free_lists = PushArray<BufferID*>(scratch_arena,
                                               config->num_devices);
  for (int device = 0; device < config->num_devices; ++device) {
    free_lists[device] = PushArray<BufferID>(scratch_arena,
                                           config->num_slabs[device]);
  }

  // Build BufferHeaders

  u32 start = 0;
  u8 *header_begin = 0;
  ptrdiff_t initial_offset = 0;
  // TODO(chogan): @configuration Assumes first Device is RAM
  for (i32 i = 0; i < config->num_slabs[0]; ++i) {
    u32 end = start + buffer_counts[0][i];
    DeviceID ram_device_id = 0;
    free_lists[ram_device_id][i] =
      MakeBufferHeaders(buffer_pool_arena, slab_buffer_sizes[0][i], start, end,
                        node_id, ram_device_id, initial_offset, &header_begin);
    start = end;
    initial_offset += buffer_counts[0][i] * slab_buffer_sizes[0][i];
  }

  // NOTE(chogan): Add remaining unused RAM headers
  for (u32 i = num_buffers[0]; i < (u32)header_counts[0]; ++i) {
    BufferHeader *header = PushClearedStruct<BufferHeader>(buffer_pool_arena);
    header->id = MakeBufferId(node_id, i);
    start += 1;
  }

  // NOTE(chogan): Add headers for the other Devices
  for (int device = 1; device < config->num_devices; ++device) {
    for (int slab = 0; slab < config->num_slabs[device]; ++slab) {
      // NOTE(chogan): File buffering scheme is one file per slab per Device
      u32 end = start + buffer_counts[device][slab];
      free_lists[device][slab] =
        MakeBufferHeaders(buffer_pool_arena, slab_buffer_sizes[device][slab],
                          start, end, node_id, device, 0, 0);
      start = end;
    }
  }

  // Build BufferPool

  BufferPool *pool = PushClearedStruct<BufferPool>(buffer_pool_arena);
  pool->headers_offset = header_begin - shmem_base;
  pool->devices_offset = (u8 *)devices - shmem_base;
  pool->targets_offset = (u8 *)targets - shmem_base;
  pool->num_devices = config->num_devices;
  pool->total_headers = total_headers;

  for (int device = 0; device < config->num_devices; ++device) {
    pool->block_sizes[device] = config->block_sizes[device];
    pool->num_headers[device] = header_counts[device];
    pool->num_slabs[device] = config->num_slabs[device];
    BufferID *free_list = PushArray<BufferID>(buffer_pool_arena,
                                              config->num_slabs[device]);
    i32 *slab_unit_sizes = PushArray<i32>(buffer_pool_arena,
                                          config->num_slabs[device]);
    i32 *slab_buffer_sizes_for_device = PushArray<i32>(buffer_pool_arena,
                                            config->num_slabs[device]);
    std::atomic<u32> *available_buffers =
      PushArray<std::atomic<u32>>(buffer_pool_arena, config->num_slabs[device]);

    for (int slab = 0; slab < config->num_slabs[device]; ++slab) {
      free_list[slab] = free_lists[device][slab];
      slab_unit_sizes[slab] = config->slab_unit_sizes[device][slab];
      slab_buffer_sizes_for_device[slab] = slab_buffer_sizes[device][slab];
      available_buffers[slab] = buffer_counts[device][slab];
    }
    pool->free_list_offsets[device] = (u8 *)free_list - shmem_base;
    pool->slab_unit_sizes_offsets[device] = (u8 *)slab_unit_sizes - shmem_base;
    pool->slab_buffer_sizes_offsets[device] =
      ((u8 *)slab_buffer_sizes_for_device - shmem_base);
    pool->buffers_available_offsets[device] =
      ((u8 *)available_buffers - shmem_base);
  }

  // NOTE(chogan): The buffer pool offset is stored at the beginning of shared
  // memory so the client processes can read it on initialization
  ptrdiff_t *buffer_pool_offset_location = (ptrdiff_t *)shmem_base;
  ptrdiff_t buffer_pool_offset = (u8 *)pool - shmem_base;
  *buffer_pool_offset_location = buffer_pool_offset;

  return buffer_pool_offset;
}

void SerializeBufferPoolToFile(SharedMemoryContext *context, FILE *file) {
  int result = fwrite(context->shm_base, context->shm_size, 1, file);

  if (result < 1) {
    // TODO(chogan): @errorhandling
    perror("Failed to serialize BufferPool to file");
  }
}

// Per-rank application side initialization

void MakeFullShmemName(char *dest, const char *base) {
  size_t base_name_length = strlen(base);
  snprintf(dest, base_name_length + 1, "%s", base);
  char *username = getenv("USER");
  if (username) {
    size_t username_length = strlen(username);
    [[maybe_unused]] size_t total_length =
      base_name_length + username_length + 1;
    // TODO(chogan): @errorhandling
    assert(total_length < kMaxBufferPoolShmemNameLength);
    snprintf(dest + base_name_length, username_length + 1, "%s", username);
  }
}

void InitFilesForBuffering(SharedMemoryContext *context, bool make_space) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  context->buffering_filenames.resize(pool->num_devices);

  // TODO(chogan): Check the limit for open files via getrlimit. We might have
  // to do some smarter opening and closing to stay under the limit. Could also
  // increase the soft limit to the hard limit.
  for (int device_id = 0; device_id < pool->num_devices; ++device_id) {
    Device *device = GetDeviceById(context, device_id);
    char *mount_point = &device->mount_point[0];

    if (strlen(mount_point) == 0) {
      // NOTE(chogan): RAM Device. No need for a file.
      continue;
    }

    bool ends_in_slash = mount_point[strlen(mount_point) - 1] == '/';
    context->buffering_filenames[device_id].resize(pool->num_slabs[device_id]);

    for (int slab = 0; slab < pool->num_slabs[device_id]; ++slab) {
      // TODO(chogan): Where does memory for filenames come from? Probably need
      // persistent memory for each application core.
      context->buffering_filenames[device_id][slab] =
        std::string(std::string(mount_point) + (ends_in_slash ? "" : "/") +
                    "device" + std::to_string(device_id) + "_slab" +
                    std::to_string(slab) + ".hermes");

      const char *buffering_fname =
        context->buffering_filenames[device_id][slab].c_str();
      FILE *buffering_file = fopen(buffering_fname, "w+");
      if (make_space) {
        if (device->has_fallocate) {
          // TODO(chogan): Use posix_fallocate when it is available
        } else {
          // TODO(chogan): Some Devices require file initialization on each
          // node, and some are shared (burst buffers) and only require one rank
          // to initialize them

          Target *target = GetTarget(context, device_id);
          [[maybe_unused]] int ftruncate_result =
            ftruncate(fileno(buffering_file), target->capacity);
          // TODO(chogan): @errorhandling
          assert(ftruncate_result == 0);
        }
      }
      context->open_streams[device_id][slab] = buffering_file;
    }
  }
}

u8 *InitSharedMemory(const char *shmem_name, size_t total_size) {
  u8 *result = 0;
  int shmem_fd =
    shm_open(shmem_name, O_CREAT | O_RDWR | O_TRUNC, S_IRUSR | S_IWUSR);

  if (shmem_fd >= 0) {
    [[maybe_unused]] int ftruncate_result = ftruncate(shmem_fd, total_size);
    // TODO(chogan): @errorhandling
    assert(ftruncate_result == 0);

    result = (u8 *)mmap(0, total_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                        shmem_fd, 0);
    // TODO(chogan): @errorhandling
    close(shmem_fd);
  } else {
    // TODO(chogan): @errorhandling
    assert(!"shm_open failed\n");
  }

  // TODO(chogan): @errorhandling
  assert(result);

  return result;
}

SharedMemoryContext GetSharedMemoryContext(char *shmem_name) {
  SharedMemoryContext result = {};

  int shmem_fd = shm_open(shmem_name, O_RDWR, S_IRUSR | S_IWUSR);

  if (shmem_fd >= 0) {
    struct stat shm_stat;
    if (fstat(shmem_fd, &shm_stat) == 0) {
      u8 *shm_base = (u8 *)mmap(0, shm_stat.st_size, PROT_READ | PROT_WRITE,
                                MAP_SHARED, shmem_fd, 0);
      close(shmem_fd);

      if (shm_base) {
        // NOTE(chogan): On startup, the buffer_pool_offset will be stored at
        // the beginning of the shared memory segment, and the
        // metadata_arena_offset will be stored immediately after that.
        ptrdiff_t *buffer_pool_offset_location = (ptrdiff_t *)shm_base;
        result.buffer_pool_offset = *buffer_pool_offset_location;
        ptrdiff_t *metadata_manager_offset_location =
          (ptrdiff_t *)(shm_base + sizeof(result.buffer_pool_offset));
        result.metadata_manager_offset = *metadata_manager_offset_location;
        result.shm_base = shm_base;
        result.shm_size = shm_stat.st_size;
      } else {
        // TODO(chogan): @logging Error handling
        perror("mmap_failed");
      }
    } else {
      // TODO(chogan): @logging Error handling
      perror("fstat failed");
    }
  } else {
    // TODO(chogan): @logging Error handling
    perror("shm_open failed");
  }

  return result;
}

void UnmapSharedMemory(SharedMemoryContext *context) {
  munmap(context->shm_base, context->shm_size);
}

void ReleaseSharedMemoryContext(SharedMemoryContext *context) {
  BufferPool *pool = GetBufferPoolFromContext(context);

  for (int device_id = 0; device_id < pool->num_devices; ++device_id) {
    for (int slab = 0; slab < pool->num_slabs[device_id]; ++slab) {
      if (context->open_streams[device_id][slab]) {
        int fclose_result = fclose(context->open_streams[device_id][slab]);
        if (fclose_result != 0) {
          // TODO(chogan): @errorhandling
          HERMES_NOT_IMPLEMENTED_YET;
        }
      }
    }
  }

  if (context->swap_file) {
    if (fclose(context->swap_file) != 0) {
      // TODO(chogan): @errorhandling
      HERMES_NOT_IMPLEMENTED_YET;
    }
  }

  UnmapSharedMemory(context);
}

// IO clients

size_t LocalWriteBufferById(SharedMemoryContext *context, BufferID id,
                            const Blob &blob, size_t offset) {
  BufferHeader *header = GetHeaderByIndex(context, id.bits.header_index);
  Device *device = GetDeviceFromHeader(context, header);
  size_t write_size = header->used;

  // TODO(chogan): Should this be a TicketMutex? It seems that at any
  // given time, only the DataOrganizer and an application core will
  // be trying to write to/from the same BufferID. In that case, it's
  // first come first serve. However, if it turns out that more
  // threads will be trying to lock the buffer, we may need to enforce
  // ordering.
  LockBuffer(header);

  u8 *at = (u8 *)blob.data + offset;
  if (device->is_byte_addressable) {
    u8 *dest = GetRamBufferPtr(context, header->id);
    memcpy(dest, at, write_size);
  } else {
    int slab_index = GetSlabIndexFromHeader(context, header);
    FILE *file = context->open_streams[device->id][slab_index];
    if (!file) {
      // TODO(chogan): Check number of opened files against maximum allowed.
      // May have to close something.
      const char *filename =
        context->buffering_filenames[device->id][slab_index].c_str();
      file = fopen(filename, "r+");
      context->open_streams[device->id][slab_index] = file;
    }
    fseek(file, header->data_offset, SEEK_SET);
    [[maybe_unused]] size_t items_written = fwrite(at, write_size, 1, file);
    // TODO(chogan): @errorhandling
    assert(items_written == 1);
    if (fflush(file) != 0) {
      // TODO(chogan): @errorhandling
      LOG(WARNING) << "fflush failed\n";
    }
    // fsync(fileno(file));
  }
  UnlockBuffer(header);

  return write_size;
}

void WriteBlobToBuffers(SharedMemoryContext *context, RpcContext *rpc,
                        const Blob &blob,
                        const std::vector<BufferID> &buffer_ids) {
  size_t bytes_left_to_write = blob.size;
  size_t offset = 0;
  // TODO(chogan): @optimization Handle sequential buffers as one I/O operation
  // TODO(chogan): @optimization Aggregate multiple RPCs into one
  for (const auto &id : buffer_ids) {
    size_t bytes_written = 0;
    if (BufferIsRemote(rpc, id)) {
      // TODO(chogan): @optimization Set up bulk transfer if blob.size is > 4K
      // TODO(chogan): @optimization Only send the portion of the blob we have
      // to write.
      // TODO(chogan): @optimization Avoid copy
      std::vector<u8> data(blob.size);
      memcpy(data.data(), blob.data, blob.size);
      bytes_written = RpcCall<size_t>(rpc, id.bits.node_id,
                                      "RemoteWriteBufferById", id, data,
                                      offset);
    } else {
      bytes_written = LocalWriteBufferById(context, id, blob, offset);
    }
    bytes_left_to_write -= bytes_written;
    offset += bytes_written;
  }
  assert(offset == blob.size);
  assert(bytes_left_to_write == 0);
}

size_t LocalReadBufferById(SharedMemoryContext *context, BufferID id,
                           Blob *blob, size_t read_offset) {
  BufferHeader *header = GetHeaderByIndex(context, id.bits.header_index);
  Device *device = GetDeviceFromHeader(context, header);
  size_t read_size = header->used;

  // TODO(chogan): Should this be a TicketMutex? It seems that at any
  // given time, only the DataOrganizer and an application core will
  // be trying to write to/from the same BufferID. In that case, it's
  // first come first serve. However, if it turns out that more
  // threads will be trying to lock the buffer, we may need to enforce
  // ordering.
  LockBuffer(header);

  size_t result = 0;
  if (device->is_byte_addressable) {
    u8 *src = GetRamBufferPtr(context, header->id);
    memcpy((u8 *)blob->data + read_offset, src, read_size);
    result = read_size;
  } else {
    int slab_index = GetSlabIndexFromHeader(context, header);
    FILE *file = context->open_streams[device->id][slab_index];
    if (!file) {
      // TODO(chogan): Check number of opened files against maximum allowed.
      // May have to close something.
      const char *filename =
        context->buffering_filenames[device->id][slab_index].c_str();
      file = fopen(filename, "r+");
    }
    fseek(file, header->data_offset, SEEK_SET);
    size_t items_read = fread((u8 *)blob->data + read_offset, read_size, 1,
                              file);
    // TODO(chogan): @errorhandling
    assert(items_read == 1);
    result = items_read * read_size;
  }
  UnlockBuffer(header);

  return result;
}

size_t ReadBlobFromBuffers(SharedMemoryContext *context, RpcContext *rpc,
                           Blob *blob, BufferIdArray *buffer_ids,
                           u32 *buffer_sizes) {
  size_t total_bytes_read = 0;
  // TODO(chogan): @optimization Handle sequential buffers as one I/O operation
  for (u32 i = 0; i < buffer_ids->length; ++i) {
    size_t bytes_read = 0;
    BufferID id = buffer_ids->ids[i];
    if (BufferIsRemote(rpc, id)) {
      // TODO(chogan): @optimization Aggregate multiple RPCs to same node into
      // one RPC.
      if (buffer_sizes[i] > KILOBYTES(4)) {
        size_t bytes_transferred = BulkRead(rpc, id.bits.node_id,
                                            "RemoteBulkReadBufferById",
                                            blob->data + total_bytes_read,
                                            buffer_sizes[i], id);
        // TODO(chogan): @errorhandling
        assert(bytes_transferred == buffer_sizes[i]);
        bytes_read += bytes_transferred;
      } else {
        std::vector<u8> data = RpcCall<std::vector<u8>>(rpc, id.bits.node_id,
                                                        "RemoteReadBufferById",
                                                        id, buffer_sizes[i]);
        bytes_read = data.size();
        // TODO(chogan): @optimization Avoid the copy
        memcpy(blob->data, data.data(), bytes_read);
      }
    } else {
      bytes_read = LocalReadBufferById(context, id, blob, total_bytes_read);
    }
    total_bytes_read += bytes_read;
  }
  // TODO(chogan): @errorhandling
  assert(total_bytes_read == blob->size);

  return total_bytes_read;
}

size_t ReadBlobById(SharedMemoryContext *context, RpcContext *rpc, Arena *arena,
                    api::Blob &dest, BlobID blob_id) {
  size_t result = 0;
  hermes::Blob blob = {};
  blob.data = dest.data();
  blob.size = dest.size();

  BufferIdArray buffer_ids = {};
  if (hermes::BlobIsInSwap(blob_id)) {
    buffer_ids = GetBufferIdsFromBlobId(arena, context, rpc, blob_id, NULL);
    SwapBlob swap_blob = IdArrayToSwapBlob(buffer_ids);
    result = ReadFromSwap(context, blob, swap_blob);
  } else {
    u32 *buffer_sizes = 0;
    buffer_ids = GetBufferIdsFromBlobId(arena, context, rpc, blob_id,
                                          &buffer_sizes);
    result = ReadBlobFromBuffers(context, rpc, &blob, &buffer_ids,
                                 buffer_sizes);
  }

  return result;
}

int OpenSwapFile(SharedMemoryContext *context, u32 node_id) {
  int result = 0;

  if (!context->swap_file) {
    MetadataManager *mdm = GetMetadataManagerFromContext(context);
    std::string swap_path = GetSwapFilename(mdm, node_id);
    context->swap_file = fopen(swap_path.c_str(), "a+");

    if (!context->swap_file) {
      // TODO(chogan): @errorhandling
      result = 1;
    }
  }

  return result;
}

SwapBlob WriteToSwap(SharedMemoryContext *context, Blob blob, u32 node_id,
                     BucketID bucket_id) {
  SwapBlob result = {};

  if (OpenSwapFile(context, node_id) == 0) {
    if (fseek(context->swap_file, 0, SEEK_END) != 0) {
      // TODO(chogan): @errorhandling
      HERMES_NOT_IMPLEMENTED_YET;
    }

    long int file_position = ftell(context->swap_file);
    if (file_position == -1) {
      // TODO(chogan): @errorhandling
      HERMES_NOT_IMPLEMENTED_YET;
    }
    result.offset = file_position;

    if (fwrite(blob.data, 1, blob.size, context->swap_file) != blob.size) {
      // TODO(chogan): @errorhandling
      HERMES_NOT_IMPLEMENTED_YET;
    }

    if (fflush(context->swap_file) != 0) {
      // TODO(chogan): @errorhandling
      HERMES_NOT_IMPLEMENTED_YET;
    }
  } else {
    // TODO(chogan): @errorhandling
    HERMES_NOT_IMPLEMENTED_YET;
  }

  result.node_id = node_id;
  result.bucket_id = bucket_id;
  result.size = blob.size;

  return result;
}

SwapBlob PutToSwap(SharedMemoryContext *context, RpcContext *rpc,
                   const std::string &name, BucketID bucket_id, const u8 *data,
                   size_t size) {
  hermes::Blob blob = {};
  blob.data = (u8 *)data;
  blob.size = size;

  u32 target_node = rpc->node_id;
  SwapBlob swap_blob =  WriteToSwap(context, blob, target_node, bucket_id);
  std::vector<BufferID> buffer_ids = SwapBlobToVec(swap_blob);
  AttachBlobToBucket(context, rpc, name.c_str(), bucket_id, buffer_ids, true);

  return swap_blob;
}

size_t ReadFromSwap(SharedMemoryContext *context, Blob blob,
                  SwapBlob swap_blob) {
  u32 node_id = swap_blob.node_id;
  if (OpenSwapFile(context, node_id) == 0) {
    if (fseek(context->swap_file, swap_blob.offset, SEEK_SET) != 0) {
      // TODO(chogan): @errorhandling
      HERMES_NOT_IMPLEMENTED_YET;
    }

    if (fread(blob.data, 1, swap_blob.size, context->swap_file) !=
        swap_blob.size) {
      // TODO(chogan): @errorhandling
      HERMES_NOT_IMPLEMENTED_YET;
    }

  } else {
    // TODO(chogan): @errorhandling
    HERMES_NOT_IMPLEMENTED_YET;
  }

  return swap_blob.size;
}

Status PlaceBlob(SharedMemoryContext *context, RpcContext *rpc,
                 PlacementSchema &schema, Blob blob, const char *name,
                 BucketID bucket_id) {
  Status result = 0;
  HERMES_BEGIN_TIMED_BLOCK("GetBuffers");
  std::vector<BufferID> buffer_ids = GetBuffers(context, schema);
  HERMES_END_TIMED_BLOCK();

  if (buffer_ids.size()) {
    MetadataManager *mdm = GetMetadataManagerFromContext(context);
    char *bucket_name = ReverseGetFromStorage(mdm, bucket_id.as_int,
                                              kMapType_Bucket);
    LOG(INFO) << "Attaching blob " << std::string(name) << " to Bucket "
              << bucket_name << std::endl;

    HERMES_BEGIN_TIMED_BLOCK("WriteBlobToBuffers");
    WriteBlobToBuffers(context, rpc, blob, buffer_ids);
    HERMES_END_TIMED_BLOCK();

    // NOTE(chogan): Update all metadata associated with this Put
    AttachBlobToBucket(context, rpc, name, bucket_id, buffer_ids);
  } else {
    // TODO(chogan): @errorhandling
    result = 1;
  }

  return result;
}

Status StdIoPersistBucket(SharedMemoryContext *context, RpcContext *rpc,
                          Arena *arena, BucketID bucket_id,
                          const std::string &file_name,
                          const std::string &open_mode) {
  Status result = 0;
  FILE *file = fopen(file_name.c_str(), open_mode.c_str());

  if (file) {
    std::vector<BlobID> blob_ids = GetBlobIds(context, rpc, bucket_id);
    for (size_t i = 0; i < blob_ids.size(); ++i) {
      ScopedTemporaryMemory scratch(arena);
      size_t blob_size = GetBlobSizeById(context, rpc, arena, blob_ids[i]);
      // TODO(chogan): @optimization We could use the actual Hermes buffers as
      // the write buffer rather than collecting the whole blob into memory. For
      // now we pay the cost of data copy in order to only do one I/O call.
      api::Blob data(blob_size);
      size_t num_bytes = blob_size > 0 ? sizeof(data[0]) * blob_size : 0;
      if (ReadBlobById(context, rpc, arena, data, blob_ids[i]) == blob_size) {
        // TODO(chogan): For now we just write the blobs in the order in which
        // they were `Put`, but once we have a Trait that represents a file
        // mapping, we'll need pwrite and offsets.
        if (fwrite(data.data(), 1, num_bytes, file) != num_bytes) {
          // TODO(chogan): @errorhandling
          result = 1;
          break;
        }
      } else {
        // TODO(chogan): @errorhandling
        result = 1;
        break;
      }
    }

    if (fclose(file) != 0) {
      // TODO(chogan): @errorhandling
      result = 1;
    }
  } else {
    // TODO(chogan): @errorhandling
    result = 1;
  }

  return result;
}

api::Status StdIoPersistBlob(SharedMemoryContext *context, RpcContext *rpc,
                             Arena *arena, BlobID blob_id,
                             FILE *file , const i32 &offset) {
  Status result = 0;

  if (file) {
    ScopedTemporaryMemory scratch(arena);
    size_t blob_size = GetBlobSizeById(context, rpc, arena, blob_id);
    // TODO(chogan): @optimization We could use the actual Hermes buffers as
    // the write buffer rather than collecting the whole blob into memory. For
    // now we pay the cost of data copy in order to only do one I/O call.
    api::Blob data(blob_size);
    size_t num_bytes = blob_size > 0 ? sizeof(data[0]) * blob_size : 0;
    if (ReadBlobById(context, rpc, arena, data, blob_id) == blob_size) {
      // TODO(chogan): For now we just write the blobs in the order in which
      // they were `Put`, but once we have a Trait that represents a file
      // mapping, we'll need pwrite and offsets.
      if (offset == -1 || fseek(file, offset, SEEK_SET) == 0) {
        if (fwrite(data.data(), 1, num_bytes, file) != num_bytes) {
          // TODO(chogan): @errorhandling
          result = 1;
        }
      } else {
        // TODO(chogan): @errorhandling
      }
    }

  } else {
    // TODO(chogan): @errorhandling
    result = 1;
  }
  return result;
}

}  // namespace hermes
