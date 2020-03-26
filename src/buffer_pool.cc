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

#include <mpi.h>
#include <thallium.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/serialization/stl/pair.hpp>

#include "memory_arena.cc"

#if defined(HERMES_COMMUNICATION_MPI)
#include "communication_mpi.cc"
#elif defined(HERMES_COMMUNICATION_ZMQ)
#include "communication_zmq.cc"
#else
#error Communication implementation required.
#endif

/**
 * @file buffer_pool.cc
 *
 * Implementation of a BufferPool that lives in shared memory. Other processes
 * interact with the BufferPool by requesting buffers through the `GetBuffers`
 * call to reserve a set of `BufferID`s and then using those IDs for I/O. Remote
 * processes can request remote buffers via the `GetBuffers` RPC call.
 */

namespace tl = thallium;

namespace hermes {

void WorldBarrier(CommunicationContext *comm) {
  comm->world_barrier(comm->state);
}

inline void BeginTicketMutex(TicketMutex *mutex) {
  u32 ticket = mutex->ticket.fetch_add(1);
  while (ticket != mutex->serving.load()) {
    // TODO(chogan): @optimization This seems to be necessary if we expect
    // oversubscription. As soon as we have more MPI ranks than logical
    // cores, the performance of the ticket mutex drops dramatically. We
    // lose about 8% when yielding and not oversubscribed, but without
    // yielding, it is unusable when oversubscribed. I want to implement a
    // ticket mutex with a waiting array at some point:
    // https://arxiv.org/pdf/1810.01573.pdf. It looks like that should give
    // us the best of both worlds.
    sched_yield();
  }
}

inline void EndTicketMutex(TicketMutex *mutex) {
  mutex->serving.fetch_add(1);
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

Tier *GetTierFromHeader(SharedMemoryContext *context, BufferHeader *header) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  Tier *tiers_base = (Tier *)(context->shm_base + pool->tier_storage_offset);
  Tier *result = tiers_base + header->tier_id;

  return result;
}

Tier *GetTierById(SharedMemoryContext *context, TierID tier_id) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  Tier *tiers_base = (Tier *)(context->shm_base + pool->tier_storage_offset);
  Tier *result = tiers_base + tier_id;

  return result;
}

BufferHeader *GetHeadersBase(SharedMemoryContext *context) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  BufferHeader *result = (BufferHeader *)(context->shm_base +
                                          pool->header_storage_offset);

  return result;
}

inline BufferHeader *GetHeaderByIndex(SharedMemoryContext *context, u32 index) {
  BufferPool *pool = GetBufferPoolFromContext(context);
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
    header->tier_id = 0;
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

i32 GetSlabUnitSize(SharedMemoryContext *context, TierID tier_id,
                    int slab_index) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  i32 result = 0;
  i32 *slab_unit_sizes = nullptr;

  if (tier_id < pool->num_tiers) {
    slab_unit_sizes = (i32 *)(context->shm_base +
                              pool->slab_unit_sizes_offsets[tier_id]);
    if (slab_index < pool->num_slabs[tier_id]) {
      result = slab_unit_sizes[slab_index];
    } else {
      // TODO(chogan): @logging
    }
  } else {
    // TODO(chogan): @logging
  }

  return result;
}

i32 GetSlabBufferSize(SharedMemoryContext *context, TierID tier_id,
                       int slab_index) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  i32 *slab_sizes = nullptr;
  i32 result = 0;

  if (tier_id < pool->num_tiers) {
    slab_sizes = (i32 *)(context->shm_base +
                         pool->slab_buffer_sizes_offsets[tier_id]);
    if (slab_index < pool->num_slabs[tier_id]) {
      result = slab_sizes[slab_index];
    } else {
      // TODO(chogan): @logging
    }
  } else {
    // TODO(chogan): @logging
  }

  return result;
}

BufferID *GetFreeListPtr(SharedMemoryContext *context, TierID tier_id) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  BufferID *result = nullptr;

  if (tier_id < pool->num_tiers) {
    result = (BufferID *)(context->shm_base + pool->free_list_offsets[tier_id]);
  }

  return result;
}

int GetSlabIndexFromHeader(SharedMemoryContext *context, BufferHeader *header) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  TierID tier_id = header->tier_id;
  i32 units = header->capacity / pool->block_sizes[tier_id];
  int result = 0;

  for (int i = 0; i < pool->num_slabs[tier_id]; ++i) {
    if (GetSlabUnitSize(context, tier_id, i) == units) {
      result = i;
      break;
    }
  }

  return result;
}

u32 GetNodeId(SharedMemoryContext *context) {
  // TODO(chogan): @implement Where will `CommunicationAPI` come from?
  (void)context;
  assert(!"Not implemented yet");
}

bool BufferIsRemote(SharedMemoryContext *context, BufferID buffer_id) {
  bool result = false;
  u32 my_node_id = GetNodeId(context);

  if (my_node_id == buffer_id.bits.node_id) {
    result = true;
  }
  return result;
}

bool IsNullBufferId(BufferID id) {
  bool result = id.as_int == 0;

  return result;
}

BufferID PeekFirstFreeBufferId(SharedMemoryContext *context, TierID tier_id,
                              int slab_index) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  BufferID result = {};
  BufferID *free_list = GetFreeListPtr(context, tier_id);
  if (slab_index < pool->num_slabs[tier_id]) {
    result = free_list[slab_index];
  }

  return result;
}

void SetFirstFreeBufferId(SharedMemoryContext *context, TierID tier_id,
                          int slab_index, BufferID new_id) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  BufferID *free_list = GetFreeListPtr(context, tier_id);
  if (slab_index < pool->num_slabs[tier_id]) {
    free_list[slab_index] = new_id;
  }
}

void ReleaseBuffer(SharedMemoryContext *context, BufferID buffer_id) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  BufferHeader *header_to_free = GetHeaderByIndex(context,
                                                  buffer_id.bits.header_index);
  if (header_to_free) {
    BeginTicketMutex(&pool->ticket_mutex);
    header_to_free->used = 0;
    header_to_free->in_use = false;
    int slab_index = GetSlabIndexFromHeader(context, header_to_free);
    TierID tier_id = header_to_free->tier_id;
    header_to_free->next_free = PeekFirstFreeBufferId(context, tier_id,
                                                     slab_index);
    SetFirstFreeBufferId(context, tier_id, slab_index, buffer_id);
    EndTicketMutex(&pool->ticket_mutex);
  }
}

void ReleaseBuffers(SharedMemoryContext *context,
                    const std::vector<BufferID> &buffer_ids) {
  for (auto id : buffer_ids) {
    ReleaseBuffer(context, id);
  }
}

BufferID GetFreeBuffer(SharedMemoryContext *context, TierID tier_id,
                       int slab_index) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  BufferID result = {};

  BeginTicketMutex(&pool->ticket_mutex);
  BufferID id = PeekFirstFreeBufferId(context, tier_id, slab_index);
  if (!IsNullBufferId(id)) {
    u32 header_index = id.bits.header_index;
    BufferHeader *header = GetHeaderByIndex(context, header_index);
    header->in_use = true;
    result = header->id;
    SetFirstFreeBufferId(context, tier_id, slab_index, header->next_free);
  }
  EndTicketMutex(&pool->ticket_mutex);

  return result;
}

std::vector<BufferID> GetBuffers(SharedMemoryContext *context,
                                 const TieredSchema &schema) {
  BufferPool *pool = GetBufferPoolFromContext(context);

  bool failed = false;
  std::vector<BufferID> result;
  for (auto &size_and_tier : schema) {
    TierID tier_id = size_and_tier.second;

    size_t size_left = size_and_tier.first;
    std::vector<size_t> num_buffers(pool->num_slabs[tier_id], 0);

    // NOTE(chogan): naive buffer selection algorithm: fill with largest
    // buffers first
    for (int i = pool->num_slabs[tier_id] - 1; i >= 0; --i) {
      size_t buffer_size = GetSlabBufferSize(context, tier_id, i);
      size_t buffers = size_left / buffer_size;
      num_buffers[i] = buffers;
      size_left -= buffers * buffer_size;
    }

    if (size_left > 0) {
      num_buffers[0] += 1;
    }

    // NOTE(chogan): Push the buffers on in "biggest first" order so that a Get
    // will reassemble the buffers into the correct original order
    for (int i = pool->num_slabs[tier_id] - 1; i >= 0; --i) {
      for (size_t j = 0; j < num_buffers[i]; ++j) {
        BufferID id = GetFreeBuffer(context, tier_id, i);
        if (id.as_int) {
          result.push_back(id);
        } else {
          failed = true;
        }
      }
    }
  }

  if (failed) {
    // NOTE(chogan): All or none operation. Must release the acquired buffers if
    // we didn't get all we asked for
    ReleaseBuffers(context, result);
    result.clear();
  }

  return result;
}

size_t GetBlobSize(SharedMemoryContext *context,
                   const std::vector<BufferID> &buffer_ids) {
  size_t result = 0;
  for (const auto &id : buffer_ids) {
    if (false /* TODO(chogan): BufferIsRemote(context, id) */) {
      // TODO(chogan): RPC
    } else {
      BufferHeader *header = GetHeaderByBufferId(context, id);
      result += header->used;
    }
  }

  return result;
}

ptrdiff_t BufferIdToOffset(SharedMemoryContext *context, BufferID id) {
  ptrdiff_t result = 0;
  BufferHeader *header = GetHeaderByIndex(context, id.bits.header_index);

  switch (header->tier_id) {
    case 0: {
      result = header->data_offset;
      break;
    }
    default:
      assert(!"Not implemented yet");
  }

  return result;
}

u8 *GetRamBufferPtr(SharedMemoryContext *context, BufferID buffer_id) {
  ptrdiff_t data_offset = BufferIdToOffset(context, buffer_id);
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
    u8 *first_buffer = PushArray<u8>(arena, block_size);

    for (int block = 0; block < num_blocks_needed - 1; ++block) {
      // NOTE(chogan): @optimization Since we don't have to store these
      // pointers, the only effect of this loop is that the arena will end up
      // with the correct "next free" address. This function isn't really
      // necessary; it's mainly just testing that everything adds up correctly.
      u8 *buffer = PushArray<u8>(arena, block_size);
      // NOTE(chogan): Make sure the buffers are perfectly aligned (no holes or
      // padding is introduced)
      i32 buffer_block_offset = (block + 1) * block_size;
      assert((u8 *)first_buffer == ((u8 *)buffer) - buffer_block_offset);
    }
  }
}

BufferID MakeBufferHeaders(Arena *arena, int buffer_size, u32 start_index,
                           u32 end_index, int node_id, TierID tier_id,
                           ptrdiff_t initial_offset, u8 **header_begin) {
  BufferHeader dummy = {};
  BufferHeader *previous = &dummy;

  for (u32 i = start_index, j = 0; i < end_index; ++i, ++j) {
    BufferHeader *header = PushClearedStruct<BufferHeader>(arena);
    header->id = MakeBufferId(node_id, i);
    header->capacity = buffer_size;
    header->tier_id = tier_id;

    // NOTE(chogan): Stored as offset from base address of shared memory
    header->data_offset = buffer_size * j + initial_offset;

    previous->next_free = header->id;
    previous = header;

    // NOTE(chogan): Store the address of the first header so we can later
    // compute the `header_storage_offset`
    if (i == 0 && header_begin) {
      *header_begin = (u8 *)header;
    }
  }

  return dummy.next_free;
}

size_t RoundUpToMultiple(size_t val, size_t multiple) {
  if (multiple == 0) {
    return val;
  }

  size_t result = val;
  size_t remainder = val % multiple;

  if (remainder != 0) {
    result += multiple - remainder;
  }

  return result;
}

size_t RoundDownToMultiple(size_t val, size_t multiple) {
  if (multiple == 0) {
    return val;
  }

  size_t result = val;
  size_t remainder = val % multiple;
  result -= remainder;

  return result;
}

Tier *InitTiers(Arena *arena, Config *config) {
  // TODO(chogan): @alignment
  Tier *result = PushArray<Tier>(arena, config->num_tiers);

  for (int i = 0; i < config->num_tiers; ++i) {
    Tier *tier = result + i;
    tier->bandwidth_mbps = config->bandwidths[i];
    tier->capacity = config->capacities[i];
    tier->latency_ns = config->latencies[i];
    tier->id = i;
    // TODO(chogan): @configuration Get this from config.
    tier->is_remote = false;
    // TODO(chogan): @configuration Get this from config.
    tier->has_fallocate = true;
    size_t path_length = strlen(config->mount_points[i]);

    if (path_length == 0) {
      tier->is_ram = true;
    } else {
      // TODO(chogan): @errorhandling
      assert(path_length < kMaxPathLength);
      snprintf(tier->mount_point, path_length + 1, "%s",
               config->mount_points[i]);
    }
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

  // TODO(chogan): @configuration Assumes RAM is first Tier
  int this_slab_unit_size = GetSlabUnitSize(context, 0, slab_index);
  int bigger_slab_unit_size = GetSlabUnitSize(context, 0, slab_index + 1);

  // TODO(chogan): Currently just handling the case where the next slab size is
  // perfectly divisible by this slab's size
  if (bigger_slab_unit_size % this_slab_unit_size != 0) {
    // TODO(chogan): @logging
    return;
  }

  int merge_factor = bigger_slab_unit_size / this_slab_unit_size;
  int new_slab_size_in_bytes = bigger_slab_unit_size * pool->block_sizes[0];
  int old_slab_size_in_bytes = this_slab_unit_size * pool->block_sizes[0];

  BeginTicketMutex(&pool->ticket_mutex);
  // TODO(chogan): Assuming first Tier is RAM
  TierID tier_id = 0;
  BufferID id = PeekFirstFreeBufferId(context, tier_id, slab_index);

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
               PeekFirstFreeBufferId(context, tier_id, slab_index).as_int) {
          // NOTE(chogan): It's possible that the buffer we're trying to pop
          // from the free list is not at the beginning of the list. In that
          // case, we have to pop and save all the free buffers before the one
          // we're interested in, and then restore them to the free list later.
          assert(saved_free_list_count < max_saved_entries);
          saved_free_list_entries[saved_free_list_count++] =
            PeekFirstFreeBufferId(context, tier_id, slab_index);

          BufferID first_id = PeekFirstFreeBufferId(context, tier_id,
                                                   slab_index);
          BufferHeader *first_free = GetHeaderByBufferId(context, first_id);
          SetFirstFreeBufferId(context, tier_id, slab_index,
                               first_free->next_free);
        }

        SetFirstFreeBufferId(context, tier_id, slab_index, header->next_free);
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
      header_to_merge->next_free = PeekFirstFreeBufferId(context, tier_id,
                                                         slab_index + 1);
      SetFirstFreeBufferId(context, tier_id, slab_index + 1,
                           header_to_merge->id);

      while (saved_free_list_count > 0) {
        // NOTE(chogan): Restore headers that we popped and saved.
        BufferID saved_id = saved_free_list_entries[--saved_free_list_count];
        BufferHeader *saved_header = GetHeaderByBufferId(context, saved_id);
        saved_header->next_free = PeekFirstFreeBufferId(context, tier_id,
                                                        slab_index);
        SetFirstFreeBufferId(context, tier_id, slab_index, saved_header->id);
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
  assert(this_slab_unit_size % smaller_slab_unit_size == 0);

  int split_factor = this_slab_unit_size / smaller_slab_unit_size;
  int new_slab_size_in_bytes = smaller_slab_unit_size * pool->block_sizes[0];

  // TODO(chogan): @optimization We don't really want to wait for a long queue
  // on the ticket mutex. If we need to split, we want to stop the world and do
  // it immediately.
  BeginTicketMutex(&pool->ticket_mutex);
  // TODO(chogan): Assuming first Tier is RAM
  TierID tier_id = 0;
  BufferID id = PeekFirstFreeBufferId(context, tier_id, slab_index);
  u32 unused_header_index = 0;
  BufferHeader *headers = GetHeadersBase(context);
  BufferHeader *next_unused_header = &headers[unused_header_index];

  while (id.as_int != 0) {
    BufferHeader *header_to_split = GetHeaderByIndex(context,
                                                     id.bits.header_index);
    ptrdiff_t old_data_offset = header_to_split->data_offset;
    SetFirstFreeBufferId(context, tier_id, slab_index,
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
          // NOTE(chogan): Assumes first Tier is RAM
          if (++unused_header_index >= pool->num_headers[0]) {
            unused_header_index = 0;
          }
          next_unused_header = &headers[unused_header_index];
        }
      }

      ResetHeader(next_unused_header);
      next_unused_header->data_offset = old_data_offset;
      next_unused_header->capacity = new_slab_size_in_bytes;

      next_unused_header->next_free = PeekFirstFreeBufferId(context, tier_id,
                                                            slab_index - 1);
      SetFirstFreeBufferId(context, tier_id, slab_index - 1,
                           next_unused_header->id);

      old_data_offset += new_slab_size_in_bytes;
    }
  }
  EndTicketMutex(&pool->ticket_mutex);
}

ptrdiff_t InitBufferPool(u8 *hermes_memory, Arena *buffer_pool_arena,
                         Arena *scratch_arena, i32 node_id, Config *config) {
  ScopedTemporaryMemory scratch(scratch_arena);

  i32 **buffer_counts = PushArray<i32*>(scratch, config->num_tiers);
  i32 **slab_buffer_sizes = PushArray<i32*>(scratch, config->num_tiers);
  i32 *header_counts = PushArray<i32>(scratch, config->num_tiers);

  for (int tier = 0; tier < config->num_tiers; ++tier) {
    slab_buffer_sizes[tier] = PushArray<i32>(scratch, config->num_slabs[tier]);
    buffer_counts[tier] = PushArray<i32>(scratch, config->num_slabs[tier]);

    for (int slab = 0; slab < config->num_slabs[tier]; ++slab) {
      slab_buffer_sizes[tier][slab] = (config->block_sizes[tier] *
                                       config->slab_unit_sizes[tier][slab]);
      f32 slab_percentage = config->desired_slab_percentages[tier][slab];
      size_t bytes_for_slab = (size_t)((f32)config->capacities[tier] *
                                       slab_percentage);
      buffer_counts[tier][slab] = (bytes_for_slab /
                                   slab_buffer_sizes[tier][slab]);
    }
  }

  // NOTE(chogan): One header per RAM block to allow for splitting and merging
  // TODO(chogan): @configuration Assumes first Tier is RAM
  // TODO(chogan): Allow splitting and merging for every Tier
  assert(config->capacities[0] % config->block_sizes[0] == 0);
  header_counts[0] = config->capacities[0] / config->block_sizes[0];

  for (int tier = 1; tier < config->num_tiers; ++tier) {
    header_counts[tier] = 0;
    for (int slab = 0; slab < config->num_slabs[tier]; ++slab) {
      header_counts[tier] += buffer_counts[tier][slab];
    }
  }

  // NOTE(chogan): Anything stored in the buffer_pool_arena (besides buffers)
  // needs to be accounted for here. It would be nice to have a compile time
  // check that makes sure anything we allocate for the buffer pool but outside
  // of it gets accounted for here.

  i32 max_headers_needed = 0;
  size_t free_lists_size = 0;
  size_t slab_metadata_size = 0;
  for (int tier = 0; tier < config->num_tiers; ++tier) {
    max_headers_needed += header_counts[tier];
    free_lists_size += config->num_slabs[tier] * sizeof(BufferID);
    // NOTE(chogan): The '* 2' is because we have an i32 for both slab unit size
    // and slab buffer size
    slab_metadata_size += config->num_slabs[tier] * sizeof(i32) * 2;
  }

  size_t headers_size = max_headers_needed * sizeof(BufferHeader);
  size_t tiers_size = config->num_tiers * sizeof(Tier);
  size_t buffer_pool_size = (sizeof(BufferPool) + free_lists_size +
                             slab_metadata_size);
  size_t required_bytes_for_metadata = (headers_size + buffer_pool_size +
                                        tiers_size);

  // TODO(chogan): @blocker Add some padding to account for possibility of
  // alignment inserting extra bytes.
  // int alignment_padding = 256;
  // int max_padding = sizeof(BufferPool) + sizeof(BufferHeader) + sizeof(Tier);
  // int alignment_bytes_needed = 0;
  // ...
  // assert(alignment_bytes_needed <= alignment_padding);

  size_t required_bytes_for_metadata_rounded =
    RoundUpToMultiple(required_bytes_for_metadata, config->block_sizes[0]);
  i32 num_blocks_reserved_for_metadata = (required_bytes_for_metadata_rounded /
                                          config->block_sizes[0]);
  // NOTE(chogan): Remove some of the smallest RAM buffers to make room for
  // metadata
  buffer_counts[0][0] -= num_blocks_reserved_for_metadata;
  // NOTE(chogan): We need fewer headers because we have fewer buffers now
  header_counts[0] -= num_blocks_reserved_for_metadata;
  u32 total_headers = max_headers_needed - num_blocks_reserved_for_metadata;

  int *num_buffers = PushArray<int>(scratch_arena, config->num_tiers);
  int total_buffers = 0;
  for (int tier = 0; tier < config->num_tiers; ++tier) {
    fprintf(stderr, "Tier %d:\n", tier);
    num_buffers[tier] = 0;
    for (int slab = 0; slab < config->num_slabs[tier]; ++slab) {
      // TODO(chogan): @logging Switch to DLOG
      fprintf(stderr, "    %d-Buffers: %d\n", slab, buffer_counts[tier][slab]);
      num_buffers[tier] += buffer_counts[tier][slab];
    }
    total_buffers += num_buffers[tier];
    fprintf(stderr, "    Num Headers: %d\n", header_counts[tier]);
    fprintf(stderr, "    Num Buffers: %d\n", num_buffers[tier]);
  }
  fprintf(stderr, "Total Buffers: %d\n", total_buffers);

  // Build RAM buffers.

  // TODO(chogan): @configuration Assumes the first Tier is RAM
  for (int slab = 0; slab < config->num_slabs[0]; ++slab) {
    PartitionRamBuffers(buffer_pool_arena, slab_buffer_sizes[0][slab],
                        buffer_counts[0][slab], config->block_sizes[slab]);
  }

  // Build Tiers

  Tier *tiers = InitTiers(buffer_pool_arena, config);

  // Create Free Lists

  BufferID **free_lists = PushArray<BufferID*>(scratch_arena,
                                               config->num_tiers);
  for (int tier = 0; tier < config->num_tiers; ++tier) {
    free_lists[tier] = PushArray<BufferID>(scratch_arena,
                                           config->num_slabs[tier]);
  }

  // Build BufferHeaders

  u32 start = 0;
  u8 *header_begin = 0;
  ptrdiff_t initial_offset = 0;
  // TODO(chogan): @configuration Assumes first Tier is RAM
  for (i32 i = 0; i < config->num_slabs[0]; ++i) {
    u32 end = start + buffer_counts[0][i];
    TierID ram_tier_id = 0;
    free_lists[ram_tier_id][i] =
      MakeBufferHeaders(buffer_pool_arena, slab_buffer_sizes[0][i], start, end,
                        node_id, ram_tier_id, initial_offset, &header_begin);
    start = end;
    initial_offset += buffer_counts[0][i] * slab_buffer_sizes[0][i];
  }

  // NOTE(chogan): Add remaining unused RAM headers
  for (u32 i = num_buffers[0]; i < (u32)header_counts[0]; ++i) {
    BufferHeader *header = PushClearedStruct<BufferHeader>(buffer_pool_arena);
    header->id = MakeBufferId(node_id, i);
    start += 1;
  }

  // NOTE(chogan): Add headers for the other Tiers
  for (int tier = 1; tier < config->num_tiers; ++tier) {
    for (int slab = 0; slab < config->num_slabs[tier]; ++slab) {
      // NOTE(chogan): File buffering scheme is one file per slab per Tier
      u32 end = start + buffer_counts[tier][slab];
      free_lists[tier][slab] = MakeBufferHeaders(buffer_pool_arena,
                                                 slab_buffer_sizes[tier][slab],
                                                 start, end, node_id, tier, 0,
                                                 0);
      start = end;
    }
  }

  // Build BufferPool

  // TODO(chogan): @alignment
  BufferPool *pool = PushClearedStruct<BufferPool>(buffer_pool_arena);
  pool->header_storage_offset = header_begin - hermes_memory;
  pool->tier_storage_offset = (u8 *)tiers - hermes_memory;
  pool->num_tiers = config->num_tiers;
  pool->total_headers = total_headers;

  for (int tier = 0; tier < config->num_tiers; ++tier) {
    pool->block_sizes[tier] = config->block_sizes[tier];
    pool->num_headers[tier] = header_counts[tier];
    pool->num_slabs[tier] = config->num_slabs[tier];
    BufferID *free_list = PushArray<BufferID>(buffer_pool_arena,
                                              config->num_slabs[tier]);
    i32 *slab_unit_sizes = PushArray<i32>(buffer_pool_arena,
                                          config->num_slabs[tier]);
    i32 *slab_buffer_sizes_for_tier = PushArray<i32>(buffer_pool_arena,
                                            config->num_slabs[tier]);
    for (int slab = 0; slab < config->num_slabs[tier]; ++slab) {
      free_list[slab] = free_lists[tier][slab];
      slab_unit_sizes[slab] = config->slab_unit_sizes[tier][slab];
      slab_buffer_sizes_for_tier[slab] = slab_buffer_sizes[tier][slab];
    }
    pool->free_list_offsets[tier] = (u8 *)free_list - hermes_memory;
    pool->slab_unit_sizes_offsets[tier] = (u8 *)slab_unit_sizes - hermes_memory;
    pool->slab_buffer_sizes_offsets[tier] = ((u8 *)slab_buffer_sizes_for_tier -
                                             hermes_memory);
  }

  // NOTE(chogan): The buffer pool offset is stored at the beginning of shared
  // memory so the client processes can read it on initialization
  ptrdiff_t *buffer_pool_offset_location = (ptrdiff_t *)hermes_memory;
  ptrdiff_t buffer_pool_offset = (u8 *)pool - hermes_memory;
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
    size_t total_length = base_name_length + username_length + 1;
    // TODO(chogan): @errorhandling
    assert(total_length < kMaxBufferPoolShmemNameLength);
    snprintf(dest + base_name_length, username_length + 1, "%s", username);
  }
}

void InitFilesForBuffering(SharedMemoryContext *context, bool make_space) {
  BufferPool *pool = GetBufferPoolFromContext(context);
  context->buffering_filenames.resize(pool->num_tiers);

  // TODO(chogan): Check the limit for open files via getrlimit. We might have
  // to do some smarter opening and closing to stay under the limit. Could also
  // increase the soft limit to the hard limit.
  for (int tier_id = 0; tier_id < pool->num_tiers; ++tier_id) {
    Tier *tier = GetTierById(context, tier_id);
    char *mount_point = &tier->mount_point[0];

    if (strlen(mount_point) == 0) {
      // NOTE(chogan): RAM Tier. No need for a file.
      continue;
    }

    bool ends_in_slash = mount_point[strlen(mount_point) - 1] == '/';
    context->buffering_filenames[tier_id].resize(pool->num_slabs[tier_id]);

    for (int slab = 0; slab < pool->num_slabs[tier_id]; ++slab) {
      // TODO(chogan): Where does memory for filenames come from? Probably need
      // persistent memory for each application core.
      context->buffering_filenames[tier_id][slab] =
        std::string(std::string(mount_point) + (ends_in_slash ? "" : "/") +
                    "tier" + std::to_string(tier_id) + "_slab" +
                    std::to_string(slab) + ".hermes");

      const char *buffering_fname =
        context->buffering_filenames[tier_id][slab].c_str();
      FILE *buffering_file = fopen(buffering_fname, "w+");
      if (make_space && tier->has_fallocate) {
        // TODO(chogan): Some Tiers require file initialization on each node,
        // and some are shared (burst buffers) and only require one rank to
        // initialize them

        // TODO(chogan): Use posix_fallocate when it is
        // available
        ftruncate(fileno(buffering_file), tier->capacity);
      }
      context->open_streams[tier_id][slab] = buffering_file;
    }
  }
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
        // the beginning of the shared memory segment.
        ptrdiff_t *buffer_pool_offset_location = (ptrdiff_t *)shm_base;
        result.buffer_pool_offset = *buffer_pool_offset_location;
        result.shm_base = shm_base;
        result.shm_size = shm_stat.st_size;
      } else {
        // TODO(chogan): @logging Error handling
      }
    } else {
      // TODO(chogan): @logging Error handling
    }
  } else {
    // TODO(chogan): @logging Error handling
  }

  return result;
}

void ReleaseSharedMemoryContext(SharedMemoryContext *context) {
  BufferPool *pool = GetBufferPoolFromContext(context);

  for (int tier_id = 0; tier_id < pool->num_tiers; ++tier_id) {
    for (int slab = 0; slab < pool->num_slabs[tier_id]; ++slab) {
      if (context->open_streams[tier_id][slab]) {
        int fclose_result = fclose(context->open_streams[tier_id][slab]);
        if (fclose_result != 0) {
          // TODO(chogan): @errorhandling
        }
      }
    }
  }
  munmap(context->shm_base, context->shm_size);
}

void StartBufferPoolRpcServer(SharedMemoryContext *context, const char *addr,
                              i32 num_rpc_threads) {
  tl::engine buffer_pool_rpc_server(addr, THALLIUM_SERVER_MODE, false,
                                    num_rpc_threads);
  // TODO(chogan): @logging
  // std::cout << "Serving at " << buffer_pool_rpc_server.self()
  //       << " with " << num_rpc_threads << " RPC threads" << std::endl;

  using std::function;
  using std::vector;
  using tl::request;

  function<void(const request&, const TieredSchema&)> rpc_get_buffers =
    [context](const request &req, const TieredSchema &schema) {
      std::vector<BufferID> result = GetBuffers(context, schema);
      req.respond(result);
    };

  function<void(const request&, const vector<BufferID>&)> rpc_release_buffers =
    [context](const request &req, const vector<BufferID> &buffer_ids) {
      (void)req;
      ReleaseBuffers(context, buffer_ids);
    };

  function<void(const request&, int)> rpc_split_buffers =
    [context](const request &req, int slab_index) {
      (void)req;
      SplitRamBufferFreeList(context, slab_index);
    };

  function<void(const request&, int)> rpc_merge_buffers =
    [context](const request &req, int slab_index) {
      (void)req;
      MergeRamBufferFreeList(context, slab_index);
    };

  function<void(const request&)> rpc_finalize =
    [&buffer_pool_rpc_server](const request &req) {
      (void)req;
      buffer_pool_rpc_server.finalize();
    };

  buffer_pool_rpc_server.define("GetBuffers", rpc_get_buffers);
  buffer_pool_rpc_server.define("ReleaseBuffers",
                                rpc_release_buffers).disable_response();
  buffer_pool_rpc_server.define("SplitBuffers",
                                rpc_split_buffers).disable_response();
  buffer_pool_rpc_server.define("MergeBuffers",
                                rpc_merge_buffers).disable_response();
  buffer_pool_rpc_server.define("Finalize", rpc_finalize).disable_response();

  // TODO(chogan): Currently the calling thread waits for finalize because
  // that's the way the tests are set up, but once the RPC server is started
  // from Hermes initialization the calling thread will need to continue
  // executing.
  buffer_pool_rpc_server.wait_for_finalize();
}

// IO clients

void WriteBlobToBuffers(SharedMemoryContext *context, const Blob &blob,
                        const std::vector<BufferID> &buffer_ids) {
  size_t bytes_left_to_write = blob.size;
  u8 *at = blob.data;
  // TODO(chogan): @optimization Handle sequential buffers as one I/O operation
  for (const auto &id : buffer_ids) {
    if (false /* TODO(chogan): BufferIsRemote(context, id) */) {
      // TODO(chogan): RPC. Probably RDMA (Thallium bulk transfer)
    } else {
      BufferHeader *header = GetHeaderByIndex(context, id.bits.header_index);
      Tier *tier = GetTierFromHeader(context, header);
      size_t write_size = header->capacity;

      if (bytes_left_to_write > header->capacity) {
        header->used = header->capacity;
        bytes_left_to_write -= header->capacity;
      } else {
        header->used = bytes_left_to_write;
        write_size = bytes_left_to_write;
      }

      // TODO(chogan): Should this be a TicketMutex? It seems that at any
      // given time, only the DataOrganizer and an application core will
      // be trying to write to/from the same BufferID. In that case, it's
      // first come first serve. However, if it turns out that more
      // threads will be trying to lock the buffer, we may need to enforce
      // ordering.
      LockBuffer(header);

      if (tier->is_ram) {
        u8 *dest = GetRamBufferPtr(context, header->id);
        memcpy(dest, at, write_size);
      } else {
        int slab_index = GetSlabIndexFromHeader(context, header);
        FILE *file = context->open_streams[tier->id][slab_index];
        if (!file) {
          // TODO(chogan): Check number of opened files against maximum allowed.
          // May have to close something.
          const char *filename =
            context->buffering_filenames[tier->id][slab_index].c_str();
          file = fopen(filename, "r+");
          context->open_streams[tier->id][slab_index] = file;
        }
        fseek(file, header->data_offset, SEEK_SET);
        size_t items_written = fwrite(at, write_size, 1, file);
        // TODO(chogan): @errorhandling
        assert(items_written == 1);
        // fflush(file);
        // fsync(fileno(file));
      }
      at += write_size;
      UnlockBuffer(header);
    }
  }
  assert(at == blob.data + blob.size);
}

size_t ReadBlobFromBuffers(SharedMemoryContext *context, Blob *blob,
                           const std::vector<BufferID> &buffer_ids) {
  u8 *at = blob->data;
  size_t total_bytes_read = 0;
  for (const auto &id : buffer_ids) {
    if (false /* TODO(chogan): BufferIsRemote(context, id) */) {
      // TODO(chogan): RPC
    } else {
      BufferHeader *header = GetHeaderByIndex(context, id.bits.header_index);
      Tier *tier = GetTierFromHeader(context, header);
      size_t read_size = header->used;

      // TODO(chogan): Should this be a TicketMutex? It seems that at any
      // given time, only the DataOrganizer and an application core will
      // be trying to write to/from the same BufferID. In that case, it's
      // first come first serve. However, if it turns out that more
      // threads will be trying to lock the buffer, we may need to enforce
      // ordering.
      LockBuffer(header);

      if (tier->is_ram) {
        u8 *src = GetRamBufferPtr(context, header->id);
        memcpy(blob->data, src, read_size);
      } else {
        int slab_index = GetSlabIndexFromHeader(context, header);
        FILE *file = context->open_streams[tier->id][slab_index];
        if (!file) {
          // TODO(chogan): Check number of opened files against maximum allowed.
          // May have to close something.
          const char *filename =
            context->buffering_filenames[tier->id][slab_index].c_str();
          fopen(filename, "r+");
        }
        fseek(file, header->data_offset, SEEK_SET);
        size_t items_read = fread(at, read_size, 1, file);
        // TODO(chogan): @errorhandling
        assert(items_read == 1);
        at += items_read * read_size;
        total_bytes_read += items_read * read_size;
      }
      UnlockBuffer(header);
    }
  }
  assert(total_bytes_read == blob->size);

  return total_bytes_read;
}

}  // namespace hermes
