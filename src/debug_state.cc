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

#if HERMES_DEBUG_HEAP

#include "debug_state.h"

namespace hermes {

DebugState *InitDebugState(const char *shmem_name) {
  u8 *base = InitSharedMemory(shmem_name, sizeof(DebugState));
  DebugState *result = (DebugState *)base;

  return result;
}

void CloseDebugState(bool unlink) {
  munmap(global_debug_map_name, sizeof(DebugState));
  munmap(global_debug_id_name, sizeof(DebugState));
  if (unlink) {
    shm_unlink(global_debug_map_name);
    shm_unlink(global_debug_id_name);
  }
}

void AddDebugAllocation(DebugState *state, u32 offset, u32 size) {
  BeginTicketMutex(&state->mutex);
  assert(state->allocation_count < kGlobalDebugMaxAllocations);
  int i = state->allocation_count++;
  state->allocations[i].offset = offset;
  state->allocations[i].size = size;
  EndTicketMutex(&state->mutex);
}

void RemoveDebugAllocation(DebugState *state, u32 offset, u32 size) {
  BeginTicketMutex(&state->mutex);
  for (u32 i = 0; i < state->allocation_count; ++i) {
    if (state->allocations[i].offset == offset &&
        state->allocations[i].size == size) {
      state->allocations[i] = state->allocations[--state->allocation_count];
      break;
    }
  }
  EndTicketMutex(&state->mutex);
}

#define HERMES_DEBUG_SERVER_INIT(grows_up)                          \
  if (grows_up) {                                                   \
    global_debug_map_state = InitDebugState(global_debug_map_name); \
  } else {                                                          \
    global_debug_id_state = InitDebugState(global_debug_id_name);   \
  }

#define HERMES_DEBUG_CLIENT_INIT()                                      \
  if (!global_debug_id_state) {                                         \
    SharedMemoryContext context =                                       \
      GetSharedMemoryContext(global_debug_id_name);                     \
    global_debug_id_state = (DebugState *)context.shm_base;             \
  }                                                                     \
  if (!global_debug_map_state) {                                        \
    SharedMemoryContext context =                                       \
      GetSharedMemoryContext(global_debug_map_name);                    \
    global_debug_map_state = (DebugState *)context.shm_base;            \
  }

#define HERMES_DEBUG_SERVER_CLOSE() CloseDebugState(true)
#define HERMES_DEBUG_CLIENT_CLOSE() CloseDebugState(false)

#define HERMES_DEBUG_TRACK_ALLOCATION(ptr, size, map)                   \
  if ((map)) {                                                          \
    AddDebugAllocation(global_debug_map_state,                          \
                       GetHeapOffset(heap, (u8 *)(ptr)), (size));       \
  } else {                                                              \
    AddDebugAllocation(global_debug_id_state,                           \
                       GetHeapOffset(heap, (u8 *)(ptr)), (size));       \
  }

#define HERMES_DEBUG_TRACK_FREE(ptr, size, map)                       \
  if ((map)) {                                                        \
    RemoveDebugAllocation(global_debug_map_state,                     \
                          GetHeapOffset(heap, (u8 *)(ptr)), (size));  \
  } else {                                                            \
    RemoveDebugAllocation(global_debug_id_state,                      \
                          GetHeapOffset(heap, (u8 *)(ptr)), (size));  \
  }

}  // namespace hermes

#else

#define HERMES_DEBUG_SERVER_INIT(grows_up)
#define HERMES_DEBUG_CLIENT_INIT()
#define HERMES_DEBUG_SERVER_CLOSE()
#define HERMES_DEBUG_CLIENT_CLOSE()
#define HERMES_DEBUG_TRACK_ALLOCATION(ptr, size, map)
#define HERMES_DEBUG_TRACK_FREE(ptr, size, map)

#endif  // HERMES_DEBUG_HEAP

