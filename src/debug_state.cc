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
  assert(state->allocation_count < global_debug_max_allocations);
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

#define DEBUG_SERVER_INIT(grows_up)                                 \
  if (grows_up) {                                                   \
    global_debug_map_state = InitDebugState(global_debug_map_name); \
  } else {                                                          \
    global_debug_id_state = InitDebugState(global_debug_id_name);   \
  }

#define DEBUG_CLIENT_INIT()                                             \
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

#define DEBUG_SERVER_CLOSE() CloseDebugState(true)
#define DEBUG_CLIENT_CLOSE() CloseDebugState(false)

#define DEBUG_TRACK_ALLOCATION(ptr, size, map)                          \
  if ((map)) {                                                          \
    AddDebugAllocation(global_debug_map_state,                          \
                       GetHeapOffset(heap, (u8 *)(ptr)), (size));       \
  } else {                                                              \
    AddDebugAllocation(global_debug_id_state,                           \
                       GetHeapOffset(heap, (u8 *)(ptr)), (size));       \
  }

#define DEBUG_TRACK_FREE(ptr, size, map)                              \
  if ((map)) {                                                        \
    RemoveDebugAllocation(global_debug_map_state,                     \
                          GetHeapOffset(heap, (u8 *)(ptr)), (size));  \
  } else {                                                            \
    RemoveDebugAllocation(global_debug_id_state,                      \
                          GetHeapOffset(heap, (u8 *)(ptr)), (size));  \
  }

}  // namespace hermes

#else

#define DEBUG_SERVER_INIT(grows_up)
#define DEBUG_CLIENT_INIT()
#define DEBUG_SERVER_CLOSE()
#define DEBUG_CLIENT_CLOSE()
#define DEBUG_TRACK_ALLOCATION(ptr, size, map)
#define DEBUG_TRACK_FREE(ptr, size, map)

#endif  // HERMES_DEBUG_HEAP

