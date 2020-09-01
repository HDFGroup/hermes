#ifndef HERMES_DEBUG_STATE_H_
#define HERMES_DEBUG_STATE_H_

namespace hermes {

struct DebugHeapAllocation {
  u32 offset;
  u32 size;
};

const int kGlobalDebugMaxAllocations = 256;

struct DebugState {
  u8 *shmem_base;
  DebugHeapAllocation allocations[kGlobalDebugMaxAllocations];
  TicketMutex mutex;
  u32 allocation_count;
};

DebugState *global_debug_id_state;
DebugState *global_debug_map_state;
char global_debug_map_name[] = "/hermes_debug_map_heap";
char global_debug_id_name[] = "/hermes_debug_id_heap";

}  // namespace hermes
#endif  // HERMES_DEBUG_STATE_H_
