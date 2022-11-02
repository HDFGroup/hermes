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

#ifndef HERMES_DEBUG_STATE_H_
#define HERMES_DEBUG_STATE_H_

namespace hermes {

/**
 A structure to represent debug heap allocation
*/
struct DebugHeapAllocation {
  u32 offset; /**< heap offset */
  u32 size;   /**< heap size */
};

/** global debug maxium allocations */
const int kGlobalDebugMaxAllocations = KILOBYTES(64);

/**
 A structure to represent debug state
*/
struct DebugState {
  u8 *shmem_base; /**< shared memory base address pointer */
  /** 64KB debug heap allocations */
  DebugHeapAllocation allocations[kGlobalDebugMaxAllocations];
  TicketMutex mutex;    /**< ticket-based mutex */
  u32 allocation_count; /**< counter for allocation */
};

DebugState *global_debug_id_state;  /**< global debug id state */
DebugState *global_debug_map_state; /**< global debug map state */
/** name for global debug map heap */
char global_debug_map_name[] = "/hermes_debug_map_heap";
/** name for global debug id heap */
char global_debug_id_name[] = "/hermes_debug_id_heap";

}  // namespace hermes
#endif  // HERMES_DEBUG_STATE_H_
