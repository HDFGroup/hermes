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

#ifndef HERMES_COMMUNICATION_H_
#define HERMES_COMMUNICATION_H_

#include "hermes_types.h"
#include "memory_management.h"

/**
 * @file communication.h
 *
 * A generic communication interface for Hermes that can be implemented by
 * multiple backends. See communication_mpi.cc for an example of how to
 * implement a communication backend.
 */

namespace hermes {

typedef void (*BarrierFunc)(void *);
typedef void (*FinalizeFunc)(void *);

struct CommunicationContext {
  BarrierFunc world_barrier;
  BarrierFunc sub_barrier;
  FinalizeFunc finalize;

  /** Details relative to the backing communciation implementation. */
  void *state;
  /** A unique identifier for each rank, relative to all ranks. */
  i32 world_proc_id;
  /** a unique identifier for each rank, releative to each ProcessKind. */
  i32 sub_proc_id;
  /** The total number of ranks. */
  i32 world_size;
  /** The total number of Hermes cores. Currently this is only defined on ranks
   * that have ProcessKind::kHermes */
  i32 hermes_size;
  /** The total number of application cores. Currently this is only defined on
   * ranks that have ProcessKind::kApp */
  i32 app_size;
  /** The total number of nodes. */
  i32 num_nodes;
  /** A unique index for each node. Starts at 1, not 0. */
  i32 node_id;
  /** Distinguishes between Hermes ranks and application ranks. */
  ProcessKind proc_kind;
  /** True if this rank is the lowest numbered rank on the current node. Lowest
   * is not relative to all ranks, but to each ProcessKind. This is useful for
   * operations that only need to happen once per node. */
  bool first_on_node;
};

size_t InitCommunication(CommunicationContext *comm, Arena *arena,
                         size_t trans_arena_size_per_node,
                         bool is_daemon = false, bool is_adapter = false);

inline void WorldBarrier(CommunicationContext *comm) {
  comm->world_barrier(comm->state);
}

inline void SubBarrier(CommunicationContext *comm) {
  comm->sub_barrier(comm->state);
}

void *GetAppCommunicator(CommunicationContext *comm);

}  // namespace hermes

#endif  // HERMES_COMMUNICATION_H_
