#ifndef HERMES_COMMUNICATION_H_
#define HERMES_COMMUNICATION_H_

#include "hermes_types.h"
#include "memory_arena.h"

/**
 * @file communication.h
 *
 * A generic communication interface for Hermes that can be implemented by
 * multiple backends. See communication_mpi.cc for an example of how to
 * implement a communication backend.
 */

namespace hermes {

typedef int (*RankFunc)(void *);
typedef int (*SizeFunc)(void *);
typedef void (*BarrierFunc)(void *);
typedef void (*FinalizeFunc)(void *);
typedef void (*CopyStateFunc)(void *src, void *dest);

struct CommunicationContext {
  RankFunc get_hermes_proc_id;
  RankFunc get_world_proc_id;
  RankFunc get_app_proc_id;
  SizeFunc get_num_hermes_procs;
  SizeFunc get_num_world_procs;
  SizeFunc get_num_app_procs;
  BarrierFunc world_barrier;
  BarrierFunc hermes_barrier;
  BarrierFunc app_barrier;
  FinalizeFunc finalize;
  CopyStateFunc copy_state;
  CopyStateFunc adjust_shared_metadata;

  void *state;
  i32 world_proc_id;
  i32 hermes_proc_id;
  i32 app_proc_id;
  i32 world_size;
  i32 hermes_size;
  i32 app_size;
  i32 num_nodes;
  // NOTE(chogan): 1-based index
  i32 node_id;
  ProcessKind proc_kind;
  bool first_on_node;
};

size_t InitCommunication(CommunicationContext *comm, Arena *arena,
                         size_t trans_arena_size_per_node,
                         bool is_daemon=false);

void WorldBarrier(CommunicationContext *comm);
void HermesBarrier(CommunicationContext *comm);
void AppBarrier(CommunicationContext *comm);

}  // namespace hermes

#endif  // HERMES_COMMUNICATION_H_
