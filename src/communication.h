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

struct SharedMemoryContext;

struct CommunicationState {
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
};

typedef int (*RankFunc)(CommunicationState*);
typedef int (*SizeFunc)(CommunicationState*);
typedef void (*NodeFunc)(CommunicationState*, Arena*);
typedef void (*BarrierFunc)(CommunicationState*);
typedef void (*FinalizeFunc)(CommunicationState*);

struct CommunicationAPI {
  RankFunc get_hermes_proc_id;
  RankFunc get_world_proc_id;
  RankFunc get_app_proc_id;
  SizeFunc get_num_hermes_procs;
  SizeFunc get_num_world_procs;
  SizeFunc get_num_app_procs;
  BarrierFunc world_barrier;
  BarrierFunc hermes_barrier;
  BarrierFunc app_barrier;
  NodeFunc get_node_info;
  FinalizeFunc finalize;
};

void InitCommunication(Arena *arena, SharedMemoryContext *context,
                       bool init_mpi);

}  // namespace hermes

#endif  // HERMES_COMMUNICATION_H_
