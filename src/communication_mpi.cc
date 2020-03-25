#include "communication.h"
#include "memory_arena.h"

/**
 * @file communication_mpi.cc
 *
 * An implementation of the Hermes communication interface in MPI.
 */

namespace hermes {

struct MPIState {
  MPI_Comm world_comm;
  MPI_Comm hermes_comm;
  MPI_Comm app_comm;
};

inline int MpiGetProcId(MPI_Comm comm) {
  int result;
  MPI_Comm_rank(comm, &result);

  return result;
}

inline int MpiGetWorldProcId(CommunicationState *comm_state) {
  MPIState *mpi_state = (MPIState *)comm_state->state;
  int result = MpiGetProcId(mpi_state->world_comm);

  return result;
}

inline int MpiGetHermesProcId(CommunicationState *comm_state) {
  MPIState *mpi_state = (MPIState *)comm_state->state;
  int result = MpiGetProcId(mpi_state->hermes_comm);

  return result;
}

inline int MpiGetAppProcId(CommunicationState *comm_state) {
  MPIState *mpi_state = (MPIState *)comm_state->state;
  int result = MpiGetProcId(mpi_state->app_comm);

  return result;
}

inline int MpiGetNumProcs(MPI_Comm comm) {
  int result;
  MPI_Comm_size(comm, &result);

  return result;
}

inline int MpiGetNumWorldProcs(CommunicationState *comm_state) {
  MPIState *mpi_state = (MPIState *)comm_state->state;
  int result = MpiGetNumProcs(mpi_state->world_comm);

  return result;
}

inline int MpiGetNumHermesProcs(CommunicationState *comm_state) {
  MPIState *mpi_state = (MPIState *)comm_state->state;
  int result = MpiGetNumProcs(mpi_state->hermes_comm);

  return result;
}

inline int MpiGetNumAppProcs(CommunicationState *comm_state) {
  MPIState *mpi_state = (MPIState *)comm_state->state;
  int result = MpiGetNumProcs(mpi_state->app_comm);

  return result;
}

inline void MpiBarrier(MPI_Comm comm) {
  MPI_Barrier(comm);
}

inline void MpiWorldBarrier(CommunicationState *comm_state) {
  MPIState *mpi_state = (MPIState *)comm_state->state;
  MpiBarrier(mpi_state->world_comm);
}

inline void MpiHermesBarrier(CommunicationState *comm_state) {
  MPIState *mpi_state = (MPIState *)comm_state->state;
  MpiBarrier(mpi_state->hermes_comm);
}

inline void MpiAppBarrier(CommunicationState *comm_state) {
  MPIState *mpi_state = (MPIState *)comm_state->state;
  MpiBarrier(mpi_state->app_comm);
}

void MpiAssignIDsToNodes(CommunicationState *comm_state) {
  int rank = comm_state->world_proc_id;
  int size = comm_state->world_size;

  size_t scratch_size = (size * sizeof(char *) +
                         size * sizeof(char) * MPI_MAX_PROCESSOR_NAME);
  u8 *scratch_memory = (u8 *)malloc(scratch_size);
  Arena scratch_arena = {};
  InitArena(&scratch_arena, scratch_size, scratch_memory);

  char **node_names = PushArray<char *>(&scratch_arena, size);
  for (int i = 0; i < size; ++i) {
    node_names[i] = PushArray<char>(&scratch_arena, MPI_MAX_PROCESSOR_NAME);
  }

  int length;
  MPI_Get_processor_name(node_names[rank], &length);

  MPIState *mpi_state = (MPIState *)comm_state->state;
  MPI_Allgather(MPI_IN_PLACE, 0, 0, node_names[0], MPI_MAX_PROCESSOR_NAME,
                MPI_CHAR, mpi_state->world_comm);

  // NOTE(chogan): Assign the first rank on each node to be the process that
  // runs the Hermes core. The other ranks become application cores.
  int first_on_node = 0;
  while (strncmp(node_names[rank], node_names[first_on_node],
                 MPI_MAX_PROCESSOR_NAME) != 0) {
    first_on_node++;
  }

  if (first_on_node == rank) {
    comm_state->proc_kind = ProcessKind::kHermes;
  } else {
    comm_state->proc_kind = ProcessKind::kApp;
  }

  // NOTE(chogan): Create a set of unique node names
  std::set<std::string> names;
  for (int i = 0; i < size; ++i) {
    std::string name(node_names[i], length);
    names.insert(name);
  }

  comm_state->num_nodes = names.size();

  // NOTE(chogan): 1-based index so 0 can be the NULL BufferID
  int index = 1;
  for (auto &name : names) {
    if (name == node_names[rank]) {
      comm_state->node_id = index;
      break;
    }
    ++index;
  }

  // DestroyArena
  free(scratch_arena.base);
}

void MpiFinalize(CommunicationState *comm_state) {
  (void)comm_state;
  MPI_Finalize();
}

void InitCommunication(Arena *arena, SharedMemoryContext *context,
                       bool init_mpi) {
  // TODO(chogan): MPI_THREAD_MULTIPLE
  if (init_mpi) {
    MPI_Init(0, 0);  // &argc, &argv
  }

  context->comm_api.get_world_proc_id = MpiGetWorldProcId;
  context->comm_api.get_hermes_proc_id = MpiGetHermesProcId;
  context->comm_api.get_app_proc_id = MpiGetAppProcId;
  context->comm_api.get_num_world_procs = MpiGetNumWorldProcs;
  context->comm_api.get_num_hermes_procs = MpiGetNumHermesProcs;
  context->comm_api.get_num_app_procs = MpiGetNumAppProcs;
  context->comm_api.world_barrier = MpiWorldBarrier;
  context->comm_api.hermes_barrier = MpiHermesBarrier;
  context->comm_api.app_barrier = MpiAppBarrier;
  context->comm_api.finalize = MpiFinalize;

  // TEMP(chogan):
  MPIState *mpi_state = (MPIState *)malloc(sizeof(MPIState));
  // MPIState *mpi_state = PushStruct<MPIState>(arena);
  mpi_state->world_comm = MPI_COMM_WORLD;
  context->comm_state.state = mpi_state;
  context->comm_state.world_proc_id = MpiGetWorldProcId(&context->comm_state);
  context->comm_state.world_size = MpiGetNumWorldProcs(&context->comm_state);

  MpiAssignIDsToNodes(&context->comm_state);

  if (context->comm_state.proc_kind == ProcessKind::kHermes) {
    MPI_Comm_split(mpi_state->world_comm, (int)ProcessKind::kHermes,
                   context->comm_state.world_proc_id, &mpi_state->hermes_comm);
    context->comm_state.hermes_proc_id = MpiGetHermesProcId(&context->comm_state);
    context->comm_state.hermes_size = MpiGetNumHermesProcs(&context->comm_state);
  } else {
    MPI_Comm_split(mpi_state->world_comm, (int)ProcessKind::kApp,
                   context->comm_state.world_proc_id, &mpi_state->app_comm);
    context->comm_state.app_proc_id = MpiGetAppProcId(&context->comm_state);
    context->comm_state.app_size = MpiGetNumAppProcs(&context->comm_state);
  }
}

}  // namespace hermes
