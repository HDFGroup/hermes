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

inline int MpiGetWorldProcId(void *state) {
  MPIState *mpi_state = (MPIState *)state;
  int result = MpiGetProcId(mpi_state->world_comm);

  return result;
}

inline int MpiGetHermesProcId(void *state) {
  MPIState *mpi_state = (MPIState *)state;
  int result = MpiGetProcId(mpi_state->hermes_comm);

  return result;
}

inline int MpiGetAppProcId(void *state) {
  MPIState *mpi_state = (MPIState *)state;
  int result = MpiGetProcId(mpi_state->app_comm);

  return result;
}

inline int MpiGetNumProcs(MPI_Comm comm) {
  int result;
  MPI_Comm_size(comm, &result);

  return result;
}

inline int MpiGetNumWorldProcs(void *state) {
  MPIState *mpi_state = (MPIState *)state;
  int result = MpiGetNumProcs(mpi_state->world_comm);

  return result;
}

inline int MpiGetNumHermesProcs(void *state) {
  MPIState *mpi_state = (MPIState *)state;
  int result = MpiGetNumProcs(mpi_state->hermes_comm);

  return result;
}

inline int MpiGetNumAppProcs(void *state) {
  MPIState *mpi_state = (MPIState *)state;
  int result = MpiGetNumProcs(mpi_state->app_comm);

  return result;
}

inline void MpiBarrier(MPI_Comm comm) {
  MPI_Barrier(comm);
}

inline void MpiWorldBarrier(void *state) {
  MPIState *mpi_state = (MPIState *)state;
  MpiBarrier(mpi_state->world_comm);
}

inline void MpiHermesBarrier(void *state) {
  MPIState *mpi_state = (MPIState *)state;
  MpiBarrier(mpi_state->hermes_comm);
}

inline void MpiAppBarrier(void *state) {
  MPIState *mpi_state = (MPIState *)state;
  MpiBarrier(mpi_state->app_comm);
}

/**
 * Returns true if the calling rank is the lowest numbered rank on its node.
 */
bool MpiFirstOnNode(MPI_Comm comm) {
  int rank = MpiGetProcId(comm);
  int size = MpiGetNumProcs(comm);

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

  MPI_Allgather(MPI_IN_PLACE, 0, 0, node_names[0], MPI_MAX_PROCESSOR_NAME,
                MPI_CHAR, comm);

  int first_on_node = 0;
  while (strncmp(node_names[rank], node_names[first_on_node],
                 MPI_MAX_PROCESSOR_NAME) != 0) {
    first_on_node++;
  }

  DestroyArena(&scratch_arena);

  bool result = false;

  if (first_on_node == rank) {
    result = true;
  }

  return result;
}

/**
 * Assigns each node an ID and returns the size in bytes of the transient arena
 * for the calling rank.
 */
size_t MpiAssignIDsToNodes(CommunicationContext *comm,
                           size_t trans_arena_size_per_node) {
  int rank = comm->world_proc_id;
  int size = comm->world_size;

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

  MPIState *mpi_state = (MPIState *)comm->state;
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
    comm->proc_kind = ProcessKind::kHermes;
  } else {
    comm->proc_kind = ProcessKind::kApp;
  }

  // NOTE(chogan): Create a set of unique node names
  std::set<std::string> names;
  for (int i = 0; i < size; ++i) {
    std::string name(node_names[i], length);
    names.insert(name);
  }

  comm->num_nodes = names.size();

  std::vector<int> ranks_per_node_local(comm->num_nodes, 0);

  // NOTE(chogan): 1-based index so 0 can be the NULL BufferID
  int index = 1;
  for (auto &name : names) {
    if (name == node_names[rank]) {
      comm->node_id = index;
      ranks_per_node_local[index - 1]++;
      break;
    }
    ++index;
  }

  // NOTE(chogan): We're returning the transient arena size in bytes for the
  // calling rank. To calculate that, we need to divide the total transient
  // arena size for each node by the number of ranks on that node.
  std::vector<int> ranks_per_node(comm->num_nodes);
  MPI_Allreduce(ranks_per_node_local.data(), ranks_per_node.data(),
                comm->num_nodes, MPI_INT, MPI_SUM, mpi_state->world_comm);

  size_t result = trans_arena_size_per_node / (size_t)ranks_per_node[comm->node_id - 1];

  DestroyArena(&scratch_arena);

  return result;
}

void MpiFinalize(void *state) {
  (void)state;
  MPI_Finalize();
}

void MpiCopyState(void *src, void *dest) {
  MPIState *src_state = (MPIState *)src;
  MPIState *dest_state = (MPIState *)dest;
  *dest_state = *src_state;
}

void MpiSyncCommState(void *hermes_state, void *app_state) {
  MPIState *hermes_mpi = (MPIState *)hermes_state;
  MPIState *app_mpi = (MPIState *)app_state;
  hermes_mpi->app_comm = app_mpi->app_comm;
}

size_t InitCommunication(CommunicationContext *comm, Arena *arena,
                         size_t trans_arena_size_per_node, bool is_daemon) {
  comm->get_world_proc_id = MpiGetWorldProcId;
  comm->get_hermes_proc_id = MpiGetHermesProcId;
  comm->get_app_proc_id = MpiGetAppProcId;
  comm->get_num_world_procs = MpiGetNumWorldProcs;
  comm->get_num_hermes_procs = MpiGetNumHermesProcs;
  comm->get_num_app_procs = MpiGetNumAppProcs;
  comm->world_barrier = MpiWorldBarrier;
  comm->hermes_barrier = MpiHermesBarrier;
  comm->app_barrier = MpiAppBarrier;
  comm->finalize = MpiFinalize;
  comm->copy_state = MpiCopyState;
  comm->sync_comm_state = MpiSyncCommState;

  MPIState *mpi_state = PushClearedStruct<MPIState>(arena);
  mpi_state->world_comm = MPI_COMM_WORLD;
  comm->state = mpi_state;
  comm->world_proc_id = comm->get_world_proc_id(comm->state);
  comm->world_size = comm->get_num_world_procs(comm->state);

  size_t trans_arena_size_for_rank =
    MpiAssignIDsToNodes(comm, trans_arena_size_per_node);

  if (is_daemon) {
    comm->first_on_node = MpiFirstOnNode(mpi_state->world_comm);
    comm->hermes_proc_id = comm->world_proc_id;
    comm->hermes_size = comm->world_size;
    comm->proc_kind = ProcessKind::kHermes;
    mpi_state->hermes_comm = mpi_state->world_comm;
  } else {
    if (comm->proc_kind == ProcessKind::kHermes) {
      MPI_Comm_split(mpi_state->world_comm, (int)ProcessKind::kHermes,
                     comm->world_proc_id, &mpi_state->hermes_comm);
      comm->hermes_proc_id = comm->get_hermes_proc_id(comm->state);
      comm->hermes_size = comm->get_num_hermes_procs(comm->state);
      comm->first_on_node = MpiFirstOnNode(mpi_state->hermes_comm);
    } else {
      MPI_Comm_split(mpi_state->world_comm, (int)ProcessKind::kApp,
                     comm->world_proc_id, &mpi_state->app_comm);
      comm->app_proc_id = comm->get_app_proc_id(comm->state);
      comm->app_size = comm->get_num_app_procs(comm->state);
      comm->first_on_node = MpiFirstOnNode(mpi_state->app_comm);
    }
  }

  return trans_arena_size_for_rank;
}

}  // namespace hermes
