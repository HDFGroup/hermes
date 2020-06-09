#include "communication.h"
#include "memory_management.h"

/**
 * @file communication_mpi.cc
 *
 * An implementation of the Hermes communication interface in MPI.
 */

namespace hermes {

struct MPIState {
  MPI_Comm world_comm;
  /** This communicator is in one of two groups, depending on the value of the
   * rank's ProcessKind. When its kHermes, sub_comm groups all Hermes cores, and
   * when its kApp, it groups all application cores. */
  MPI_Comm sub_comm;
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

inline int MpiGetSubProcId(void *state) {
  MPIState *mpi_state = (MPIState *)state;
  int result = MpiGetProcId(mpi_state->sub_comm);

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

inline void MpiBarrier(MPI_Comm comm) {
  MPI_Barrier(comm);
}

inline void MpiWorldBarrier(void *state) {
  MPIState *mpi_state = (MPIState *)state;
  MpiBarrier(mpi_state->world_comm);
}

inline void MpiSubBarrier(void *state) {
  MPIState *mpi_state = (MPIState *)state;
  MpiBarrier(mpi_state->sub_comm);
}

/**
 * Returns true if the calling rank is the lowest numbered rank on its node in
 * communicator @p comm.
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

  Arena scratch_arena = InitArenaAndAllocate(scratch_size);

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

size_t InitCommunication(CommunicationContext *comm, Arena *arena,
                         size_t trans_arena_size_per_node, bool is_daemon) {
  comm->world_barrier = MpiWorldBarrier;
  comm->sub_barrier = MpiSubBarrier;
  comm->finalize = MpiFinalize;

  MPIState *mpi_state = PushClearedStruct<MPIState>(arena);
  mpi_state->world_comm = MPI_COMM_WORLD;
  comm->state = mpi_state;
  comm->world_proc_id = MpiGetWorldProcId(comm->state);
  comm->world_size = MpiGetNumWorldProcs(comm->state);

  size_t trans_arena_size_for_rank =
    MpiAssignIDsToNodes(comm, trans_arena_size_per_node);

  if (is_daemon) {
    mpi_state->sub_comm = mpi_state->world_comm;
    comm->sub_proc_id = comm->world_proc_id;
    comm->hermes_size = comm->world_size;
    comm->proc_kind = ProcessKind::kHermes;
    comm->first_on_node = MpiFirstOnNode(mpi_state->world_comm);
  } else {
    int color = (int)comm->proc_kind;
    MPI_Comm_split(mpi_state->world_comm, color, comm->world_proc_id,
                   &mpi_state->sub_comm);

    comm->first_on_node = MpiFirstOnNode(mpi_state->sub_comm);
    comm->sub_proc_id = MpiGetSubProcId(comm->state);

    switch (comm->proc_kind) {
      case ProcessKind::kHermes: {
        MPI_Comm_size(mpi_state->sub_comm, &comm->hermes_size);
        break;
      }
      case ProcessKind::kApp: {
        MPI_Comm_size(mpi_state->sub_comm, &comm->app_size);
        break;
      }
      default: {
        assert(!"Invalid code path\n");
        break;
      }
    }
  }

  return trans_arena_size_for_rank;
}

}  // namespace hermes
