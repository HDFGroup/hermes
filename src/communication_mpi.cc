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

inline MPI_Comm MpiSplitCommunicator(MPI_Comm comm, int color, int rank) {
  MPI_Comm result = {};
  MPI_Comm_split(comm, color, rank, &result);

  return result;
}

void MpiGetNodeInfo(CommunicationState *comm_state, Arena *scratch_arena) {
  ScopedTemporaryMemory scratch(scratch_arena);

  int rank = comm_state->hermes_proc_id;
  int size = comm_state->hermes_size;

  char **node_names = PushArray<char *>(scratch, size);
  for (int i = 0; i < size; ++i) {
    node_names[i] = PushArray<char>(scratch, MPI_MAX_PROCESSOR_NAME);
  }

  int length;
  MPI_Get_processor_name(node_names[rank], &length);

  MPIState *mpi_state = (MPIState *)comm_state->state;
  MPI_Allgather(MPI_IN_PLACE, 0, 0, node_names[0], MPI_MAX_PROCESSOR_NAME,
                MPI_CHAR, mpi_state->hermes_comm);

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

  context->comm_api.get_node_info = MpiGetNodeInfo;
  context->comm_api.get_world_proc_id = MpiGetWorldProcId;
  context->comm_api.get_hermes_proc_id = MpiGetHermesProcId;
  // TODO(chogan): @implement
  // comm_api.get_app_proc_id = MpiGetAppProcId;
  context->comm_api.get_num_world_procs = MpiGetNumWorldProcs;
  context->comm_api.get_num_hermes_procs = MpiGetNumHermesProcs;
  // TODO(chogan): @implement
  // comm_api.get_num_app_procs = MpiGetNumAppProcs;
  context->comm_api.world_barrier = MpiWorldBarrier;
  context->comm_api.hermes_barrier = MpiHermesBarrier;
  // TODO(chogan): @implement
  // comm_api.app_barrier = MpiAppBarrier;
  context->comm_api.finalize = MpiFinalize;

  MPIState *mpi_state = PushStruct<MPIState>(arena);
  mpi_state->world_comm = MPI_COMM_WORLD;
  context->comm_state.state = mpi_state;
  context->comm_state.world_proc_id = MpiGetWorldProcId(&context->comm_state);
  context->comm_state.world_size = MpiGetNumWorldProcs(&context->comm_state);

  int color = (int)ProcessKind::kHermes;
  int world_id = context->comm_state.world_proc_id;
  mpi_state->hermes_comm = MpiSplitCommunicator(mpi_state->world_comm, color,
                                                world_id);
  context->comm_state.hermes_proc_id = MpiGetHermesProcId(&context->comm_state);
  context->comm_state.hermes_size = MpiGetNumHermesProcs(&context->comm_state);
  context->comm_state.node_id = -1;
  context->comm_state.num_nodes = -1;

  // TODO(chogan): Application core communication intialization is currently
  // being done in buffer_pool_test.cc. It will eventually need to be done on
  // Hermes initialization.
}

}  // namespace hermes
