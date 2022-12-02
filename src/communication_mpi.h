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

#ifndef HERMES_SRC_COMMUNICATION_MPI_H
#define HERMES_SRC_COMMUNICATION_MPI_H

#include "communication.h"
#include "mpi.h"
#include <set>

/**
 * @file communication_mpi.cc
 *
 * An implementation of the Hermes communication interface in MPI.
 */

namespace hermes {

class MpiCommunicator : public CommunicationContext {
 public:
  MPI_Comm world_comm;   /**< MPI world communicator */
  /** This communicator is in one of two groups, depending on the value of the
   * rank's ProcessKind. When its kHermes, sub_comm groups all Hermes cores, and
   * when its kApp, it groups all application cores. */
  MPI_Comm sub_comm;

 public:
  /**
   * get the MPI process ID of \a comm MPI communicator.
   */
  inline int GetProcId(MPI_Comm comm) {
    int result;
    MPI_Comm_rank(comm, &result);
    return result;
  }

  /**
   get the MPI process ID of MPI world communicator from \a  MPI.
*/
  inline int GetWorldProcId() {
    int result = GetProcId(world_comm);
    return result;
  }

  /**
     get the MPI process ID of MPI sub-communicator from \a  MPI.
  */
  inline int GetSubProcId() {
    int result = GetProcId(sub_comm);
    return result;
  }

  /**
     get the number of MPI processes of \a comm MPI communicator.
  */
  inline int GetNumProcs(MPI_Comm comm) {
    int result;
    MPI_Comm_size(comm, &result);
    return result;
  }

  /**
     get the number of MPI processes of MPI world communicator.
  */
  inline int GetNumWorldProcs() {
    return GetNumProcs(world_comm);
  }

  /**
    a wrapper for MPI_Barrier() fucntion
  */
  inline void Barrier(MPI_Comm comm) {
    MPI_Barrier(comm);
  }

  /**
    a wrapper for MPI global communicator's MPI_Barrier() function
  */
  inline void WorldBarrier() override {
    Barrier(world_comm);
  }

  /**
    a wrapper for MPI sub-communicator's MPI_Barrier() function
  */
  inline void SubBarrier() override {
    Barrier(sub_comm);
  }

  /**
 * Returns true if the calling rank is the lowest numbered rank on its node in
 * communicator @p comm.
   */
  bool FirstOnNode(MPI_Comm comm) {
    int rank = GetProcId(comm);
    int size = GetNumProcs(comm);

    // TODO(llogan): arena -> labstor allocator
    size_t scratch_size = (size * sizeof(char *) +
                           size * sizeof(char) * MPI_MAX_PROCESSOR_NAME);
    std::vector<char*> node_names(size);
    for (size_t i = 0; i < size; ++i) {
      node_names[i] = new char[MPI_MAX_PROCESSOR_NAME];
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
  size_t AssignIDsToNodes(size_t trans_arena_size_per_node) {
    int rank = world_proc_id;
    int size = world_size;

    // TODO(llogan): arena -> labstor allocator
    size_t scratch_size = (size * sizeof(char *) +
                           size * sizeof(char) * MPI_MAX_PROCESSOR_NAME);
    std::vector<char*> node_names;
    node_names.resize(size);
    for (size_t i = 0; i < size; ++i) {
      node_names[i] = new char[MPI_MAX_PROCESSOR_NAME];
    }

    int length;
    MPI_Get_processor_name(node_names[rank], &length);

    MPI_Allgather(MPI_IN_PLACE, 0, 0, node_names[0], MPI_MAX_PROCESSOR_NAME,
                  MPI_CHAR, world_comm);

    // NOTE(chogan): Assign the first rank on each node to be the process that
    // runs the Hermes core. The other ranks become application cores.
    int first_on_node = 0;
    while (strncmp(node_names[rank], node_names[first_on_node],
                   MPI_MAX_PROCESSOR_NAME) != 0) {
      first_on_node++;
    }

    if (first_on_node == rank) {
      proc_kind = ProcessKind::kHermes;
    } else {
      proc_kind = ProcessKind::kApp;
    }

    // NOTE(chogan): Create a set of unique node names
    std::set<std::string> names;
    for (int i = 0; i < size; ++i) {
      std::string name(node_names[i], length);
      names.insert(name);
    }

    num_nodes = names.size();

    std::vector<int> ranks_per_node_local(num_nodes, 0);

    // NOTE(chogan): 1-based index so 0 can be the NULL BufferID
    int index = 1;
    for (auto &name : names) {
      if (name == node_names[rank]) {
        node_id = index;
        ranks_per_node_local[index - 1]++;
        break;
      }
      ++index;
    }

    // NOTE(chogan): We're returning the transient arena size in bytes for the
    // calling rank. To calculate that, we need to divide the total transient
    // arena size for each node by the number of ranks on that node.
    std::vector<int> ranks_per_node(num_nodes);
    MPI_Allreduce(ranks_per_node_local.data(), ranks_per_node.data(),
                  num_nodes, MPI_INT, MPI_SUM, world_comm);

    size_t result =
        trans_arena_size_per_node / (size_t)ranks_per_node[node_id - 1];

    return result;
  }

  /**
     a wrapper for MPI_Finalize() function
   */
  void Finalize() override {
    MPI_Finalize();
  }

  /**
     initialize MPI communication.
   */
  MpiCommunicator(size_t trans_arena_size_per_node, bool is_daemon,
                  bool is_adapter) {
    world_comm = MPI_COMM_WORLD;
    world_proc_id = GetWorldProcId();
    world_size = GetNumWorldProcs();

    size_t trans_arena_size_for_rank =
        AssignIDsToNodes(trans_arena_size_per_node);

    if (is_daemon || is_adapter) {
      sub_comm = world_comm;
      sub_proc_id = world_proc_id;
      if (is_daemon) {
        hermes_size = world_size;
        proc_kind = ProcessKind::kHermes;
      } else {
        app_size = world_size;
        proc_kind = ProcessKind::kApp;
      }
      first_on_node = FirstOnNode(world_comm);
    } else {
      int color = (int)proc_kind;
      MPI_Comm_split(world_comm, color, world_proc_id,
                     &sub_comm);

      first_on_node = FirstOnNode(sub_comm);
      sub_proc_id = GetSubProcId();

      switch (proc_kind) {
        case ProcessKind::kHermes: {
          MPI_Comm_size(sub_comm, &hermes_size);
          break;
        }
        case ProcessKind::kApp: {
          MPI_Comm_size(sub_comm, &app_size);
          break;
        }
        default: {
          HERMES_INVALID_CODE_PATH;
          break;
        }
      }
    }

    // return trans_arena_size_for_rank;
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_COMMUNICATION_MPI_H
