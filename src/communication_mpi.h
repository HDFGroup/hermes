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
#include <mpi.h>
#include <set>

/**
 * @file communication_mpi.cc
 *
 * An implementation of the Hermes communication interface in MPI.
 */

namespace hermes {

class MpiCommunicator : public CommunicationContext {
 public:
  MPI_Comm world_comm_;   /**< Represents all processes */
  MPI_Comm node_comm_;    /**< Represents all nodes */

 public:
  /** a wrapper for MPI global communicator's MPI_Barrier() function */
  inline void WorldBarrier() override {
    Barrier(world_comm_);
  }

  /** a wrapper for MPI_Finalize() function */
  void Finalize() override {
    MPI_Finalize();
  }

  /** initialize MPI communication. */
  MpiCommunicator(HermesType type) {
    world_comm_ = MPI_COMM_WORLD;
    world_proc_id_ = GetWorldProcId();
    world_size_ = GetNumWorldProcs();
    type_ = type;
  }

 private:
  /** get the MPI process ID of \a comm MPI communicator. */
  inline int GetProcId(MPI_Comm comm) {
    int result;
    MPI_Comm_rank(comm, &result);
    return result;
  }

  /** get the MPI process ID of MPI world communicator from \a  MPI. */
  inline int GetWorldProcId() {
    int result = GetProcId(world_comm_);
    return result;
  }

  /** get the number of MPI processes of \a comm MPI communicator. */
  inline int GetNumProcs(MPI_Comm comm) {
    int result;
    MPI_Comm_size(comm, &result);
    return result;
  }

  /** get the number of MPI processes of MPI world communicator. */
  inline int GetNumWorldProcs() {
    return GetNumProcs(world_comm_);
  }

  /** a wrapper for MPI_Barrier() function */
  inline void Barrier(MPI_Comm comm) {
    MPI_Barrier(comm);
  }
};

}  // namespace hermes

#endif  // HERMES_SRC_COMMUNICATION_MPI_H
