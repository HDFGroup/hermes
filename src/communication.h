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

/**
 * @file communication.h
 *
 * A generic communication interface for Hermes that can be implemented by
 * multiple backends. See communication_mpi.cc for an example of how to
 * implement a communication backend.
 *
 * NOTE: this file is only applied to HermesType::kServer.
 * This does not get used in kClient mode.
 * It is used almost solely to ensure RPC servers remain
 * running until all nodes want to terminate their
 * Hermes core.
 */

namespace hermes {

/**
 A structure to represent MPI communication context
*/
class CommunicationContext {
 public:
  /** A unique identifier for each rank, relative to all ranks. */
  i32 world_proc_id_;
  /** The total number of ranks. */
  i32 world_size_;
  /** The type of communicator this is */
  HermesType type_;

 public:
  CommunicationContext() : world_proc_id_(0), world_size_(0) {}
  virtual void WorldBarrier() = 0; /** E.g., MPI_Barrier(MPI_COMM_WORLD)*/
  virtual void Finalize() = 0; /** E.g., MPI_Finalize() */
};

}  // namespace hermes

#include "communication_factory.h"

#endif  // HERMES_COMMUNICATION_H_
