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

#include "hermes.h"
#include "test_utils.h"

#include <mpi.h>

namespace hapi = hermes::api;
using HermesPtr = std::shared_ptr<hapi::Hermes>;

void TestIsBoFunction() {
  using hermes::IsBoFunction;
  Assert(IsBoFunction("BO::TriggerBufferOrganizer"));
  Assert(IsBoFunction("BO::A"));
  Assert(IsBoFunction("BO::"));
  Assert(!IsBoFunction(""));
  Assert(!IsBoFunction("A"));
  Assert(!IsBoFunction("BO:"));
  Assert(!IsBoFunction("BO:A"));
  Assert(!IsBoFunction("TriggerBufferOrganizer"));
}

void TestBoTasks() {
  using hermes::BoTask;
  using hermes::BufferID;
  using hermes::TargetID;

  HermesPtr hermes = hermes::InitHermesDaemon();

  const int kNumThreads = 3;
  for (int i = 0; i < kNumThreads; ++i) {
    BoTask task = {};
    BufferID bid = {};
    TargetID tid = {};
    bid.as_int = i;
    tid.as_int = i;
    if (i == 0) {
      task.op = hermes::BoOperation::kMove;
      task.args.move_args = {bid, tid};
    } else if (i == 1) {
      task.op = hermes::BoOperation::kCopy;
      task.args.copy_args = {bid, tid};
    } else {
      task.op = hermes::BoOperation::kDelete;
      task.args.delete_args = {bid};
    }
    LocalEnqueueBoTask(&hermes->context_, task);
  }

  hermes->Finalize(true);
}

int main(int argc, char *argv[]) {
  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  TestIsBoFunction();
  TestBoTasks();

  MPI_Finalize();
  return 0;
}
