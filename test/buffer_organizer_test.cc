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

#include <numeric>

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

void TestTrickleDown() {
  using hermes::u8;
  HermesPtr hermes = hermes::InitHermesDaemon();
  const int io_size = KILOBYTES(4);
  const int iters = 4;
  const int total_bytes = io_size * iters;
  const int sleep_seconds = 1;
  std::string final_destination = "./test_trickle_down.out";
  hapi::Bucket bkt("trickle_down", hermes);

  std::vector<u8> data(iters);
  std::iota(data.begin(), data.end(), 'a');

  for (int i = 0; i < iters; ++i) {
    hapi::Blob blob(io_size, data[i]);
    std::string blob_name = std::to_string(i);
    hapi::FlushInfo flush(final_destination, i * io_size);
    hapi::Context ctx;;
    ctx.flush = flush;
    ctx.policy = hermes::api::PlacementPolicy::kRoundRobin;

    bkt.Put(blob_name, blob, ctx);
    std::this_thread::sleep_for(std::chrono::seconds(sleep_seconds));
  }

  hermes->Finalize(true);

  // NOTE(chogan): Verify that file is on disk with correct data
  FILE *fh = fopen(final_destination.c_str(), "r");
  Assert(fh);

  std::vector<u8> result(total_bytes, 0);
  size_t bytes_read = fread(result.data(), 1, total_bytes, fh);
  Assert(bytes_read == total_bytes);

  for (int i = 0; i < iters; ++i) {
    for (int j = 0; j < io_size; ++j) {
      int index = i * j;
      Assert(result[index] == data[i]);
    }
  }

  Assert(fclose(fh) == 0);
  Assert(std::remove(final_destination.c_str()) == 0);
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
  TestTrickleDown();

  MPI_Finalize();
  return 0;
}
