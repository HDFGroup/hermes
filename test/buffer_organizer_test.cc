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

#include <numeric>

#include "hermes.h"
#include "vbucket.h"
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
void TestBackgroundFlush() {
  using hermes::u8;
  HermesPtr hermes = hermes::InitHermesDaemon();
  const int io_size = KILOBYTES(4);
  const int iters = 4;
  const int total_bytes = io_size * iters;
  std::string final_destination = "./test_background_flush.out";
  std::string bkt_name = "background_flush";
  hapi::Bucket bkt(bkt_name, hermes);
  hapi::VBucket vbkt(bkt_name, hermes);

  hapi::FileMappingTrait mapping;
  mapping.filename = final_destination;
  hapi::PersistTrait persist_trait(mapping, false);
  vbkt.Attach(&persist_trait);

  std::vector<u8> data(iters);
  std::iota(data.begin(), data.end(), 'a');

  for (int i = 0; i < iters; ++i) {
    hapi::Blob blob(io_size, data[i]);
    std::string blob_name = std::to_string(i);
    bkt.Put(blob_name, blob);

    persist_trait.file_mapping.offset_map.emplace(blob_name, i * io_size);
    vbkt.Link(blob_name, bkt_name);
  }

  vbkt.Destroy();
  bkt.Destroy();

  hermes->Finalize(true);

  // NOTE(chogan): Verify that file is on disk with correct data
  FILE *fh = fopen(final_destination.c_str(), "r");
  Assert(fh);

  std::vector<u8> result(total_bytes, 0);
  size_t bytes_read = fread(result.data(), 1, total_bytes, fh);
  Assert(bytes_read == total_bytes);

  for (int i = 0; i < iters; ++i) {
    for (int j = 0; j < io_size; ++j) {
      int index = i * io_size + j;
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
  TestBackgroundFlush();

  MPI_Finalize();
  return 0;
}
