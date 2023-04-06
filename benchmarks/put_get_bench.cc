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

#include <iostream>
#include <hermes_shm/util/timer.h>
#include <mpi.h>
#include "hermes.h"
#include "bucket.h"

namespace hapi = hermes::api;
using Timer = hshm::HighResMonotonicTimer;

void GatherTimes(std::string test_name, Timer &t) {
  MPI_Barrier(MPI_COMM_WORLD);
  int nprocs, rank;
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  double time = t.GetSec(), max;
  MPI_Reduce(&time, &max,
             1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
  if (rank == 0) {
    std::cout << test_name << ": Time (sec): " << max << std::endl;
  }
}

void PutTest(hapi::Hermes *hermes,
             int rank, int repeat, size_t blobs_per_rank, size_t blob_size) {
  Timer t;
  auto bkt = hermes->GetBucket("hello");
  hermes::api::Context ctx;
  hermes::BlobId blob_id;
  hermes::Blob blob(blob_size);
  t.Resume();
  for (int j = 0; j < repeat; ++j) {
    for (size_t i = 0; i < blobs_per_rank; ++i) {
      size_t blob_name_int = rank * blobs_per_rank + i;
      std::string name = std::to_string(blob_name_int);
      bkt.Put(name, blob, blob_id, ctx);
    }
  }
  t.Pause();
  GatherTimes("Put", t);
}

void GetTest(hapi::Hermes *hermes,
             int rank, int repeat, size_t blobs_per_rank, size_t blob_size) {
  Timer t;
  auto bkt = hermes->GetBucket("hello");
  hermes::api::Context ctx;
  hermes::BlobId blob_id;
  t.Resume();
  for (int j = 0; j < repeat; ++j) {
    for (size_t i = 0; i < blobs_per_rank; ++i) {
      size_t blob_name_int = rank * blobs_per_rank + i;
      std::string name = std::to_string(blob_name_int);
      hermes::Blob ret;
      bkt.GetBlobId(name, blob_id);
      bkt.Get(blob_id, ret, ctx);
    }
  }
  t.Pause();
  GatherTimes("Get", t);
}

int main(int argc, char **argv) {
  int rank;
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  if (argc != 3) {
    printf("USAGE: ./put_get_bench [blob_size (K/M/G)] [blobs_per_rank]\n");
    exit(1);
  }

  auto hermes = hapi::Hermes::Create(hermes::HermesType::kClient);
  size_t blob_size = hshm::ConfigParse::ParseSize(argv[1]);
  size_t blobs_per_rank = atoi(argv[2]);

  MPI_Barrier(MPI_COMM_WORLD);
  PutTest(hermes, rank, 1, blobs_per_rank, blob_size);
  MPI_Barrier(MPI_COMM_WORLD);
  GetTest(hermes, rank, 1, blobs_per_rank, blob_size);
  hermes->Finalize();
  MPI_Finalize();
}
