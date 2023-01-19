//
// Created by lukemartinlogan on 12/27/22.
//

#include <iostream>
#include <labstor/util/timer.h>
#include <mpi.h>
#include "hermes.h"
#include "bucket.h"

namespace hapi = hermes::api;
using Timer = labstor::HighResMonotonicTimer;

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
             int rank, int repeat, int blobs_per_rank, size_t blob_size) {
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
      bkt->Put(name, std::move(blob), blob_id, ctx);
    }
  }
  t.Pause();
  GatherTimes("Put", t);
}

void GetTest(hapi::Hermes *hermes,
             int rank, int repeat, int blobs_per_rank, size_t blob_size) {
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
      hermes::Blob ret;
      bkt->GetBlobId(name, blob_id, ctx);
      bkt->Get(blob_id, ret, ctx);
    }
  }
  t.Pause();
  GatherTimes("Get", t);
}

int main(int argc, char **argv) {
  int rank;
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  auto hermes = hapi::Hermes::Create(hermes::HermesType::kClient);
  int blobs_per_rank = 1024;

  size_t blob_size = KILOBYTES(64);
  MPI_Barrier(MPI_COMM_WORLD);
  PutTest(hermes, rank, 1, blobs_per_rank, blob_size);
  MPI_Barrier(MPI_COMM_WORLD);
  // GetTest(hermes, rank, 1, blobs_per_rank, blob_size);
  hermes->Finalize();
  MPI_Finalize();
}