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

// #define HERMES_ENABLE_PROFILING 1

#include "mpi.h"
#include <stdlib.h>
#include "hermes.h"
#include "hermes_shm/memory/backend/posix_shm_mmap.h"
#include <hermes_shm/util/timer.h>

using hshm::ipc::PosixShmMmap;
using Timer = hshm::HighResMonotonicTimer;
PosixShmMmap backend;

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

void PutTest(int rank, int repeat, size_t blobs_per_rank, size_t blob_size) {
  Timer t;
  std::vector<char> data(blob_size);
  t.Resume();
  for (int j = 0; j < repeat; ++j) {
    for (size_t i = 0; i < blobs_per_rank; ++i) {
      size_t blob_off = (rank * blobs_per_rank + i) * blob_size;
      memcpy(backend.data_ + blob_off, data.data(), data.size());
    }
  }
  t.Pause();
  GatherTimes("Put", t);
}

void GetTest(int rank, int repeat, size_t blobs_per_rank, size_t blob_size) {
  Timer t;
  std::vector<char> data(blob_size);
  t.Resume();
  for (int j = 0; j < repeat; ++j) {
    for (size_t i = 0; i < blobs_per_rank; ++i) {
      size_t blob_off = (rank * blobs_per_rank + i) * blob_size;
      memcpy(data.data(), backend.data_ + blob_off, data.size());
    }
  }
  t.Pause();
  GatherTimes("Get", t);
}

int main(int argc, char **argv) {
  int rank, nprocs;
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  if (argc != 3) {
    printf("USAGE: ./memcpy_bench [blob_size (K/M/G)] [blobs_per_rank]\n");
    exit(1);
  }
  size_t blob_size = hshm::ConfigParse::ParseSize(argv[1]);
  size_t blobs_per_rank = atoi(argv[2]);
  size_t backend_size = nprocs * blob_size * blobs_per_rank;

  std::string shm_url = "test_mem_backend";
  if (rank == 0) {
    if (!backend.shm_init(backend_size, shm_url)) {
      throw std::runtime_error("Couldn't create backend");
    }
  }
  MPI_Barrier(MPI_COMM_WORLD);
  if (rank != 0) {
    backend.shm_deserialize(shm_url);
  }
  MPI_Barrier(MPI_COMM_WORLD);

  PutTest(rank, 1, blobs_per_rank, blob_size);
  MPI_Barrier(MPI_COMM_WORLD);
  GetTest(rank, 1, blobs_per_rank, blob_size);

  MPI_Finalize();
}
