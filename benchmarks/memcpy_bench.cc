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
hipc::MemoryBackend *backend;

void GatherTimes(const std::string &backend_type,
                 const std::string &test_name,
                 size_t size_per_rank, Timer &t) {
  MPI_Barrier(MPI_COMM_WORLD);
  int nprocs, rank;
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  double time = t.GetSec(), max;
  MPI_Reduce(&time, &max,
             1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
  if (rank == 0) {
    HIPRINT("{} {}: MBps: {}\n",
            backend_type, test_name,
            size_per_rank * nprocs / t.GetUsec())
  }
}

void PutTest(const std::string &backend_type, int rank, int repeat,
             size_t blobs_per_rank, size_t blob_size) {
  Timer t;
  std::vector<char> data(blob_size);
  t.Resume();
  for (int j = 0; j < repeat; ++j) {
    for (size_t i = 0; i < blobs_per_rank; ++i) {
      size_t blob_off = (rank * blobs_per_rank + i) * blob_size;
      memcpy(backend->data_ + blob_off, data.data(), data.size());
    }
  }
  t.Pause();
  GatherTimes(backend_type, "Put", blobs_per_rank * blob_size * repeat, t);
}

void GetTest(const std::string &backend_type, int rank, int repeat,
             size_t blobs_per_rank, size_t blob_size) {
  Timer t;
  std::vector<char> data(blob_size);
  t.Resume();
  for (int j = 0; j < repeat; ++j) {
    for (size_t i = 0; i < blobs_per_rank; ++i) {
      size_t blob_off = (rank * blobs_per_rank + i) * blob_size;
      memcpy(data.data(), backend->data_ + blob_off, data.size());
    }
  }
  t.Pause();
  GatherTimes(backend_type, "Get", blobs_per_rank * blob_size * repeat, t);
}

template<typename BackendT>
void MemcpyBench(int nprocs, int rank,
                 size_t blob_size,
                 size_t blobs_per_rank) {
  std::string shm_url = "test_mem_backend";
  std::string backend_type;
  size_t backend_size;
  hipc::MemoryBackendType type;

  if constexpr(std::is_same_v<BackendT, hipc::PosixShmMmap>) {
    backend_type = "kPosixShmMmap";
    type = hipc::MemoryBackendType::kPosixShmMmap;
    backend_size = nprocs * blob_size * blobs_per_rank;
  } else if constexpr(std::is_same_v<BackendT, hipc::PosixMmap>) {
    backend_type = "kPosixMmap";
    type = hipc::MemoryBackendType::kPosixMmap;
    backend_size = nprocs * blob_size * blobs_per_rank;
  } else {
    HELOG(kFatal, "Invalid backend type");
  }

  if (rank == 0) {
    backend = HERMES_MEMORY_MANAGER->
        CreateBackend<BackendT>(backend_size, shm_url);
  }
  MPI_Barrier(MPI_COMM_WORLD);
  if (rank != 0) {
    HERMES_MEMORY_MANAGER->AttachBackend(type, shm_url);
  }
  MPI_Barrier(MPI_COMM_WORLD);

  PutTest(backend_type, rank, 1, blobs_per_rank, blob_size);
  MPI_Barrier(MPI_COMM_WORLD);
  GetTest(backend_type, rank, 1, blobs_per_rank, blob_size);
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

  MemcpyBench<hipc::PosixMmap>(nprocs, rank, blob_size, blobs_per_rank);
  MemcpyBench<hipc::PosixShmMmap>(nprocs, rank, blob_size, blobs_per_rank);

  MPI_Finalize();
}
