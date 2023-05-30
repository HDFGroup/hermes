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
#include "traits/prefetcher/prefetcher_trait.h"

using hshm::ipc::PosixShmMmap;
using Timer = hshm::HighResMonotonicTimer;
hipc::MemoryBackend *backend;

void GatherTimes(const std::string &test_name,
                 size_t total_size,
                 Timer &t, Timer &io_t) {
  MPI_Barrier(MPI_COMM_WORLD);
  int nprocs, rank;
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  double time = t.GetUsec(), max_runtime;
  MPI_Reduce(&time, &max_runtime,
             1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
  double io_time = t.GetUsec(), max_io_time;
  MPI_Reduce(&io_time, &max_io_time,
             1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
  if (rank == 0) {
    HIPRINT("{} {}: MBps: {}, Time: {}\n",
            nprocs, test_name,
            total_size / max_io_time,
            max_runtime)
  }
}

/** Each process PUTS into the same bucket, but with different blob names */
void PutTest(int nprocs, int rank,
             size_t blobs_per_checkpt,
             size_t num_checkpts,
             size_t blob_size,
             int compute_sec) {
  Timer t, io_t;
  auto bkt = HERMES->GetBucket("hello");
  hermes::api::Context ctx;
  hermes::BlobId blob_id;
  hermes::Blob blob(blob_size);
  t.Resume();

  size_t blobs_per_rank = blobs_per_checkpt * num_checkpts;
  size_t cur_blob = 0;
  for (size_t i = 0; i < num_checkpts; ++i) {
    io_t.Resume();
    for (size_t j = 0; j < blobs_per_checkpt; ++j) {
      size_t blob_name_int = rank * blobs_per_rank + cur_blob;
      std::string name = std::to_string(blob_name_int);
      bkt.Put(name, blob, blob_id, ctx);
      ++cur_blob;
    }
    io_t.Pause();
    sleep(compute_sec);
  }
  t.Pause();
  GatherTimes("Put", nprocs * blobs_per_rank * blob_size, t, io_t);
}

/**
 * Each process GETS from the same bucket, but with different blob names
 * MUST run PutTest first.
 * */
void GetTest(int nprocs, int rank,
             size_t blobs_per_checkpt,
             size_t num_checkpts,
             size_t blob_size,
             int compute_sec) {
  Timer t, io_t;
  auto bkt = HERMES->GetBucket("hello");
  hermes::api::Context ctx;
  hermes::BlobId blob_id;
  t.Resume();
  size_t blobs_per_rank = blobs_per_checkpt * num_checkpts;
  size_t cur_blob = 0;
  for (size_t i = 0; i < num_checkpts; ++i) {
    io_t.Resume();
    for (size_t j = 0; j < blobs_per_checkpt; ++j) {
      size_t blob_name_int = rank * blobs_per_rank + cur_blob;
      std::string name = std::to_string(blob_name_int);
      hermes::Blob ret;
      bkt.GetBlobId(name, blob_id);
      bkt.Get(blob_id, ret, ctx);
      ++cur_blob;
    }
    io_t.Pause();
    sleep(compute_sec);
  }
  t.Pause();
  GatherTimes("Get", nprocs * blobs_per_rank * blob_size, t, io_t);
}

int main(int argc, char **argv) {
  int rank, nprocs;
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  if (argc != 7) {
    printf("USAGE: ./reorganize [prefetch] [blob_size (K/M/G)] "
           "[blobs_per_checkpt] [num_checkpts] "
           "[compute_put (sec)] [compute_get (sec)]\n");
    exit(1);
  }
  int with_prefetch = atoi(argv[1]);
  size_t blob_size = hshm::ConfigParse::ParseSize(argv[2]);
  size_t blobs_per_checkpt = atoi(argv[3]);
  size_t num_checkpts = atoi(argv[4]);
  int compute_put = atoi(argv[5]);
  int compute_get = atoi(argv[6]);

  // Register the Apriori trait
  hermes::TraitId apriori_trait = HERMES->RegisterTrait<hapi::PrefetcherTrait>(
    "apriori", hermes::PrefetcherType::kApriori);
  if (with_prefetch) {
    auto bkt = HERMES->GetBucket("hello");
    bkt.AttachTrait(apriori_trait);
  }

  PutTest(nprocs, rank, blobs_per_checkpt, num_checkpts,
          blob_size, compute_put);
  MPI_Barrier(MPI_COMM_WORLD);
  GetTest(nprocs, rank, blobs_per_checkpt, num_checkpts,
          blob_size, compute_get);
  MPI_Finalize();
}
