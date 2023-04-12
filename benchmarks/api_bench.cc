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

/** Gather times per-process */
void GatherTimes(std::string test_name, size_t io_size, Timer &t) {
  MPI_Barrier(MPI_COMM_WORLD);
  int rank, nprocs;
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  double time = t.GetSec(), max;
  MPI_Reduce(&time, &max,
             1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
  if (rank == 0) {
    double mbps = io_size / (max * 1000000);
    HIPRINT("{}: Time: {} sec, MBps (or MOps): {}, Count: {}, Nprocs: {}\n",
            test_name, max, mbps, io_size, nprocs);
  }
}

/** Each process PUTS into the same bucket, but with different blob names */
void PutTest(hapi::Hermes *hermes,
             int nprocs, int rank,
             int repeat, size_t blobs_per_rank, size_t blob_size) {
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
  GatherTimes("Put", nprocs * blobs_per_rank * blob_size * repeat, t);
}

/**
 * Each process GETS from the same bucket, but with different blob names
 * MUST run PutTest first.
 * */
void GetTest(hapi::Hermes *hermes,
             int nprocs, int rank,
             int repeat, size_t blobs_per_rank, size_t blob_size) {
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
  GatherTimes("Get", nprocs * blobs_per_rank * blob_size * repeat, t);
}

/** Each process PUTs then GETs */
void PutGetTest(hapi::Hermes *hermes,
                int nprocs, int rank, int repeat,
                size_t blobs_per_rank, size_t blob_size) {
  PutTest(hermes, nprocs, rank, repeat, blobs_per_rank, blob_size);
  MPI_Barrier(MPI_COMM_WORLD);
  GetTest(hermes, nprocs, rank, repeat, blobs_per_rank, blob_size);
}

/** Each process creates a set of buckets */
void CreateBucketTest(hapi::Hermes *hermes, int nprocs, int rank,
                      size_t bkts_per_rank) {
  Timer t;
  t.Resume();
  for (size_t i = 0; i < bkts_per_rank; ++i) {
    int bkt_name = rank * bkts_per_rank + i;
    hapi::Bucket bkt = hermes->GetBucket(std::to_string(bkt_name));
  }
  t.Pause();
  GatherTimes("CreateBucket", bkts_per_rank * nprocs, t);
}

/** Each process gets existing buckets */
void GetBucketTest(hapi::Hermes *hermes, int nprocs, int rank,
                   size_t bkts_per_rank) {
  // Initially create the buckets
  for (size_t i = 0; i < bkts_per_rank; ++i) {
    int bkt_name = rank * bkts_per_rank + i;
    hapi::Bucket bkt = hermes->GetBucket(std::to_string(bkt_name));
  }

  // Get existing buckets
  Timer t;
  t.Resume();
  for (size_t i = 0; i < bkts_per_rank; ++i) {
    int bkt_name = rank * bkts_per_rank + i;
    hapi::Bucket bkt = hermes->GetBucket(std::to_string(bkt_name));
  }
  t.Pause();
  GatherTimes("CreateBucket", bkts_per_rank * nprocs, t);
}

/** Each process creates (no data) a blob in a single bucket */
void CreateBlobOneBucketTest(hapi::Hermes *hermes,
                             int nprocs, int rank,
                             size_t blobs_per_rank) {
  Timer t;
  hapi::Bucket bkt = hermes->GetBucket("CreateBlobOneBucket");
  hapi::Context ctx;
  hermes::BlobId blob_id;

  t.Resume();
  for (size_t i = 0; i < blobs_per_rank; ++i) {
    size_t blob_name_int = rank * blobs_per_rank + i;
    std::string name = std::to_string(blob_name_int);
    bkt.TryCreateBlob(name, blob_id, ctx);
  }
  t.Pause();
  GatherTimes("CreateBlobOneBucket", blobs_per_rank * nprocs, t);
}

/** Each process creates (no data) a blob in a process-specific bucket */
void CreateBlobPerBucketTest(hapi::Hermes *hermes,
                             int nprocs, int rank,
                             size_t blobs_per_rank) {
  Timer t;
  hapi::Bucket bkt = hermes->GetBucket(
      hshm::Formatter::format("CreateBlobPerBucket{}", rank));
  hapi::Context ctx;
  hermes::BlobId blob_id;

  t.Resume();
  for (size_t i = 0; i < blobs_per_rank; ++i) {
    size_t blob_name_int = rank * blobs_per_rank + i;
    std::string name = std::to_string(blob_name_int);
    bkt.TryCreateBlob(name, blob_id, ctx);
  }
  t.Pause();
  GatherTimes("CreateBlobPerBucket", nprocs * blobs_per_rank, t);
}

/** Each process deletes a number of buckets */
void DeleteBucketTest(hapi::Hermes *hermes,
                      int nprocs, int rank,
                      size_t bkt_per_rank,
                      size_t blobs_per_bucket) {
  Timer t;
  hapi::Context ctx;
  hermes::BlobId blob_id;

  // Create the buckets
  for (size_t i = 0; i < bkt_per_rank; ++i) {
    hapi::Bucket bkt = hermes->GetBucket(
        hshm::Formatter::format("DeleteBucket{}", rank));
    for (size_t j = 0; j < blobs_per_bucket; ++j) {
      std::string name = std::to_string(j);
      bkt.TryCreateBlob(name, blob_id, ctx);
    }
  }

  // Delete the buckets
  t.Resume();
  for (size_t i = 0; i < bkt_per_rank; ++i) {
    hapi::Bucket bkt = hermes->GetBucket(
        hshm::Formatter::format("DeleteBucket{}", rank));
    size_t blob_name_int = rank * bkt_per_rank + i;
    std::string name = std::to_string(blob_name_int);
    bkt.TryCreateBlob(name, blob_id, ctx);
  }
  t.Pause();
  GatherTimes("DeleteBucket", nprocs * bkt_per_rank * blobs_per_bucket, t);
}

/** Each process deletes blobs from a single bucket */
void DeleteBlobOneBucket(hapi::Hermes *hermes,
                         int nprocs, int rank,
                         size_t blobs_per_rank) {
  Timer t;
  CreateBlobOneBucketTest(hermes, nprocs, rank, blobs_per_rank);
  MPI_Barrier(MPI_COMM_WORLD);
  hapi::Bucket bkt = hermes->GetBucket("CreateBlobOneBucket");
  hermes::BlobId blob_id;
  hapi::Context ctx;

  // Delete blobs from a bucket
  t.Resume();
  for (size_t i = 0; i < blobs_per_rank; ++i) {
    size_t blob_name_int = rank * blobs_per_rank + i;
    std::string name = std::to_string(blob_name_int);
    bkt.GetBlobId(name, blob_id);
    bkt.DestroyBlob(blob_id, ctx);
  }
  t.Pause();
  GatherTimes("CreateBlobOneBucket", nprocs * blobs_per_rank, t);
}


#define REQUIRE_ARGC_GE(N) \
  if (argc < (N)) { \
    HIPRINT("Requires fewer than {} params\n", N); \
    help(); \
  }

#define REQUIRE_ARGC(N) \
  if (argc != (N)) { \
    HIPRINT("Requires exactly {} params\n", N); \
    help(); \
  }

void help() {
  printf("USAGE: ./api_bench [mode] ...\n");
  printf("USAGE: ./api_bench put [blob_size (K/M/G)] [blobs_per_rank]\n");
  printf("USAGE: ./api_bench putget [blob_size (K/M/G)] [blobs_per_rank]\n");
  printf("USAGE: ./api_bench create_bkt [bkts_per_rank]\n");
  printf("USAGE: ./api_bench get_bkt [bkts_per_rank]\n");
  printf("USAGE: ./api_bench create_blob_1bkt [blobs_per_rank]\n");
  printf("USAGE: ./api_bench create_blob_Nbkt [blobs_per_rank]\n");
  printf("USAGE: ./api_bench del_bkt [bkt_per_rank] [blobs_per_bkt]\n");
  printf("USAGE: ./api_bench del_blobs [blobs_per_rank]\n");
  exit(1);
}

int main(int argc, char **argv) {
  int rank, nprocs;
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  auto hermes = hapi::Hermes::Create(hermes::HermesType::kClient);

  // Get mode
  REQUIRE_ARGC_GE(2)
  std::string mode = argv[1];

  MPI_Barrier(MPI_COMM_WORLD);

  // Run tests
  if (mode == "put") {
    REQUIRE_ARGC(4)
    size_t blob_size = hshm::ConfigParse::ParseSize(argv[2]);
    size_t blobs_per_rank = atoi(argv[3]);
    PutTest(hermes, nprocs, rank, 1, blobs_per_rank, blob_size);
  } else if (mode == "putget") {
    REQUIRE_ARGC(4)
    size_t blob_size = hshm::ConfigParse::ParseSize(argv[2]);
    size_t blobs_per_rank = atoi(argv[3]);
    PutGetTest(hermes, nprocs, rank, 1, blobs_per_rank, blob_size);
  } else if (mode == "create_bkt") {
    REQUIRE_ARGC(3)
    size_t bkts_per_rank = atoi(argv[2]);
    CreateBucketTest(hermes, nprocs, rank, bkts_per_rank);
  } else if (mode == "get_bkt") {
    REQUIRE_ARGC(3)
    size_t bkts_per_rank = atoi(argv[2]);
    GetBucketTest(hermes, nprocs, rank, bkts_per_rank);
  } else if (mode == "create_blob_1bkt") {
    REQUIRE_ARGC(3)
    size_t blobs_per_rank = atoi(argv[2]);
    CreateBlobOneBucketTest(hermes, nprocs, rank, blobs_per_rank);
  } else if (mode == "create_blob_Nbkt") {
    REQUIRE_ARGC(3)
    size_t blobs_per_rank = atoi(argv[2]);
    CreateBlobPerBucketTest(hermes, nprocs, rank, blobs_per_rank);
  } else if (mode == "del_bkt") {
    REQUIRE_ARGC(4)
    size_t bkt_per_rank = atoi(argv[2]);
    size_t blobs_per_bkt = atoi(argv[3]);
    DeleteBucketTest(hermes, nprocs, rank, bkt_per_rank, blobs_per_bkt);
  } else if (mode == "del_blobs") {
    REQUIRE_ARGC(4)
    size_t blobs_per_rank = atoi(argv[2]);
    DeleteBlobOneBucket(hermes, nprocs, rank, blobs_per_rank);
  }

  hermes->Finalize();
  MPI_Finalize();
}
