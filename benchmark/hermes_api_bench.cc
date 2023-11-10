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
#include <hermes_shm/util/timer_mpi.h>
#include <mpi.h>
#include "hermes/hermes.h"
#include "hermes/bucket.h"
#include "hrun/work_orchestrator/affinity.h"

namespace hapi = hermes;
using hshm::MpiTimer;

/** Gather times per-process */
void GatherTimes(std::string test_name, size_t io_size, MpiTimer &t) {
  t.Collect();
  if (t.rank_ == 0) {
    double max = t.GetSec();
    double mbps = io_size / t.GetUsec();
    HIPRINT("{}: Time: {} sec, MBps (or MOps): {}, Count: {}, Nprocs: {}\n",
            test_name, max, mbps, io_size, t.nprocs_);
  }
}

/** Each process PUTS into the same bucket, but with different blob names */
void PutTest(int nprocs, int rank,
             int repeat, size_t blobs_per_rank, size_t blob_size) {
  MpiTimer t(MPI_COMM_WORLD);
  hermes::Context ctx;
  hermes::Bucket bkt("hello", ctx);
  hermes::Blob blob(blob_size);
  t.Resume();
  for (int j = 0; j < repeat; ++j) {
    for (size_t i = 0; i < blobs_per_rank; ++i) {
      size_t blob_name_int = rank * blobs_per_rank + i;
      std::string name = std::to_string(blob_name_int);
      bkt.AsyncPut(name, blob, ctx);
    }
  }
  t.Pause();
  HILOG(kInfo, "Finished PUT")
  GatherTimes("Put", nprocs * blobs_per_rank * blob_size * repeat, t);
}

/**
 * Each process GETS from the same bucket, but with different blob names
 * MUST run PutTest first.
 * */
void GetTest(int nprocs, int rank,
             int repeat, size_t blobs_per_rank, size_t blob_size) {
  MpiTimer t(MPI_COMM_WORLD);
  hermes::Context ctx;
  hermes::Bucket bkt("hello", ctx);
  t.Resume();
  for (int j = 0; j < repeat; ++j) {
    for (size_t i = 0; i < blobs_per_rank; ++i) {
      hermes::Blob ret(blob_size);
      size_t blob_name_int = rank * blobs_per_rank + i;
      std::string name = std::to_string(blob_name_int);
      bkt.Get(name, ret, ctx);
    }
  }
  t.Pause();
  GatherTimes("Get", nprocs * blobs_per_rank * blob_size * repeat, t);
}

/** Each process PUTs then GETs */
void PutGetTest(int nprocs, int rank, int repeat,
                size_t blobs_per_rank, size_t blob_size) {
  PutTest(nprocs, rank, repeat, blobs_per_rank, blob_size);
  MPI_Barrier(MPI_COMM_WORLD);
  HRUN_ADMIN->FlushRoot(DomainId::GetGlobal());
  GetTest(nprocs, rank, repeat, blobs_per_rank, blob_size);
}

/** Each process PUTS into the same bucket, but with different blob names */
void PartialPutTest(int nprocs, int rank,
                    int repeat, size_t blobs_per_rank,
                    size_t blob_size, size_t part_size) {
  MpiTimer t(MPI_COMM_WORLD);
  hermes::Context ctx;
  hermes::Bucket bkt("hello", ctx);
  hermes::Blob blob(blob_size);
  t.Resume();
  for (int j = 0; j < repeat; ++j) {
    for (size_t i = 0; i < blobs_per_rank; ++i) {
      size_t blob_name_int = rank * blobs_per_rank + i;
      std::string name = std::to_string(blob_name_int);
      for (size_t cur_size = 0; cur_size < blob_size; cur_size += part_size) {
        bkt.PartialPut(name, blob, cur_size, ctx);
      }
    }
  }
  t.Pause();
  GatherTimes("PartialPut", nprocs * blobs_per_rank * blob_size * repeat, t);
}

/**
 * Each process GETS from the same bucket, but with different blob names
 * MUST run PutTest first.
 * */
void PartialGetTest(int nprocs, int rank,
                    int repeat, size_t blobs_per_rank,
                    size_t blob_size, size_t part_size) {
  MpiTimer t(MPI_COMM_WORLD);
  hermes::Context ctx;
  hermes::Bucket bkt("hello", ctx);
  t.Resume();
  for (int j = 0; j < repeat; ++j) {
    for (size_t i = 0; i < blobs_per_rank; ++i) {
      size_t blob_name_int = rank * blobs_per_rank + i;
      std::string name = std::to_string(blob_name_int);
      hermes::Blob ret(blob_size);
      for (size_t cur_size = 0; cur_size < blob_size; cur_size += part_size) {
        bkt.PartialGet(name, ret, cur_size, ctx);
      }
    }
  }
  t.Pause();
  GatherTimes("PartialGet", nprocs * blobs_per_rank * blob_size * repeat, t);
}

/** Each process PUTs then GETs */
void PartialPutGetTest(int nprocs, int rank, int repeat,
                       size_t blobs_per_rank, size_t blob_size,
                       size_t part_size) {
  PartialPutTest(nprocs, rank, repeat, blobs_per_rank, blob_size, part_size);
  MPI_Barrier(MPI_COMM_WORLD);
  PartialGetTest(nprocs, rank, repeat, blobs_per_rank, blob_size, part_size);
}

/** Each process creates a set of buckets */
void CreateBucketTest(int nprocs, int rank,
                      size_t bkts_per_rank) {
  MpiTimer t(MPI_COMM_WORLD);
  t.Resume();
  hapi::Context ctx;
  std::unordered_map<std::string, std::string> mdm_;
  for (size_t i = 0; i < bkts_per_rank; ++i) {
    int bkt_name_int = rank * bkts_per_rank + i;
    std::string bkt_name = std::to_string(bkt_name_int);
    hermes::Bucket bkt(bkt_name, ctx);
  }
  t.Pause();
  GatherTimes("CreateBucket", bkts_per_rank * nprocs, t);
}

/** Each process gets existing buckets */
void GetBucketTest(int nprocs, int rank,
                   size_t bkts_per_rank) {
  // Initially create the buckets
  hapi::Context ctx;
  for (size_t i = 0; i < bkts_per_rank; ++i) {
    int bkt_name = rank * bkts_per_rank + i;
    hermes::Bucket bkt(std::to_string(bkt_name), ctx);
  }

  // Get existing buckets
  MpiTimer t(MPI_COMM_WORLD);
  t.Resume();
  for (size_t i = 0; i < bkts_per_rank; ++i) {
    int bkt_name = rank * bkts_per_rank + i;
    hapi::Bucket bkt(std::to_string(bkt_name), ctx);
  }
  t.Pause();
  GatherTimes("GetBucket", bkts_per_rank * nprocs, t);
}

/** Each process deletes a number of buckets */
void DeleteBucketTest(int nprocs, int rank,
                      size_t bkt_per_rank,
                      size_t blobs_per_bucket) {
  MpiTimer t(MPI_COMM_WORLD);
  hapi::Context ctx;

  // Create the buckets
  for (size_t i = 0; i < bkt_per_rank; ++i) {
    hapi::Bucket bkt(hshm::Formatter::format("DeleteBucket{}", rank), ctx);
    hapi::Blob blob;
    for (size_t j = 0; j < blobs_per_bucket; ++j) {
      std::string name = std::to_string(j);
      bkt.Put(name, blob,  ctx);
    }
  }

  // Delete the buckets
  t.Resume();
  for (size_t i = 0; i < bkt_per_rank; ++i) {
    hapi::Bucket bkt(hshm::Formatter::format("DeleteBucket{}", rank), ctx);
    bkt.Destroy();
  }
  t.Pause();
  GatherTimes("DeleteBucket", nprocs * bkt_per_rank * blobs_per_bucket, t);
}

/** Each process deletes blobs from a single bucket */
void DeleteBlobOneBucket(int nprocs, int rank,
                         size_t blobs_per_rank) {
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
  printf("USAGE: ./api_bench pputget [blob_size (K/M/G)] [part_size (K/M/G)] [blobs_per_rank]\n");
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
  TRANSPARENT_HRUN();
  HERMES->ClientInit();

  // Get mode
  REQUIRE_ARGC_GE(2)
  std::string mode = argv[1];

  // Set CPU affinity
  // TODO(logan): remove
  ProcessAffiner::SetCpuAffinity(getpid(), 8);

  MPI_Barrier(MPI_COMM_WORLD);

  HIPRINT("Beginning {}\n", mode)

  // Run tests
  try {
    if (mode == "put") {
      REQUIRE_ARGC(4)
      size_t blob_size = hshm::ConfigParse::ParseSize(argv[2]);
      size_t blobs_per_rank = atoi(argv[3]);
      PutTest(nprocs, rank, 1, blobs_per_rank, blob_size);
    } else if (mode == "putget") {
      REQUIRE_ARGC(4)
      size_t blob_size = hshm::ConfigParse::ParseSize(argv[2]);
      size_t blobs_per_rank = atoi(argv[3]);
      PutGetTest(nprocs, rank, 1, blobs_per_rank, blob_size);
    } else if (mode == "pputget") {
      REQUIRE_ARGC(5)
      size_t blob_size = hshm::ConfigParse::ParseSize(argv[2]);
      size_t part_size = hshm::ConfigParse::ParseSize(argv[3]);
      size_t blobs_per_rank = atoi(argv[4]);
      PartialPutGetTest(nprocs, rank, 1, blobs_per_rank, blob_size, part_size);
    } else if (mode == "create_bkt") {
      REQUIRE_ARGC(3)
      size_t bkts_per_rank = atoi(argv[2]);
      CreateBucketTest(nprocs, rank, bkts_per_rank);
    } else if (mode == "get_bkt") {
      REQUIRE_ARGC(3)
      size_t bkts_per_rank = atoi(argv[2]);
      GetBucketTest(nprocs, rank, bkts_per_rank);
    } else if (mode == "del_bkt") {
      REQUIRE_ARGC(4)
      size_t bkt_per_rank = atoi(argv[2]);
      size_t blobs_per_bkt = atoi(argv[3]);
      DeleteBucketTest(nprocs, rank, bkt_per_rank, blobs_per_bkt);
    } else if (mode == "del_blobs") {
      REQUIRE_ARGC(4)
      size_t blobs_per_rank = atoi(argv[2]);
      DeleteBlobOneBucket(nprocs, rank, blobs_per_rank);
    }
  } catch (hshm::Error &err) {
    HELOG(kFatal, "Error: {}", err.what());
  }
  MPI_Finalize();
}
