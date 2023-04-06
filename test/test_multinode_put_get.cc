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
#include "hermes_shm/util/formatter.h"
#include "bucket.h"
#include "basic_test.h"

static const int kBlobSize = KILOBYTES(64);
static const int kBlobsPerRank = 1024;

void MainPretest() {
  hapi::Hermes::Create(hermes::HermesType::kClient);
}

void MainPosttest() {
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Barrier(MPI_COMM_WORLD);
  if (rank == 0) {
    HERMES->Finalize();
  }
}

// Put to my bucket
void PutTest(int rank) {
  std::string bkt_name = hshm::Formatter::format("bucket{}", rank);
  auto bkt = HERMES->GetBucket(bkt_name);
  hermes::api::Context ctx;
  hermes::BlobId blob_id;
  hermes::Blob blob(kBlobSize);
  for (size_t i = 0; i < kBlobsPerRank; ++i) {
    std::string name = std::to_string(i);
    memset(blob.data(), i, kBlobSize);
    bkt.Put(name, blob, blob_id, ctx);
  }
}

// Get from a different rank
void GetTest(int rank, int nprocs) {
  std::string bkt_name = hshm::Formatter::format(
      "bucket{}", (rank + 1) % nprocs);
  auto bkt = HERMES->GetBucket(bkt_name);
  hermes::api::Context ctx;
  hermes::BlobId blob_id;
  for (size_t i = 0; i < kBlobsPerRank; ++i) {
    std::string name = std::to_string(i);
    hermes::Blob ret;
    bkt.GetBlobId(name, blob_id);
    bkt.Get(blob_id, ret, ctx);
    REQUIRE(VerifyBuffer(ret.data(), kBlobSize, i));
  }
}

TEST_CASE("ParallelPutGetDifferentBuckets") {
  int rank, nprocs;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  MPI_Barrier(MPI_COMM_WORLD);
  PutTest(rank);
  MPI_Barrier(MPI_COMM_WORLD);
  GetTest(rank, nprocs);
}
