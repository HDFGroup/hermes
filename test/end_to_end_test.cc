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

#include <assert.h>

#include <mpi.h>

#include "hermes.h"
#include "bucket.h"
#include "test_utils.h"

namespace hapi = hermes::api;

void TestPutGetBucket(hapi::Bucket &bucket, int app_rank, int app_size) {
  size_t bytes_per_rank = KILOBYTES(8);
  if (app_size) {
    size_t data_size = KILOBYTES(8);
    bytes_per_rank = data_size / app_size;
    size_t remaining_bytes = data_size % app_size;

    if (app_rank == app_size - 1) {
      bytes_per_rank += remaining_bytes;
    }
  }

  hapi::Blob put_data(bytes_per_rank, rand() % 255);

  std::string blob_name = "test_blob" + std::to_string(app_rank);
  hermes::api::Status status = bucket.Put(blob_name, put_data);
  Assert(status.Succeeded());

  hapi::Blob get_result;
  size_t blob_size = bucket.Get(blob_name, get_result);
  get_result.resize(blob_size);
  blob_size = bucket.Get(blob_name, get_result);

  Assert(put_data == get_result);
}

void TestBulkTransfer(std::shared_ptr<hapi::Hermes> hermes, int app_rank) {
  size_t transfer_size = KILOBYTES(8);

  hapi::Bucket bucket(std::string("bulk_bucket"), hermes);
  std::string blob_name = "1";

  hapi::Blob put_data(transfer_size, 'x');

  if (app_rank == 0) {
    hermes::api::Status status = bucket.Put(blob_name, put_data);
    Assert(status.Succeeded());
  }

  hermes->AppBarrier();

  if (app_rank != 0) {
    hapi::Blob get_result;
    size_t blob_size = bucket.Get(blob_name, get_result);
    get_result.resize(blob_size);
    blob_size = bucket.Get(blob_name, get_result);

    Assert(get_result == put_data);

    bucket.Release();
  }

  hermes->AppBarrier();

  if (app_rank == 0) {
    bucket.Destroy();
  }
}

int main(int argc, char **argv) {
  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  char *config_file = 0;
  if (argc == 2) {
    config_file = argv[1];
  }

  std::shared_ptr<hapi::Hermes> hermes = hapi::InitHermes(config_file);

  if (hermes->IsApplicationCore()) {
    int app_rank = hermes->GetProcessRank();
    int app_size = hermes->GetNumProcesses();

    // Each rank puts and gets its portion of a blob to a shared bucket
    hapi::Bucket shared_bucket(std::string("test_bucket"), hermes);
    TestPutGetBucket(shared_bucket, app_rank, app_size);

    if (app_rank != 0) {
      shared_bucket.Release();
    }

    hermes->AppBarrier();

    if (app_rank == 0) {
      shared_bucket.Destroy();
    }

    hermes->AppBarrier();

    // Each rank puts a whole blob to its own bucket
    hapi::Bucket own_bucket(std::string("test_bucket_") +
                            std::to_string(app_rank), hermes);
    TestPutGetBucket(own_bucket, app_rank, 0);
    own_bucket.Destroy();

    TestBulkTransfer(hermes, app_rank);

    if (hermes->rpc_.num_nodes > 1) {
      if (app_rank == 0) {
        Assert(hermes->rpc_.node_id == 1);
        hermes::BoTask task = {};
        task.op = hermes::BoOperation::kMove;
        hermes::BufferID b_id = {};
        b_id.as_int = 1;
        hermes::TargetID t_id = {};
        t_id.as_int = 2;
        task.args.move_args.src = b_id;
        task.args.move_args.dest = t_id;
        hermes::RpcCall<bool>(&hermes->rpc_, 2, "BO::EnqueueBoTask", task,
                              hermes::BoPriority::kLow);
      }
    }

  } else {
    // Hermes core. No user code here.
  }

  hermes->Finalize();

  MPI_Finalize();

  return 0;
}
