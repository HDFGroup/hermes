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
#include <vbucket.h>

#include "bucket.h"
#include "hermes.h"
#include "test_utils.h"

namespace hapi = hermes::api;

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

  int num_blobs_per_rank = 16;
  int data_size = 8 * 1024;
  hapi::Blob put_data(data_size, rand() % 255);
  hapi::Blob get_data(data_size, 255);
  if (hermes->IsApplicationCore()) {
    int app_rank = hermes->GetProcessRank();
    int app_size = hermes->GetNumProcesses();

    hapi::Context ctx;

    // Each rank puts and gets its portion of a blob to a shared bucket
    std::string bucket_name = "test_bucket" + std::to_string(app_rank);
    hapi::Bucket rank_bucket(bucket_name, hermes, ctx);
    for (int i = 0; i < num_blobs_per_rank; ++i) {
      std::string blob_name =
          "Blob_" + std::to_string(app_rank) + "_" + std::to_string(i);
      rank_bucket.Put(blob_name, put_data, ctx);
    }
    hapi::VBucket shared("shared_vbucket", hermes, false, ctx);
    for (int i = 0; i < num_blobs_per_rank; ++i) {
      std::string blob_name =
          "Blob_" + std::to_string(app_rank) + "_" + std::to_string(i);
      shared.Link(blob_name, bucket_name, ctx);
    }
    hermes->AppBarrier();
    if (app_rank == 0) {
      auto blob_ids = shared.GetLinks(ctx);
      Assert(blob_ids.size() == (unsigned long)app_size * num_blobs_per_rank);
      for (int rank = 0; rank < app_size; ++rank) {
        std::string bucket_name_temp = "test_bucket" + std::to_string(rank);
        for (int i = 0; i < num_blobs_per_rank; ++i) {
          std::string blob_name =
              "Blob_" + std::to_string(rank) + "_" + std::to_string(i);
          auto exists = shared.ContainsBlob(blob_name, bucket_name_temp);
          Assert(exists);
        }
      }
    }
    hermes->AppBarrier();
    rank_bucket.Destroy(ctx);
    hermes->AppBarrier();
    if (app_rank == 0) {
      auto blob_ids = shared.GetLinks(ctx);
      Assert(blob_ids.size() == (unsigned long)app_size * num_blobs_per_rank);
      for (int rank = 0; rank < app_size; ++rank) {
        std::string bucket_name_temp = "test_bucket" + std::to_string(rank);
        for (int i = 0; i < num_blobs_per_rank; ++i) {
          std::string blob_name =
              "Blob_" + std::to_string(rank) + "_" + std::to_string(i);
          auto exists = shared.ContainsBlob(blob_name, bucket_name_temp);
          Assert(!exists);
        }
      }
    }
    if (app_rank == 0) {
      shared.Destroy(ctx);
    }
    hermes->AppBarrier();
  } else {
    // Hermes core. No user code here.
  }

  hermes->Finalize();

  MPI_Finalize();

  return 0;
}
