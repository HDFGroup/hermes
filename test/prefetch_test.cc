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

#include <mpi.h>
#include "hermes.h"
#include "bucket.h"
#include "vbucket.h"
#include <mapper/abstract_mapper.h>
#include <memory>
#include "timer.h"

namespace hapi = hermes::api;

bool VerifyBlob(hapi::Blob &blob, int nonce, size_t ret_size) {
  if (ret_size != blob.size()) {
    LOG(INFO) << "The blob get did not get full blob: "
              << " ret_size: " << ret_size
              << " blob_size: " << std::endl;
    return false;
  }
  for (auto &c : blob) {
    if (c != static_cast<unsigned char>(nonce)) {
      LOG(INFO) << "The blob get was not successful"
                << " nonce: " << (int)nonce
                << " ret: " << (int)c;
      return false;
    }
  }
  return true;
}

int main(int argc, char **argv) {
  MPI_Init(&argc, &argv);
  std::shared_ptr<hapi::Hermes> hermes =
      hermes::InitHermesDaemon(getenv("HERMES_CONF"));

  if (argc != 6) {
    std::cout << "USAGE: ./prefetch_test"
              << "[with_prefetch] [dataset_mb] [blob_size_mb]"
              << "[read_ahead] [phase] [clear_cache_path]" << std::endl;
  }

  bool with_prefetch = atoi(argv[1]);
  size_t dataset_size = MEGABYTES(atoi(argv[2]));
  size_t blob_size = MEGABYTES(atoi(argv[3]));
  size_t blob_count = dataset_size / blob_size;
  int read_ahead = atoi(argv[4]);
  int phase = atoi(argv[5]);
  std::string clear_cache = argv[6];
  std::string clear_cache_script = ("bash " + clear_cache);

  std::cout << clear_cache_script << std::endl;
  system(clear_cache_script.c_str());

  std::cout << "Dataset Size: " << dataset_size / MEGABYTES(1)
            << " Blob Size: " << blob_size / MEGABYTES(1)
            << " Blob Count: " << blob_count << std::endl;

  hapi::Context ctx;
  if (with_prefetch) {
    ctx.pctx_.hint_ = hapi::PrefetchHint::kFileSequential;
    ctx.pctx_.read_ahead_ = read_ahead;
    printf("Prefetch is enabled\n");
  }
  std::string bkt_name = "PREFETCH";
  auto bkt = std::make_shared<hapi::Bucket>(
      bkt_name, hermes, ctx);

  // Place blobs
  for (int i = 0; i < blob_count; ++i) {
    hapi::Blob blob;
    blob.resize(blob_size, i);
    hermes::adapter::BlobPlacement p;
    p.page_ = i;
    std::string blob_name = p.CreateBlobName();
    bkt->Put(blob_name, blob);
  }
  LOG(INFO) << "FINISHED PUTTING ALL BLOBS" << std::endl;

  //std::cout << clear_cache_script << std::endl;
  //system(clear_cache_script.c_str());

  // Get blobs (sequentially)
  hermes::HighResMonotonicTimer t;
  t.Resume();
  for (int i = 0; i < blob_count; ++i) {
    hapi::Blob blob;
    blob.resize(blob_size, -1);
    hermes::adapter::BlobPlacement p;
    p.page_ = i;
    std::string blob_name = p.CreateBlobName();
    size_t ret = bkt->Get(blob_name, blob);
    assert(VerifyBlob(blob, i, ret));
    if (phase > 0) {
      hermes::HighResMonotonicTimer t2;
      t2.Resume();
      while(t2.GetMsecFromStart() < phase) {
        ABT_thread_yield();
      }
    }
  }
  t.Pause();
  LOG(INFO) << "FINISHED GETTING ALL BLOBS" << std::endl;
  std::cout <<"TIME: " << t.GetMsec() << std::endl;

  hermes->Finalize(true);
  MPI_Finalize();
}
