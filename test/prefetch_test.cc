//
// Created by lukemartinlogan on 10/20/22.
//

#include <mpi.h>
#include "hermes.h"
#include "bucket.h"
#include "vbucket.h"
#include <mapper/abstract_mapper.h>
#include <memory>

namespace hapi = hermes::api;

bool VerifyBlob(hapi::Blob &blob, int nonce, size_t ret_size) {
  if (ret_size != blob.size()) {
    LOG(INFO) << "The blob get did not get full blob" << std::endl;
    return false;
  }
  for (auto &c : blob) {
    if (c != static_cast<char>(nonce)) {
      LOG(INFO) << "The blob get was not successful" << std::endl;
      return false;
    }
  }
  return true;
}

int main(int argc, char **argv) {
  MPI_Init(&argc, &argv);
  std::shared_ptr<hapi::Hermes> hermes =
      hermes::InitHermesDaemon(getenv("HERMES_CONF"));

  bool with_prefetch = atoi(argv[1]);

  hapi::Context ctx;
  if (with_prefetch) {
    ctx.pctx_.hint_ = hapi::PrefetchHint::kFileSequential;
    ctx.pctx_.read_ahead_ = 1;
  }
  std::string bkt_name = "PREFETCH";
  auto bkt = std::make_shared<hapi::Bucket>(
      bkt_name, hermes, ctx);

  // Place 100 1MB blobs
  for (int i = 0; i < 100; ++i) {
    hapi::Blob blob;
    blob.resize(MEGABYTES(1), i);
    hermes::adapter::BlobPlacement p;
    p.page_ = i;
    std::string blob_name = p.CreateBlobName();
    bkt->Put(blob_name, blob);
  }
  LOG(INFO) << "FINISHED PUTTING ALL BLOBS" << std::endl;

  // Get 100 1MB blobs (sequentially)
  for (int i = 0; i < 100; ++i) {
    hapi::Blob blob;
    blob.resize(MEGABYTES(1), -1);
    hermes::adapter::BlobPlacement p;
    p.page_ = i;
    std::string blob_name = p.CreateBlobName();
    size_t ret = bkt->Get(blob_name, blob);
    assert(VerifyBlob(blob, i, ret));
    usleep(500000);
  }
  LOG(INFO) << "FINISHED GETTING ALL BLOBS" << std::endl;

  hermes->Finalize(true);
  MPI_Finalize();
}