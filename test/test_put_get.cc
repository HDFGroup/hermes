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

#include <cstdlib>
#include <string>

#include <mpi.h>
#include <glog/logging.h>
#include "hermes.h"
#include "bucket.h"

#include "verify_buffer.h"

namespace hapi = hermes::api;

void TestManyPuts(hapi::Hermes *hermes) {
  auto bkt = hermes->GetBucket("hello");
  int num_blobs = 1;
  size_t blob_size = MEGABYTES(150);
  hermes::api::Context ctx;
  hermes::BlobId blob_id;

  for (size_t i = 0; i < num_blobs; ++i) {
    hermes::Blob blob(nullptr, blob_size);
    std::string name = std::to_string(i);
    char nonce = i % 256;
    memset(blob.data_mutable(), nonce, blob_size);
    bkt->Put(name, std::move(blob), blob_id, ctx);
  }

  for (size_t i = 0; i < num_blobs; ++i) {
    std::string name = std::to_string(i);
    char nonce = i % 256;
    hermes::Blob blob;
    bkt->GetBlobId(name, blob_id, ctx);
    bkt->Get(blob_id, blob, ctx);
    assert(blob.size() == blob_size);
    assert(VerifyBuffer(blob.data(), blob_size, nonce));
  }
}

void TestBlobOverride(hapi::Hermes *hermes) {
  auto bkt = hermes->GetBucket("hello");
  auto bkt2 = hermes->GetBucket("hello");
  hermes::api::Context ctx;
  hermes::BlobId blob_id;
  hermes::Blob blob(nullptr, 1024);
  for (size_t i = 0; i < 1024; ++i) {
    memset(blob.data_mutable(), 10, 1024);
    bkt2->Put("0", std::move(blob), blob_id, ctx);
    hermes::Blob ret;
    bkt->Get(blob_id, ret, ctx);
    assert(ret.size() == 1024);
    assert(VerifyBuffer(ret.data(), 1024, 10));
  }
}

int main(int argc, char* argv[]) {
  MPI_Init(&argc, &argv);
  auto hermes = hapi::Hermes::Create(hermes::HermesType::kClient);

  TestManyPuts(hermes);

  hermes->Finalize();
  MPI_Finalize();
  return 0;
}