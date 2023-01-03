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

int main(int argc, char* argv[]) {
  MPI_Init(&argc, &argv);
  auto hermes = hapi::Hermes::Create(hermes::HermesType::kClient);
  auto bkt = hermes->GetBucket("hello");
  auto bkt2 = hermes->GetBucket("hello");

  hermes::api::Context ctx;
  hermes::BlobId blob_id;
  hermes::Blob blob(1024);
  for (size_t i = 0; i < 1024; ++i) {
    memset(blob.data_mutable(), 10, 1024);
    bkt2->Put("0", std::move(blob), blob_id, ctx);
    hermes::Blob ret;
    bkt->Get(blob_id, ret, ctx);
    assert(ret.size() == 1024);
    assert(VerifyBuffer(ret.data(), 1024, 10));
  }

  hermes->Finalize();
  MPI_Finalize();
  return 0;
}