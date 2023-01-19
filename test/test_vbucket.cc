//
// Created by lukemartinlogan on 1/5/23.
//

#include <cstdlib>
#include <string>

#include <mpi.h>
#include <glog/logging.h>
#include "hermes.h"
#include "bucket.h"
#include "vbucket.h"

#include "basic_test.h"

namespace hapi = hermes::api;

void MainPretest() {
  hapi::Hermes::Create(hermes::HermesType::kClient);
}

void MainPosttest() {
  HERMES->Finalize();
}

void TestVBucketCreateDestroy(hapi::Hermes *hermes) {
  auto bkt = hermes->GetBucket("hello");
  auto vbkt = hermes->GetVBucket("hello");
  hermes::api::Context ctx;

  hapi::Blob blob(1024);
  hermes::BlobId blob_id;
  bkt->Put("0", blob, blob_id, ctx);
  vbkt->Link(blob_id, ctx);
}


TEST_CASE("TestVBucketCreate") {
  TestVBucketCreateDestroy(HERMES);
}