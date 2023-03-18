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

#include "basic_test.h"

namespace hapi = hermes::api;

void MainPretest() {
  hapi::Hermes::Create(hermes::HermesType::kClient);
}

void MainPosttest() {
  HERMES->Finalize();
}

void TestBlobCreates(hapi::Hermes *hermes) {
  auto bkt = hermes->GetBucket("hello");
  int num_blobs = 1;
  size_t blob_size = MEGABYTES(150);
  hermes::api::Context ctx;
  hermes::BlobId blob_id;

  for (size_t i = 0; i < num_blobs; ++i) {
    hermes::Blob blob(blob_size);
    std::string name = std::to_string(i);
    char nonce = i % 256;
    memset(blob.data(), nonce, blob_size);
    bkt->Put(name, blob, blob_id, ctx);
  }

  for (size_t i = 0; i < num_blobs; ++i) {
    std::string name = std::to_string(i);
    char nonce = i % 256;
    hermes::Blob blob;
    bkt->GetBlobId(name, blob_id);
    REQUIRE(!blob_id.IsNull());
    bkt->Get(blob_id, blob, ctx);
    REQUIRE(blob.size() == blob_size);
    REQUIRE(VerifyBuffer(blob.data(), blob_size, nonce));
  }
}

void TestBlobOverride(hapi::Hermes *hermes) {
  auto bkt = hermes->GetBucket("hello");
  auto bkt2 = hermes->GetBucket("hello");
  hermes::api::Context ctx;
  hermes::BlobId blob_id;
  hermes::Blob blob(1024);
  for (size_t i = 0; i < 1024; ++i) {
    memset(blob.data(), i, 1024);
    bkt2->Put("0", blob, blob_id, ctx);
    hermes::Blob ret;
    bkt->Get(blob_id, ret, ctx);
    REQUIRE(ret.size() == 1024);
    REQUIRE(VerifyBuffer(ret.data(), 1024, i));
  }
}

void TestBucketRename(hapi::Hermes *hermes) {
  auto bkt = hermes->GetBucket("hello");
  bkt->Rename("hello2");
  auto bkt1 = hermes->GetBucket("hello");
  auto bkt2 = hermes->GetBucket("hello2");

  REQUIRE(!bkt2->IsNull());
  REQUIRE(bkt1->GetId() != bkt2->GetId());
  REQUIRE(bkt->GetId() == bkt2->GetId());
}

void TestBucketClear(hapi::Hermes *hermes) {
  auto bkt = hermes->GetBucket("hello");
  int num_blobs = 16;
  size_t blob_size = MEGABYTES(150);
  hermes::api::Context ctx;
  hermes::BlobId blob_id;

  for (size_t i = 0; i < num_blobs; ++i) {
    hermes::Blob blob(blob_size);
    std::string name = std::to_string(i);
    char nonce = i % 256;
    memset(blob.data(), nonce, blob_size);
    bkt->Put(name, std::move(blob), blob_id, ctx);
  }

  bkt->Clear();

  REQUIRE(bkt->GetSize() == 0);
  auto blobs = bkt->GetContainedBlobIds();
  REQUIRE(blobs.size() == 0);
}

void TestBucketDestroy(hapi::Hermes *hermes) {
  auto bkt = hermes->GetBucket("hello");
  int num_blobs = 1;
  size_t blob_size = MEGABYTES(150);
  hermes::api::Context ctx;
  hermes::BlobId blob_id;

  for (size_t i = 0; i < num_blobs; ++i) {
    hermes::Blob blob(blob_size);
    std::string name = std::to_string(i);
    char nonce = i % 256;
    memset(blob.data(), nonce, blob_size);
    bkt->Put(name, std::move(blob), blob_id, ctx);
  }

  bkt->Destroy();

  // Check if the bucket can be re-obtained
  auto new_bkt = hermes->GetBucket("hello");
  REQUIRE(bkt->GetId() != new_bkt->GetId());
  REQUIRE(new_bkt->GetSize() == 0);
}

void TestBlobRename(hapi::Hermes *hermes) {
  auto bkt = hermes->GetBucket("hello");
  hapi::Blob blob(1024);
  hermes::BlobId blob_id;
  hapi::Context ctx;
  bkt->Put("0", blob, blob_id, ctx);
  bkt->RenameBlob(blob_id, "1", ctx);

  {
    hermes::BlobId blob_get_id;
    bkt->GetBlobId("0", blob_get_id);
    REQUIRE(blob_get_id.IsNull());
  }

  {
    hermes::BlobId blob_get_id;
    bkt->GetBlobId("1", blob_get_id);
    REQUIRE(!blob_get_id.IsNull());
    REQUIRE(blob_get_id == blob_id);
  }
}

void TestBlobDestroy(hapi::Hermes *hermes) {
  auto bkt = hermes->GetBucket("hello");
  hapi::Blob blob(1024);
  hermes::BlobId blob_id;
  hapi::Context ctx;
  bkt->Put("0", blob, blob_id, ctx);
  bkt->DestroyBlob(blob_id, ctx);
  {
    hermes::BlobId blob_id_get;
    bkt->GetBlobId("0", blob_id_get);
    REQUIRE(blob_id_get.IsNull());
  }
}

TEST_CASE("TestCreateBlobName") {
  hermes::TagId bkt_id(1, 1);
  std::string blob_name("0");
  hipc::uptr<hipc::charbuf> n1 =
      hermes::MetadataManager::CreateBlobName(bkt_id, blob_name);
  hipc::uptr<hipc::charbuf> n2 =
      hermes::MetadataManager::CreateBlobName(bkt_id, blob_name);
  REQUIRE(*n1 == *n2);
}

TEST_CASE("TestBlobCreates") {
  TestBlobCreates(HERMES);
}

TEST_CASE("TestBlobOverride") {
  TestBlobOverride(HERMES);
}

TEST_CASE("TestBucketRename") {
  TestBucketRename(HERMES);
}

TEST_CASE("TestBucketClear") {
  TestBucketClear(HERMES);
}

TEST_CASE("TestBucketDestroy") {
  TestBucketDestroy(HERMES);
}

TEST_CASE("TestBlobRename") {
  TestBlobRename(HERMES);
}

TEST_CASE("TestBlobDestroy") {
  TestBlobDestroy(HERMES);
}
