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

void TestPartialPut(std::shared_ptr<hapi::Bucket> &bkt,
                    int blob_name_num,
                    size_t large_size,
                    size_t small_size,
                    size_t off) {
  // Create a 1MB blob
  std::string blob_name = std::to_string(blob_name_num);
  hapi::Context ctx;
  hermes::BlobId blob_id;
  hapi::Blob write_blob(large_size);
  memset(write_blob.data(), 10, write_blob.size());
  bkt->Put(blob_name, write_blob, blob_id, ctx);
  REQUIRE(!blob_id.IsNull());

  // Put the first 64KB of the blob
  hapi::Blob small_blob(small_size);
  hermes::BlobId new_id;
  IoStatus status;
  hermes::adapter::IoClientContext opts;
  memset(small_blob.data(), 5, small_blob.size());
  opts.backend_size_ = write_blob.size();
  bkt->PartialPutOrCreate(blob_name,
                          small_blob,
                          off,
                          new_id,
                          status,
                          opts,
                          ctx);
  REQUIRE(new_id == blob_id);

  // Get the 1MB blob
  hapi::Blob read_blob;
  bkt->Get(blob_id, read_blob, ctx);

  // Verify the contents
  size_t blob_off = 0;
  REQUIRE(VerifyBuffer(read_blob.data() + blob_off,
                       off,
                       10));
  blob_off += off;
  REQUIRE(VerifyBuffer(read_blob.data() + blob_off,
                       small_blob.size(),
                       5));
  blob_off += small_blob.size();
  REQUIRE(VerifyBuffer(read_blob.data() + blob_off,
                       write_blob.size() - blob_off,
                       10));
}

TEST_CASE("TestPartial") {
  auto bkt = HERMES->GetBucket("test_partial_put_full");

  SECTION("Modify subsets of large blob") {
    TestPartialPut(bkt, 0, MEGABYTES(1), MEGABYTES(1), 0);
    TestPartialPut(bkt, 0, MEGABYTES(1), KILOBYTES(64), 0);
    TestPartialPut(bkt, 1, MEGABYTES(1), KILOBYTES(64), 10);
    TestPartialPut(bkt, 2, MEGABYTES(1), KILOBYTES(64), 30);
    TestPartialPut(bkt, 3, MEGABYTES(1), KILOBYTES(64), 500);
    TestPartialPut(bkt, 4, MEGABYTES(1), KILOBYTES(64), KILOBYTES(64));
    TestPartialPut(bkt, 4, MEGABYTES(1),
                   MEGABYTES(1) - KILOBYTES(64),
                   KILOBYTES(64));
  }
}
