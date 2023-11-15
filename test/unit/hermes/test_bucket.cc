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

#include "basic_test.h"
#include "hrun/api/hrun_client.h"
#include "hrun_admin/hrun_admin.h"
#include "hermes/hermes.h"
#include "hermes/bucket.h"
#include "data_stager/factory/binary_stager.h"
#include <mpi.h>

TEST_CASE("TestHermesConnect") {
  int rank, nprocs;
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  HERMES->ClientInit();
  MPI_Barrier(MPI_COMM_WORLD);
}

TEST_CASE("TestHermesPut1n") {
  int rank, nprocs;
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  // Initialize Hermes on all nodes
  HERMES->ClientInit();

  if (rank == 0) {
    // Create a bucket
    hermes::Context ctx;
    hermes::Bucket bkt("hello");

    size_t count_per_proc = 16;
    size_t off = rank * count_per_proc;
    size_t max_blobs = 4;
    size_t proc_count = off + count_per_proc;
    for (size_t i = off; i < proc_count; ++i) {
      HILOG(kInfo, "Iteration: {}", i);
      // Put a blob
      hermes::Blob blob(KILOBYTES(4));
      memset(blob.data(), i % 256, blob.size());
      hermes::BlobId blob_id =
          bkt.Put(std::to_string(i % max_blobs), blob, ctx);

      // Get a blob
      HILOG(kInfo, "Put {} returned successfully", i);
      size_t size = bkt.GetBlobSize(blob_id);
      REQUIRE(blob.size() == size);
    }
  }
  MPI_Barrier(MPI_COMM_WORLD);
}

TEST_CASE("TestHermesPut") {
  int rank, nprocs;
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  // Initialize Hermes on all nodes
  HERMES->ClientInit();

  // Create a bucket
  hermes::Context ctx;
  hermes::Bucket bkt("hello");

  size_t count_per_proc = 16;
  size_t off = rank * count_per_proc;
  size_t proc_count = off + count_per_proc;
  for (size_t i = off; i < proc_count; ++i) {
    HILOG(kInfo, "Iteration: {}", i);
    // Put a blob
    hermes::Blob blob(KILOBYTES(4));
    memset(blob.data(), i % 256, blob.size());
    hermes::BlobId blob_id = bkt.Put(std::to_string(i), blob, ctx);

    // Get a blob
    HILOG(kInfo, "Put {} returned successfully", i);
    size_t size = bkt.GetBlobSize(blob_id);
    REQUIRE(blob.size() == size);
  }
  MPI_Barrier(MPI_COMM_WORLD);
}

TEST_CASE("TestHermesAsyncPut") {
  int rank, nprocs;
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  // Initialize Hermes on all nodes
  HERMES->ClientInit();

  // Create a bucket
  hermes::Context ctx;
  hermes::Bucket bkt("hello");

  size_t count_per_proc = 256;
  size_t off = rank * count_per_proc;
  size_t proc_count = off + count_per_proc;
  for (size_t i = off; i < proc_count; ++i) {
    HILOG(kInfo, "Iteration: {}", i);
    // Put a blob
    hermes::Blob blob(MEGABYTES(1));
    memset(blob.data(), i % 256, blob.size());
    bkt.AsyncPut(std::to_string(i), blob, ctx);
  }
  MPI_Barrier(MPI_COMM_WORLD);
  HRUN_ADMIN->FlushRoot(DomainId::GetGlobal());
}

TEST_CASE("TestHermesAsyncPutLocalFlush") {
  int rank, nprocs;
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  // Initialize Hermes on all nodes
  HERMES->ClientInit();

  // Create a bucket
  hermes::Context ctx;
  hermes::Bucket bkt("hello");

  size_t count_per_proc = 256;
  size_t off = rank * count_per_proc;
  size_t proc_count = off + count_per_proc;
  for (size_t i = off; i < proc_count; ++i) {
    HILOG(kInfo, "Iteration: {}", i);
    // Put a blob
    hermes::Blob blob(MEGABYTES(1));
    memset(blob.data(), i % 256, blob.size());
    bkt.AsyncPut(std::to_string(i), blob, ctx);
  }
  MPI_Barrier(MPI_COMM_WORLD);
  if (rank == 0) {
    HRUN_ADMIN->FlushRoot(DomainId::GetLocal());
  }
  MPI_Barrier(MPI_COMM_WORLD);
}

TEST_CASE("TestHermesPutGet") {
  int rank, nprocs;
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  // Initialize Hermes on all nodes
  HERMES->ClientInit();

  // Create a bucket
  HILOG(kInfo, "WE ARE HERE!!!")
  hermes::Context ctx;
  hermes::Bucket bkt("hello");
  HILOG(kInfo, "BUCKET LOADED!!!")

  size_t count_per_proc = 16;
  size_t off = rank * count_per_proc;
  size_t proc_count = off + count_per_proc;
  for (int rep = 0; rep < 4; ++rep) {
    for (size_t i = off; i < proc_count; ++i) {
      HILOG(kInfo, "Iteration: {} with blob name {}", i, std::to_string(i));
      // Put a blob
      hermes::Blob blob(MEGABYTES(1));
      memset(blob.data(), i % 256, blob.size());
      hermes::BlobId blob_id = bkt.Put(std::to_string(i), blob, ctx);
      HILOG(kInfo, "(iteration {}) Using BlobID: {}", i, blob_id);
      // Get a blob
      hermes::Blob blob2;
      bkt.Get(blob_id, blob2, ctx);
      REQUIRE(blob.size() == blob2.size());
      REQUIRE(blob == blob2);
    }
    sleep(5);
  }
}

TEST_CASE("TestHermesPartialPutGet") {
  int rank, nprocs;
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  // Initialize Hermes on all nodes
  HERMES->ClientInit();

  // Create a bucket
  HILOG(kInfo, "WE ARE HERE!!!")
  hermes::Context ctx;
  hermes::Bucket bkt("hello");
  HILOG(kInfo, "BUCKET LOADED!!!")

  size_t count_per_proc = 16;
  size_t off = rank * count_per_proc;
  size_t proc_count = off + count_per_proc;
  size_t half_blob = KILOBYTES(4);
  for (size_t i = off; i < proc_count; ++i) {
    HILOG(kInfo, "Iteration: {}", i);
    // Make left and right blob
    hermes::Blob lblob(half_blob);
    memset(lblob.data(), i % 256, lblob.size());
    hermes::Blob rblob(half_blob);
    memset(rblob.data(), (i + 1) % 256, rblob.size());

    // PartialPut a blob
    hermes::BlobId lblob_id =
        bkt.PartialPut(std::to_string(i), lblob, 0, ctx);
    hermes::BlobId rblob_id =
        bkt.PartialPut(std::to_string(i), rblob, half_blob, ctx);
    REQUIRE(lblob_id == rblob_id);

    // PartialGet a blob
    hermes::Blob lblob2(half_blob);
    hermes::Blob rblob2(half_blob);
    bkt.PartialGet(lblob_id, lblob2, 0, ctx);
    bkt.PartialGet(rblob_id, rblob2, half_blob, ctx);
    REQUIRE(lblob2.size() == half_blob);
    REQUIRE(rblob2.size() == half_blob);
    REQUIRE(lblob == lblob2);
    REQUIRE(rblob == rblob2);
    REQUIRE(lblob2 != rblob2);
  }
}

TEST_CASE("TestHermesSerializedPutGet") {
  int rank, nprocs;
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  // Initialize Hermes on all nodes
  HERMES->ClientInit();

  // Create a bucket
  hermes::Context ctx;
  hermes::Bucket bkt("hello");

  size_t count_per_proc = 4;
  size_t off = rank * count_per_proc;
  size_t proc_count = off + count_per_proc;
  for (int rep = 0; rep < 4; ++rep) {
    for (size_t i = off; i < proc_count; ++i) {
      HILOG(kInfo, "Iteration: {} with blob name {}", i, std::to_string(i));
      // Put a blob
      std::vector<int> data(1024, i);
      hermes::BlobId blob_id = bkt.Put<std::vector<int>>(
          std::to_string(i), data, ctx);
      HILOG(kInfo, "(iteration {}) Using BlobID: {}", i, blob_id);
      // Get a blob
      std::vector<int> data2(1024, i);
      bkt.Get<std::vector<int>>(blob_id, data2, ctx);
      REQUIRE(data == data2);
    }
  }
}

TEST_CASE("TestHermesBlobDestroy") {
  int rank, nprocs;
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  // Initialize Hermes on all nodes
  HERMES->ClientInit();

  // Create a bucket
  hermes::Context ctx;
  hermes::Bucket bkt("hello");

  size_t count_per_proc = 16;
  size_t off = rank * count_per_proc;
  size_t proc_count = off + count_per_proc;
  for (size_t i = off; i < proc_count; ++i) {
    HILOG(kInfo, "Iteration: {}", i);
    // Put a blob
    hermes::Blob blob(KILOBYTES(4));
    memset(blob.data(), i % 256, blob.size());
    hermes::BlobId blob_id = bkt.Put(std::to_string(i), blob, ctx);
    bkt.DestroyBlob(blob_id, ctx);
    REQUIRE(!bkt.ContainsBlob(std::to_string(i)));
  }
}

TEST_CASE("TestHermesBucketDestroy") {
  // TODO(llogan): need to inform bucket when a blob has been placed in it
  int rank, nprocs;
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  // Initialize Hermes on all nodes
  HERMES->ClientInit();

  // Create a bucket
  hermes::Context ctx;
  hermes::Bucket bkt("hello");

  size_t count_per_proc = 16;
  size_t off = rank * count_per_proc;
  size_t proc_count = off + count_per_proc;
  for (size_t i = off; i < proc_count; ++i) {
    HILOG(kInfo, "Iteration: {}", i);
    // Put a blob
    hermes::Blob blob(KILOBYTES(4));
    memset(blob.data(), i % 256, blob.size());
    bkt.Put(std::to_string(i), blob, ctx);
  }

  bkt.Destroy();
  MPI_Barrier(MPI_COMM_WORLD);

  for (size_t i = off; i < proc_count; ++i) {
    REQUIRE(!bkt.ContainsBlob(std::to_string(i)));
  }
}

TEST_CASE("TestHermesReorganizeBlob") {
  int rank, nprocs;
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  // Initialize Hermes on all nodes
  HERMES->ClientInit();

  // Create a bucket
  hermes::Context ctx;
  hermes::Bucket bkt("hello");

  size_t count_per_proc = 16;
  size_t off = rank * count_per_proc;
  size_t proc_count = off + count_per_proc;
  for (size_t i = off; i < proc_count; ++i) {
    HILOG(kInfo, "Iteration: {}", i);
    // Put a blob
    hermes::Blob blob(KILOBYTES(4));
    memset(blob.data(), i % 256, blob.size());
    hermes::BlobId blob_id = bkt.Put(std::to_string(i), blob, ctx);
    bkt.ReorganizeBlob(blob_id, .5, 0, ctx);
    hermes::Blob blob2;
    bkt.Get(blob_id, blob2, ctx);
    REQUIRE(blob.size() == blob2.size());
    REQUIRE(blob == blob2);
  }

  for (size_t i = off; i < proc_count; ++i) {
    HILOG(kInfo, "ContainsBlob Iteration: {}", i);
    REQUIRE(bkt.ContainsBlob(std::to_string(i)));
  }

  MPI_Barrier(MPI_COMM_WORLD);
}

TEST_CASE("TestHermesBucketAppend") {
  int rank, nprocs;
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  // Initialize Hermes on all nodes
  HERMES->ClientInit();

  // Create a bucket
  hermes::Context ctx;
  hermes::Bucket bkt("append_test");

  // Put a few blobs in the bucket
  size_t page_size = KILOBYTES(4);
  size_t count_per_proc = 16;
  size_t off = rank * count_per_proc;
  size_t proc_count = off + count_per_proc;
  for (size_t i = off; i < proc_count; ++i) {
    HILOG(kInfo, "Iteration: {}", i);
    // Put a blob
    hermes::Blob blob(KILOBYTES(4));
    memset(blob.data(), i % 256, blob.size());
    bkt.Append(blob, page_size, ctx);
  }
  MPI_Barrier(MPI_COMM_WORLD);
  for (size_t i = off; i < proc_count; ++i) {
    HILOG(kInfo, "ContainsBlob Iteration: {}", i);
    REQUIRE(bkt.ContainsBlob(std::to_string(i)));
    HILOG(kInfo, "ContainsBlob Iteration: {} SUCCESS", i);
  }
  // REQUIRE(bkt.GetSize() == count_per_proc * nprocs * page_size);
  MPI_Barrier(MPI_COMM_WORLD);
}

TEST_CASE("TestHermesBucketAppend1n") {
  int rank, nprocs;
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  if (rank == 0) {
    // Initialize Hermes on all nodes
    HERMES->ClientInit();

    // Create a bucket
    hermes::Context ctx;
    hermes::Bucket bkt("append_test");

    // Put a few blobs in the bucket
    size_t page_size = KILOBYTES(4);
    size_t count_per_proc = 16;
    size_t off = rank * count_per_proc;
    size_t proc_count = off + count_per_proc;
    for (size_t i = off; i < proc_count; ++i) {
      HILOG(kInfo, "Iteration: {}", i);
      // Put a blob
      hermes::Blob blob(KILOBYTES(4));
      memset(blob.data(), i % 256, blob.size());
      bkt.Append(blob, page_size, ctx);
      hermes::Blob blob2;
      bkt.Get(std::to_string(i), blob2, ctx);
      REQUIRE(blob.size() == blob2.size());
      REQUIRE(blob == blob2);
    }
    for (size_t i = off; i < proc_count; ++i) {
      HILOG(kInfo, "ContainsBlob Iteration: {}", i);
      REQUIRE(bkt.ContainsBlob(std::to_string(i)));
    }
  }
  MPI_Barrier(MPI_COMM_WORLD);
}

TEST_CASE("TestHermesMultiGetBucket") {
  int rank, nprocs;
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  // Initialize Hermes on all nodes
  HERMES->ClientInit();

  // Create a bucket
  hermes::Context ctx;
  hermes::Bucket bkt("append_test" + std::to_string(rank));
  u32 num_blobs = 1024;

  // Put a few blobs in the bucket
  for (int i = 0; i < num_blobs; ++i) {
    hermes::Blob blob(KILOBYTES(4));
    memset(blob.data(), i % 256, blob.size());
    hermes::BlobId blob_id = bkt.Put(std::to_string(i), blob, ctx);
    HILOG(kInfo, "(iteration {}) Using BlobID: {}", i, blob_id);
    // Get a blob
    hermes::Blob blob2;
    bkt.Get(blob_id, blob2, ctx);
    REQUIRE(blob.size() == blob2.size());
    REQUIRE(blob == blob2);
  }

  // Get contained blob ids
  MPI_Barrier(MPI_COMM_WORLD);
}

TEST_CASE("TestHermesGetContainedBlobIds") {
  int rank, nprocs;
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  // Initialize Hermes on all nodes
  HERMES->ClientInit();

  // Create a bucket
  hermes::Context ctx;
  hermes::Bucket bkt("append_test" + std::to_string(rank));
  u32 num_blobs = 1024;

  // Put a few blobs in the bucket
  for (int i = 0; i < num_blobs; ++i) {
    hermes::Blob blob(KILOBYTES(4));
    memset(blob.data(), i % 256, blob.size());
    hermes::BlobId blob_id = bkt.Put(std::to_string(i), blob, ctx);
    HILOG(kInfo, "(iteration {}) Using BlobID: {}", i, blob_id);
  }
  MPI_Barrier(MPI_COMM_WORLD);

  // Get contained blob ids
  std::vector<hermes::BlobId> blob_ids;
  blob_ids = bkt.GetContainedBlobIds();
  REQUIRE(blob_ids.size() == num_blobs);
  MPI_Barrier(MPI_COMM_WORLD);
}

TEST_CASE("TestHermesDataStager") {
  int rank, nprocs;
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  // create dataset
  std::string home_dir = getenv("HOME");
  std::string path = home_dir + "/test.txt";
  size_t count_per_proc = 16;
  size_t off = rank * count_per_proc;
  size_t proc_count = off + count_per_proc;
  size_t page_size = KILOBYTES(4);
  size_t file_size = nprocs * page_size * 16;
  std::vector<char> data(file_size, 0);
  if (rank == 0) {
    FILE *file = fopen(path.c_str(), "w");
    fwrite(data.data(), sizeof(char), data.size(), file);
    fclose(file);
  }
  MPI_Barrier(MPI_COMM_WORLD);

  // Initialize Hermes on all nodes
  HERMES->ClientInit();

  // Create a stageable bucket
  using hermes::data_stager::BinaryFileStager;
  hermes::Context ctx;
  ctx.flags_.SetBits(HERMES_IS_FILE);
  hshm::charbuf url =
      BinaryFileStager::BuildFileUrl(path, page_size);
  hermes::Bucket bkt(url.str(), file_size, HERMES_IS_FILE);

  // Put a few blobs in the bucket
  for (size_t i = off; i < proc_count; ++i) {
    HILOG(kInfo, "Iteration: {}", i);
    // Put a blob
    hermes::Blob blob(page_size / 2);
    memset(blob.data(), i % 256, blob.size());
    hshm::charbuf blob_name = hermes::adapter::BlobPlacement::CreateBlobName(i);
    bkt.PartialPut(blob_name.str(), blob, 0, ctx);
    hermes::Blob blob2;
    bkt.Get(blob_name.str(), blob2, ctx);
    REQUIRE(blob2.size() == page_size);
    hermes::Blob full_blob(page_size);
    memcpy(full_blob.data(), blob.data(), blob.size());
    memcpy(full_blob.data() + blob.size(), data.data(), page_size / 2);
    REQUIRE(full_blob == blob2);
  }
  for (size_t i = off; i < proc_count; ++i) {
    hshm::charbuf blob_name = hermes::adapter::BlobPlacement::CreateBlobName(i);
    HILOG(kInfo, "ContainsBlob Iteration: {}", i);
    REQUIRE(bkt.ContainsBlob(blob_name.str()));
  }
  MPI_Barrier(MPI_COMM_WORLD);

  // Verify staging happened
  HRUN_ADMIN->FlushRoot(DomainId::GetGlobal());
  HILOG(kInfo, "Flushing finished")
}

TEST_CASE("TestHermesDataOp") {
  int rank, nprocs;
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  // Initialize Hermes on all nodes
  HERMES->ClientInit();

  // Create a bucket that supports derived quantities
  using hermes::data_stager::BinaryFileStager;
  hermes::Context ctx;
  ctx.flags_.SetBits(HERMES_HAS_DERIVED);
  std::string url = "data_bkt";
  hermes::Bucket bkt(url, 0, HERMES_HAS_DERIVED);

  // Create the derived quantity with
  hermes::data_op::OpGraph op_graph;
  hermes::data_op::Op op;
  op.in_.emplace_back(url);
  op.var_name_.url_ = url + "_min";
  op.op_name_ = "min";
  op_graph.ops_.emplace_back(op);
  HERMES->RegisterOp(op_graph);

  size_t count_per_proc = 256;
  size_t off = rank * count_per_proc;
  size_t proc_count = off + count_per_proc;
  size_t page_size = MEGABYTES(1);

  HILOG(kInfo, "GENERATING VALUES BETWEEN 5 and 261");

  // Put a few blobs in the bucket
  for (size_t i = off; i < proc_count; ++i) {
    HILOG(kInfo, "Iteration: {}", i);
    // Put a blob
    float val = 5 + i % 256;
    hermes::Blob blob(page_size);
    float *data = (float*)blob.data();
    for (size_t j = 0; j < page_size / sizeof(float); ++j) {
      data[j] = val;
    }
    memcpy(blob.data(), data, blob.size());
    std::string blob_name = std::to_string(i);
    bkt.Put(blob_name, blob, ctx);
  }
  MPI_Barrier(MPI_COMM_WORLD);

  // HRUN_ADMIN->FlushRoot(DomainId::GetGlobal());
  // Verify derived operator happens
  hermes::Bucket bkt_min("data_bkt_min", 0, 0);
  size_t size;
  do {
    size = bkt_min.GetSize();
    if (size != sizeof(float) * count_per_proc * nprocs) {
      HILOG(kInfo, "Waiting for derived data");
      sleep(1);
    } else {
      break;
    }
  } while (true);

  hermes::Blob blob2;
  bkt_min.Get(std::to_string(0), blob2, ctx);
  float min = *(float *)blob2.data();
  REQUIRE(size == sizeof(float) * count_per_proc * nprocs);
  REQUIRE(min == 5);

  HILOG(kInfo, "MINIMUM VALUE QUERY FROM EMPRESS: {}", min);
}

TEST_CASE("TestHermesCollectMetadata") {
  int rank, nprocs;
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  // Initialize Hermes on all nodes
  HERMES->ClientInit();

  // Create a bucket
  hermes::Context ctx;
  hermes::Bucket bkt("append_test" + std::to_string(rank));
  u32 num_blobs = 1024;

  // Put a few blobs in the bucket
  for (int i = 0; i < num_blobs; ++i) {
    hermes::Blob blob(KILOBYTES(4));
    memset(blob.data(), i % 256, blob.size());
    hermes::BlobId blob_id = bkt.Put(std::to_string(i), blob, ctx);
    HILOG(kInfo, "(iteration {}) Using BlobID: {}", i, blob_id);
  }
  MPI_Barrier(MPI_COMM_WORLD);

  // Get contained blob ids
  hermes::MetadataTable table = HERMES->CollectMetadataSnapshot();
  REQUIRE(table.blob_info_.size() == 1024 * nprocs);
  REQUIRE(table.bkt_info_.size() == nprocs);
  REQUIRE(table.target_info_.size() >= 4);
  MPI_Barrier(MPI_COMM_WORLD);
}

TEST_CASE("TestHermesDataPlacement") {
  int rank, nprocs;
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  // Initialize Hermes on all nodes
  HERMES->ClientInit();

  // Create a bucket
  hermes::Context ctx;
  hermes::Bucket bkt("hello");

  size_t count_per_proc = 16;
  size_t off = rank * count_per_proc;
  size_t proc_count = off + count_per_proc;

  // Put a few blobs in the bucket
  HILOG(kInfo, "Initially placing blobs")
  for (size_t i = off; i < proc_count; ++i) {
    HILOG(kInfo, "Iteration: {}", i);
    hermes::Blob blob(MEGABYTES(1));
    memset(blob.data(), i % 256, blob.size());
    bkt.AsyncPut(std::to_string(i), blob, ctx);
  }
  MPI_Barrier(MPI_COMM_WORLD);
  sleep(20);

  while (true) {
    // Demote half of blobs
    HILOG(kInfo, "Demoting blobs")
    for (size_t i = off; i < proc_count; ++i) {
      HILOG(kInfo, "Iteration: {}", i);
      hermes::BlobId blob_id = bkt.GetBlobId(std::to_string(i));
      bkt.ReorganizeBlob(blob_id, .5, 0, ctx);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    sleep(20);

    // Promote half of blobs
    HILOG(kInfo, "Promoting blobs")
    for (size_t i = off; i < proc_count; ++i) {
      HILOG(kInfo, "Iteration: {}", i);
      hermes::BlobId blob_id = bkt.GetBlobId(std::to_string(i));
      bkt.ReorganizeBlob(blob_id, 1, 0, ctx);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    sleep(20);
  }
}

TEST_CASE("TestHermesDataPlacementFancy") {
  int rank, nprocs;
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  // Initialize Hermes on all nodes
  HERMES->ClientInit();

  // Create a bucket
  hermes::Context ctx;
  hermes::Bucket bkt("hello");

  size_t count_per_proc = 16;
  size_t off = rank * count_per_proc;
  size_t proc_count = off + count_per_proc;

  // Put a few blobs in the bucket
  HILOG(kInfo, "Initially placing blobs")
  for (size_t i = off; i < proc_count; ++i) {
    HILOG(kInfo, "Iteration: {}", i);
    hermes::Blob blob(MEGABYTES(1));
    memset(blob.data(), i % 256, blob.size());
    bkt.AsyncPut(std::to_string(i), blob, ctx);
  }
  MPI_Barrier(MPI_COMM_WORLD);
  sleep(20);

  std::vector<float> scores = {
      1.0, .5, .01, 0
  };
  int count = 0;

  while (true) {
    int score_id = count % scores.size();
    // Demote half of blobs
    HILOG(kInfo, "Demoting blobs")
    for (size_t i = off; i < proc_count; ++i) {
      HILOG(kInfo, "Iteration: {}", i);
      hermes::BlobId blob_id = bkt.GetBlobId(std::to_string(i));
      bkt.ReorganizeBlob(blob_id, scores[score_id], 0, ctx);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    sleep(5);
    count += 1;
  }
}
