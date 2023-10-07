//
// Created by llogan on 7/1/23.
//

#include "basic_test.h"
#include "labstor/api/labstor_client.h"
#include "labstor_admin/labstor_admin.h"
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
      hermes::BlobId blob_id = bkt.Put(std::to_string(i % max_blobs), blob, ctx);

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

  size_t count_per_proc = 4;
  size_t off = rank * count_per_proc;
  size_t proc_count = off + count_per_proc;
  for (int rep = 0; rep < 4; ++rep) {
    for (size_t i = off; i < proc_count; ++i) {
      HILOG(kInfo, "Iteration: {} with blob name {}", i, std::to_string(i));
      // Put a blob
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
    hermes::BlobId lblob_id = bkt.PartialPut(std::to_string(i), lblob, 0, ctx);
    hermes::BlobId rblob_id = bkt.PartialPut(std::to_string(i), rblob, half_blob, ctx);
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
      hermes::BlobId blob_id = bkt.Put(std::to_string(i), data, ctx);
      HILOG(kInfo, "(iteration {}) Using BlobID: {}", i, blob_id);
      // Get a blob
      std::vector<int> data2(1024, i);
      bkt.Get(blob_id, data2, ctx);
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

TEST_CASE("TestHermesDataStager") {
  int rank, nprocs;
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  if (rank == 0) {
    std::vector<char> data(KILOBYTES(256));
    FILE *file = fopen("/tmp/test.txt", "w");
    fwrite(data.data(), sizeof(char), data.size(), file);
    fclose(file);
  }
  MPI_Barrier(MPI_COMM_WORLD);

  // Initialize Hermes on all nodes
  HERMES->ClientInit();

  // Create a stageable bucket
  size_t stage_size = KILOBYTES(256);
  size_t page_size = KILOBYTES(4);
  using hermes::data_stager::BinaryFileStager;
  hermes::Context ctx;
  ctx.flags_.SetBits(HERMES_IS_FILE);
  hshm::charbuf url = BinaryFileStager::BuildFileUrl("/tmp/test.txt", page_size);
  hermes::Bucket bkt(url.str(), stage_size, HERMES_IS_FILE);

  // Put a few blobs in the bucket
  size_t count_per_proc = 16;
  size_t off = rank * count_per_proc;
  size_t proc_count = off + count_per_proc;
  for (size_t i = off; i < proc_count; ++i) {
    HILOG(kInfo, "Iteration: {}", i);
    // Put a blob
    hermes::Blob blob(page_size);
    memset(blob.data(), i % 256, blob.size());
    bkt.Put(std::to_string(i), blob, ctx);
    hermes::Blob blob2;
    bkt.Get(std::to_string(i), blob2, ctx);
    REQUIRE(blob2.size() == stage_size);
    REQUIRE(blob == blob2);
  }
  for (size_t i = off; i < proc_count; ++i) {
    HILOG(kInfo, "ContainsBlob Iteration: {}", i);
    REQUIRE(bkt.ContainsBlob(std::to_string(i)));
  }
  MPI_Barrier(MPI_COMM_WORLD);

  // Verify staging happened
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