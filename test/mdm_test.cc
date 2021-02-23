/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Distributed under BSD 3-Clause license.                                   *
* Copyright by The HDF Group.                                               *
* Copyright by the Illinois Institute of Technology.                        *
* All rights reserved.                                                      *
*                                                                           *
* This file is part of Hermes. The full Hermes copyright notice, including  *
* terms governing use, modification, and redistribution, is contained in    *
* the COPYFILE, which can be found at the top directory. If you do not have *
* access to either file, you may request a copy from help@hdfgroup.org.     *
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#include <string>

#include <mpi.h>

#include "hermes.h"
#include "bucket.h"
#include "vbucket.h"
#include "metadata_management_internal.h"
#include "test_utils.h"

using namespace hermes;  // NOLINT(*)
namespace hapi = hermes::api;
using HermesPtr = std::shared_ptr<hapi::Hermes>;

static void TestNullIds() {
  BucketID bkt_id = {};
  VBucketID vbkt_id = {};
  BlobID blob_id = {};

  Assert(IsNullBucketId(bkt_id));
  Assert(IsNullVBucketId(vbkt_id));
  Assert(IsNullBlobId(blob_id));
}

static void TestGetMapMutex() {
  MetadataManager mdm = {};

  for (int i = 0; i < kMapType_Count; ++i) {
    Assert(GetMapMutex(&mdm, (MapType)i));
  }
}

static void TestLocalGetNextFreeBucketId(HermesPtr hermes) {
  // NOTE(chogan): Test that the app doesn't fail when creating more buckets
  // than the maximum allowed by the configuration.

  hapi::Context ctx;
  MetadataManager *mdm = GetMetadataManagerFromContext(&hermes->context_);

  for (u32 i = 0; i < mdm->max_buckets; ++i) {
    std::string bucket_name = "bucket" + std::to_string(i);
    hapi::Bucket bucket(bucket_name, hermes, ctx);
    bucket.Close(ctx);
  }
  
  try {
    std::string fail_name = "this_should_fail";
    hapi::Bucket bucket(fail_name, hermes, ctx);
  }
  catch (const std::runtime_error& e) {
    std::cout << "Standard exception: " << e.what() << std::endl;
  }
  catch (const std::length_error& e) {
    std::cout << "Standard exception: " << e.what() << std::endl;
  }

  for (u32 i = 0; i < mdm->max_buckets; ++i) {
    std::string name = "bucket" + std::to_string(i);
    hapi::Bucket bucket(name, hermes, ctx);
    bucket.Destroy(ctx);
  }
}

static void TestGetOrCreateBucketId(HermesPtr hermes) {
  // NOTE(chogan): Create a bucket, close it, then open it again and ensure the
  // IDs are the same.
  hapi::Context ctx;
  std::string bucket_name = "bucket";
  hapi::Bucket new_bucket(bucket_name, hermes, ctx);
  u64 id = new_bucket.GetId();
  new_bucket.Close(ctx);

  hapi::Bucket existing_bucket(bucket_name, hermes, ctx);
  Assert(existing_bucket.GetId() == id);
  existing_bucket.Destroy(ctx);
}

static void TestRenameBlob(HermesPtr hermes) {
  hapi::Context ctx;
  std::string bucket_name = "rename_blob_test";
  hapi::Bucket bucket(bucket_name, hermes, ctx);

  u8 data[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  static_assert(sizeof(data[0]) == sizeof(u8));

  std::string old_blob_name = "old_blob_name";
  Assert(bucket.Put(old_blob_name, data, sizeof(data), ctx).Succeeded());

  std::string new_blob_name = "new_blob_name";
  bucket.RenameBlob(old_blob_name, new_blob_name, ctx);
  Assert(!bucket.ContainsBlob(old_blob_name));

  size_t blob_size = bucket.GetBlobSize(&hermes->trans_arena_, new_blob_name,
                                        ctx);
  Assert(blob_size == sizeof(data));

  hapi::Blob retrieved_data(blob_size);
  bucket.Get(new_blob_name, retrieved_data, ctx);

  for (size_t i = 0; i < retrieved_data.size(); ++i) {
    Assert(retrieved_data[i] == data[i]);
  }
  bucket.Destroy(ctx);
}

static void TestRenameBucket(HermesPtr hermes) {
  hapi::Context ctx;
  std::string old_bucket_name = "old_bucket";
  hapi::Bucket bucket(old_bucket_name, hermes, ctx);

  u8 data[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  static_assert(sizeof(data[0]) == sizeof(u8));
  std::string blob_name = "renamed_bucket_blob";
  bucket.Put(blob_name, data, sizeof(data), ctx);

  std::string new_bucket_name = "new_bucket";
  bucket.Rename(new_bucket_name, ctx);
  bucket.Close(ctx);

  hapi::Bucket renamed_bucket(new_bucket_name, hermes, ctx);
  size_t blob_size = renamed_bucket.GetBlobSize(&hermes->trans_arena_,
                                                blob_name, ctx);
  Assert(blob_size == sizeof(data));

  hapi::Blob retrieved_data(blob_size);
  renamed_bucket.Get(blob_name, retrieved_data, ctx);

  for (size_t i = 0; i < retrieved_data.size(); ++i) {
    Assert(retrieved_data[i] == data[i]);
  }
  renamed_bucket.Destroy(ctx);
}

static void TestBucketRefCounting(HermesPtr hermes) {
  hapi::Context ctx;
  std::string name = "refcounted_bucket";

  hapi::Bucket bucket1(name, hermes, ctx);
  hapi::Bucket bucket2(name, hermes, ctx);

  // Refcount of "refcounted_bucket" is 2
  bucket1.Destroy(ctx);

  // Bucket should not have been destroyed
  Assert(bucket1.IsValid());

  bucket1.Close(ctx);
  // Refcount is 1

  bucket2.Destroy(ctx);

  Assert(!bucket2.IsValid());
  Assert(!bucket1.IsValid());
}

static void TestMaxNameLength(HermesPtr hermes) {
  // Bucket with a name that's too large is invalid.
  hapi::Context ctx;
  try {
    std::string long_bucket_name(kMaxBucketNameSize + 1, 'x');
    hapi::Bucket invalid_bucket(long_bucket_name, hermes, ctx);
  }
  catch (const std::runtime_error& e) {
    std::cout << "Standard exception: " << e.what() << std::endl;
  }
  catch (const std::length_error& e) {
    std::cout << "Standard exception: " << e.what() << std::endl;
  }

  // Put fails when a blob name is too long
  try {
    std::string name = "b1";
    std::string long_blob_name(kMaxBlobNameSize + 1, 'x');
    hapi::Bucket bucket(name, hermes, ctx);
    hapi::Blob blob('x');
    Status status = bucket.Put(long_blob_name, blob, ctx);
    Assert(!status.Succeeded());
    Assert(!bucket.ContainsBlob(long_blob_name));

    // Vector Put fails if one name is too long
    std::string a = "a";
    std::string b = "b";
    std::string c = "c";
    std::vector<std::string> blob_names = {a, b, long_blob_name, c};
    std::vector<hapi::Blob> blobs = {blob, blob, blob, blob};
    status = bucket.Put(blob_names, blobs, ctx);
    Assert(!status.Succeeded());
    Assert(!bucket.ContainsBlob(long_blob_name));
    Assert(!bucket.ContainsBlob(a));
    Assert(!bucket.ContainsBlob(b));
    Assert(!bucket.ContainsBlob(c));

    bucket.Destroy(ctx);
  }
  catch (const std::runtime_error& e) {
    std::cout << "Standard exception: " << e.what() << std::endl;
  }
  catch (const std::length_error& e) {
    std::cout << "Standard exception: " << e.what() << std::endl;
  }
}

int main(int argc, char **argv) {
  try {
    int mpi_threads_provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
    if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
      fprintf(stderr, R"(Didn't receive appropriate MPI threading \
              specification\n)");
      return 1;
    }

    HermesPtr hermes = hapi::InitHermes(NULL, true);

    TestNullIds();
    TestGetMapMutex();
    TestLocalGetNextFreeBucketId(hermes);
    TestGetOrCreateBucketId(hermes);
    TestRenameBlob(hermes);
    TestRenameBucket(hermes);
    TestBucketRefCounting(hermes);
    TestMaxNameLength(hermes);

    hermes->Finalize(true);

    MPI_Finalize();
  }
  catch (const std::runtime_error& e) {
    std::cout << "Standard exception: " << e.what() << std::endl;
  }
  catch (const std::length_error& e) {
    std::cout << "Standard exception: " << e.what() << std::endl;
  }
  catch ( ... ) {
    std::cout << "Catch exception\n";
  }

  return 0;
}
