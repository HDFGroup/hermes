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

// TODO(chogan): Need to spawn a process and check its exit code.
#if 0
static void TestMetadataArenaErrorHandler() {
}
#endif

// TODO(chogan): VBucket functionality not implemented yet
#if 0
static void TestGetVBucketIdByName(HermesPtr hermes) {
  std::string vbucket_name("my_vbucket");
  hapi::VBucket vbucket(vbucket_name, hermes);
  VBucketID id = GetVBucketIdByName(&hermes->context_, &hermes->rpc_,
                                    vbucket_name.c_str());
  Assert(!IsNullVBucketId(id));
}

static void TestPutVBucketId() {
}

static void TestGetVBucketInfoByIndex() {
}
#endif

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

  std::string fail_name = "this_should_fail";
  hapi::Bucket bucket(fail_name, hermes, ctx);
  Assert(!bucket.IsValid());

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
  BucketID id = new_bucket.GetId();
  new_bucket.Close(ctx);

  hapi::Bucket existing_bucket(bucket_name, hermes, ctx);
  Assert(existing_bucket.GetId().as_int == id.as_int);
  existing_bucket.Destroy(ctx);
}

static void TestRenameBlob(HermesPtr hermes) {
  hapi::Context ctx;
  std::string bucket_name = "rename_blob_test";
  hapi::Bucket bucket(bucket_name, hermes, ctx);

  u8 data[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  static_assert(sizeof(data[0]) == sizeof(u8));

  std::string old_blob_name = "old_blob_name";
  Assert(bucket.Put(old_blob_name, data, sizeof(data), ctx) == 0);

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

#if 0
static void TestLocalIncrementRefcount() {
}
static void TestIncrementRefcount() {
}
static void TestLocalDecrementRefcount() {
}
static void TestDecrementRefcount() {
}
static void TestGetRemainingCapacity() {
}
static void TestGetLocalSystemViewState() {
}
static void TestVecToSwapBlob() {
}
#endif

int main(int argc, char **argv) {
  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  HermesPtr hermes = hapi::InitHermes(NULL, true);

  TestNullIds();
  TestGetMapMutex();
  // TestMetadataArenaErrorHandler();
  // TestGetVBucketIdByName(hermes);
  // TestPutVBucketId();
  // TestGetVBucketInfoByIndex();
  TestLocalGetNextFreeBucketId(hermes);
  TestGetOrCreateBucketId(hermes);
  TestRenameBlob(hermes);
  TestRenameBucket(hermes);
  // Assert(TestLocalIncrementRefcount());
  // Assert(TestIncrementRefcount());
  // Assert(TestLocalDecrementRefcount());
  // Assert(TestDecrementRefcount());
  // Assert(TestGetRemainingCapacity());
  // Assert(TestGetLocalSystemViewState());
  // Assert(TestVecToSwapBlob());

  hermes->Finalize(true);

  MPI_Finalize();

  return 0;
}
