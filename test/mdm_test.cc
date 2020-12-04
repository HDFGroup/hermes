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

static bool TestIsNullBucketId() {
  BucketID id = {};
  bool result = IsNullBucketId(id);

  return result;
}

static bool TestIsNullVBucketId() {
  VBucketID id = {};
  bool result = IsNullVBucketId(id);

  return result;
}

static bool TestIsNullBlobId() {
  BlobID id = {};
  bool result = IsNullBlobId(id);

  return result;
}

static bool TestGetMapMutex() {
  bool result = true;
  MetadataManager mdm = {};

  for (int i = 0; i < kMapType_Count; ++i) {
    if (GetMapMutex(&mdm, (MapType)i) == 0) {
      result = false;
    }
  }

  return result;
}

// TODO(chogan): Need to spawn a process and check its exit code.
#if 0
static bool TestMetadataArenaErrorHandler() {
  bool result = false;

  return result;
}
#endif

// TODO(chogan): Not implemented yet
#if 0
static bool TestGetVBucketIdByName(HermesPtr hermes) {
  std::string vbucket_name("my_vbucket");
  hapi::VBucket vbucket(vbucket_name, hermes);
  VBucketID id = GetVBucketIdByName(&hermes->context_, &hermes->rpc_,
                                    vbucket_name.c_str());
  bool result = true;

  if (IsNullVBucketId(id)) {
    result = false;
  }

  return result;
}

static bool TestPutVBucketId() {
  bool result = false;

  return result;
}
static bool TestGetVBucketInfoByIndex() {
  
  bool result = false;

  return result;
}
#endif

static bool TestLocalGetNextFreeBucketId(HermesPtr hermes) {

  // NOTE(chogan): Test that the app doesn't fail when creating more buckets
  // than the maximum allowed by the configuration.

  hapi::Context ctx;
  MetadataManager *mdm = GetMetadataManagerFromContext(&hermes->context_);
  bool result = true;

  ScopedTemporaryMemory scratch(&hermes->trans_arena_);
  BucketID *ids = PushArray<BucketID>(scratch, mdm->max_buckets);

  for (u32 i = 0; i < mdm->max_buckets; ++i) {
    std::string bucket_name = "bucket" + std::to_string(i);
    hapi::Bucket bucket(bucket_name, hermes, ctx);
    ids[i] = bucket.GetId();
  }

  std::string fail_name = "this_should_fail";
  hapi::Bucket bucket(fail_name, hermes, ctx);

  if (bucket.IsValid()) {
    result = false;
  }

  for (u32 i = 0; i < mdm->max_buckets; ++i) {
    std::string name = "bucket" + std::to_string(i);
    LocalDestroyBucket(&hermes->context_, &hermes->rpc_, name.c_str(), ids[i]);
  }
  return result;
}

static bool TestGetOrCreateBucketId(HermesPtr hermes) {
  
  bool result = false;

  return result;
}

static bool TestGetNextFreeVBucketId() {
  
  bool result = false;

  return result;
}
static bool TestRenameBlob() {
  
  bool result = false;

  return result;
}

static bool TestLocalRenameBucket() {
  
  bool result = false;

  return result;
}

static bool TestRenameBucket() {
  
  bool result = false;

  return result;
}

static bool TestLocalIncrementRefcount() {
  bool result = false;

  return result;
}
static bool TestIncrementRefcount() {
  bool result = false;

  return result;
}
static bool TestLocalDecrementRefcount() {
  bool result = false;

  return result;
}
static bool TestDecrementRefcount() {
  bool result = false;

  return result;
}

static bool TestGetRemainingCapacity() {
  bool result = false;

  return result;
}

static bool TestGetLocalSystemViewState() {
  bool result = false;

  return result;
}

static bool TestVecToSwapBlob() {
  bool result = false;

  return result;
}

int main(int argc, char **argv) {
  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  HermesPtr hermes = hapi::InitHermes(NULL, true);

  Assert(TestIsNullBucketId());
  Assert(TestIsNullVBucketId());
  Assert(TestIsNullBlobId());
  Assert(TestGetMapMutex());
  // Assert(TestMetadataArenaErrorHandler());
  // Assert(TestGetVBucketIdByName(hermes));
  // Assert(TestPutVBucketId());
  // Assert(TestGetVBucketInfoByIndex());
  Assert(TestLocalGetNextFreeBucketId(hermes));
  Assert(TestGetOrCreateBucketId(hermes));
  Assert(TestGetNextFreeVBucketId());
  Assert(TestRenameBlob());
  Assert(TestLocalRenameBucket());
  Assert(TestRenameBucket());
  Assert(TestLocalIncrementRefcount());
  Assert(TestIncrementRefcount());
  Assert(TestLocalDecrementRefcount());
  Assert(TestDecrementRefcount());
  Assert(TestGetRemainingCapacity());
  Assert(TestGetLocalSystemViewState());
  Assert(TestVecToSwapBlob());

  hermes->Finalize(true);

  MPI_Finalize();

  return 0;
}
