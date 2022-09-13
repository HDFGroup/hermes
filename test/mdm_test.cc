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

#include <string>

#include <mpi.h>

#include "hermes.h"
#include "bucket.h"
#include "vbucket.h"
#include "buffer_pool_internal.h"
#include "metadata_management_internal.h"
#include "metadata_storage.h"
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

  MetadataManager *mdm = GetMetadataManagerFromContext(&hermes->context_);

  for (u32 i = 0; i < mdm->max_buckets; ++i) {
    std::string bucket_name = "bucket" + std::to_string(i);
    hapi::Bucket bucket(bucket_name, hermes);
  }

  std::string fail_name = "this_should_fail";
  bool threw_runtime_error = false;
  try {
    hapi::Bucket bucket(fail_name, hermes);
  }
  catch (const std::runtime_error& e) {
      threw_runtime_error = true;
  }
  Assert(threw_runtime_error);

  for (u32 i = 0; i < mdm->max_buckets; ++i) {
    std::string name = "bucket" + std::to_string(i);
    hapi::Bucket bucket(name, hermes);
    bucket.Destroy();
  }
}

static void TestGetOrCreateBucketId(HermesPtr hermes) {
  // NOTE(chogan): Create a bucket, close it, then open it again and ensure the
  // IDs are the same.
  std::string bucket_name = "bucket";
  hapi::Bucket new_bucket(bucket_name, hermes);
  u64 id = new_bucket.GetId();
  new_bucket.Release();

  hapi::Bucket existing_bucket(bucket_name, hermes);
  Assert(existing_bucket.GetId() == id);
  existing_bucket.Destroy();
}

static void TestRenameBlob(HermesPtr hermes) {
  std::string bucket_name = "rename_blob_test";
  hapi::Bucket bucket(bucket_name, hermes);

  u8 data[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  static_assert(sizeof(data[0]) == sizeof(u8));

  std::string old_blob_name = "old_blob_name";
  Assert(bucket.Put(old_blob_name, data, sizeof(data)).Succeeded());

  std::string new_blob_name = "new_blob_name";
  bucket.RenameBlob(old_blob_name, new_blob_name);
  Assert(!bucket.ContainsBlob(old_blob_name));

  hapi::Context ctx;
  size_t blob_size = bucket.GetBlobSize(&hermes->trans_arena_, new_blob_name,
                                        ctx);
  Assert(blob_size == sizeof(data));

  hapi::Blob retrieved_data(blob_size);
  bucket.Get(new_blob_name, retrieved_data);

  for (size_t i = 0; i < retrieved_data.size(); ++i) {
    Assert(retrieved_data[i] == data[i]);
  }
  bucket.Destroy();
}

static void TestRenameBucket(HermesPtr hermes) {
  std::string old_bucket_name = "old_bucket";
  hapi::Bucket bucket(old_bucket_name, hermes);

  u8 data[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  static_assert(sizeof(data[0]) == sizeof(u8));
  std::string blob_name = "renamed_bucket_blob";
  Assert(bucket.Put(blob_name, data, sizeof(data)).Succeeded());

  std::string new_bucket_name = "new_bucket";
  bucket.Rename(new_bucket_name);
  bucket.Release();

  hapi::Bucket renamed_bucket(new_bucket_name, hermes);
  hapi::Context ctx;
  size_t blob_size = renamed_bucket.GetBlobSize(&hermes->trans_arena_,
                                                blob_name, ctx);
  Assert(blob_size == sizeof(data));

  hapi::Blob retrieved_data(blob_size);
  renamed_bucket.Get(blob_name, retrieved_data);

  for (size_t i = 0; i < retrieved_data.size(); ++i) {
    Assert(retrieved_data[i] == data[i]);
  }
  renamed_bucket.Destroy();
}

static void TestBucketRefCounting(HermesPtr hermes) {
  std::string name = "refcounted_bucket";

  hapi::Bucket bucket1(name, hermes);
  hapi::Bucket bucket2(name, hermes);

  // Refcount of "refcounted_bucket" is 2
  bucket1.Destroy();

  // Bucket should not have been destroyed
  Assert(bucket1.IsValid());

  bucket1.Release();
  // Refcount is 1

  bucket2.Destroy();

  Assert(!bucket2.IsValid());
  Assert(!bucket1.IsValid());
}

static void TestMaxNameLength(HermesPtr hermes) {
  // Bucket with a name that's too large is invalid.
  std::string long_bucket_name(kMaxBucketNameSize + 1, 'x');
  bool threw_length_error = false;
  try {
    hapi::Bucket invalid_bucket(long_bucket_name, hermes);
  }
  catch (const std::length_error& e) {
    threw_length_error = true;
  }
  Assert(threw_length_error);

  // Put fails when a blob name is too long
  std::string name = "b1";
  std::string long_blob_name(kMaxBlobNameSize + 1, 'x');
  hapi::Bucket bucket(name, hermes);
  hapi::Blob blob('x');
  Status status = bucket.Put(long_blob_name, blob);
  Assert(status.Failed());
  Assert(!bucket.ContainsBlob(long_blob_name));

  // Vector Put fails if one name is too long
  std::string a = "a";
  std::string b = "b";
  std::string c = "c";
  std::vector<std::string> blob_names = {a, b, long_blob_name, c};
  std::vector<hapi::Blob> blobs = {blob, blob, blob, blob};
  status = bucket.Put(blob_names, blobs);
  Assert(status.Failed());
  Assert(!bucket.ContainsBlob(long_blob_name));
  Assert(!bucket.ContainsBlob(a));
  Assert(!bucket.ContainsBlob(b));
  Assert(!bucket.ContainsBlob(c));

  bucket.Destroy();
}

static void TestGetRelativeNodeId() {
  RpcContext rpc = {};
  rpc.num_nodes = 10;
  rpc.node_id = 1;

  Assert(GetNextNode(&rpc) == 2);
  Assert(GetPreviousNode(&rpc) == 10);

  rpc.node_id = 10;
  Assert(GetNextNode(&rpc) == 1);
  Assert(GetPreviousNode(&rpc) == 9);
}

static void TestDuplicateBlobNames(HermesPtr hermes) {
  const size_t blob_size = 8;
  hapi::Bucket b1("b1", hermes);
  hapi::Bucket b2("b2", hermes);
  std::string blob_name("duplicate");
  hapi::Blob blob1(blob_size, 'x');
  hapi::Blob blob2(blob_size, 'z');

  Assert(b1.Put(blob_name, blob1).Succeeded());
  Assert(!b2.ContainsBlob(blob_name));

  Assert(b2.Put(blob_name, blob2).Succeeded());

  Assert(b1.ContainsBlob(blob_name));
  Assert(b2.ContainsBlob(blob_name));

  hapi::Blob result(blob_size, '0');
  Assert(b1.Get(blob_name, result) == blob_size);
  Assert(result == blob1);
  Assert(b2.Get(blob_name, result) == blob_size);
  Assert(result == blob2);

  Assert(b1.Destroy().Succeeded());
  Assert(b2.Destroy().Succeeded());
}

static void TestGetBucketIdFromBlobId(HermesPtr hermes) {
  const size_t blob_size = 8;
  hapi::Bucket b1("b1", hermes);
  std::string blob_name("blob1");
  hapi::Blob blob1(blob_size, 'x');
  Assert(b1.Put(blob_name, blob1).Succeeded());

  BucketID b1_id = {};
  b1_id.as_int = b1.GetId();
  BlobID blob_id =
    hermes::GetBlobId(&hermes->context_, &hermes->rpc_, blob_name, b1_id);

  BucketID bucket_id =
    hermes::GetBucketIdFromBlobId(&hermes->context_, &hermes->rpc_, blob_id);

  Assert(bucket_id.as_int == b1.GetId());
  Assert(b1.Destroy().Succeeded());
}

static void TestHexStringToU64() {
  std::string zero1("0");
  std::string zero2 = zero1 + zero1;
  std::string zero4 = zero2 + zero2;
  std::string zero8 = zero4 + zero4;

  std::string one_str = zero8 + zero4 + zero2 + "01";
  std::string ff_str = zero8 + zero4 + zero2 + "ff";
  std::string all_f_str("ffffffffffffffff");
  std::string bucket_id_str = zero4 + zero2 + "01" + zero4 + zero2 + "0e";
  std::string count_str("123456789abcdef0");

  u64 one = 0x1ULL;
  u64 ff = 0xffULL;
  u64 bucket_id = 0x10000000eULL;
  u64 all_f = 0xffffffffffffffffULL;
  u64 count = 0x123456789abcdef0ULL;

  BucketID id = {};
  id.as_int = 1311768467463790320;
  std::string blob_name = MakeInternalBlobName(std::string("my_blob"), id);

  Assert(HexStringToU64(one_str) == one);
  Assert(HexStringToU64(ff_str) == ff);
  Assert(HexStringToU64(bucket_id_str) == bucket_id);
  Assert(HexStringToU64(all_f_str) == all_f);
  Assert(HexStringToU64(count_str) == count);
  Assert(HexStringToU64(blob_name) == count);
}

void TestSwapBlobsExistInBucket() {
  hermes::Config config = {};
  hermes::InitDefaultConfig(&config);
  const int kNumDevices = 4;
  for (int i = 0; i < kNumDevices; ++i) {
    // NOTE(chogan): Restrict buffering capacity so blobs go to swap
    config.capacities[i] = MEGABYTES(1);
  }
  std::shared_ptr<hapi::Hermes> hermes = hermes::InitHermes(&config, true);

  hermes::api::Bucket swap_bucket("swap_test", hermes);
  hermes::api::Blob blob(128*1024, 255);
  auto success_blob_names = std::vector<std::string>();
  for (int i = 0; i < 32; i++) {
    std::string blob_name = "Blob" + std::to_string(i);
    hapi::Status status = swap_bucket.Put(blob_name, blob);
    if (!status.Failed()) {
      success_blob_names.push_back(blob_name);
    }
  }
  for (const auto &blob_name : success_blob_names) {
    bool exists = swap_bucket.ContainsBlob(blob_name);
    Assert(exists);
  }

  hermes->Finalize(true);
}

static void TestBlobInfoMap() {
  HermesPtr hermes = hapi::InitHermes(NULL, true);

  const int kIters = 4;
  hapi::Bucket bkt("test_blob_info_map", hermes);

  hapi::Blob data(64, 'x');

  for (int i = 0; i < kIters; ++i) {
    std::string name = "blob_" + std::to_string(i);
    bkt.Put(name, data);
  }

  for (int i = 0; i < kIters; ++i) {
    std::string name = "blob_" + std::to_string(i);
    bkt.DeleteBlob(name);
  }

  bkt.Destroy();

  hermes->Finalize(true);
}

static void TestMdmViz() {
  using namespace hermes;  // NOLINT(*)

  Config config = {};
  InitDefaultConfig(&config);
  config.num_devices = 1;
  config.num_targets = 1;
  config.block_sizes[0] = 4096;
  config.num_slabs[0] = 1;
  config.desired_slab_percentages[0][0] = 1.0f;

  HermesPtr hermes = InitHermesDaemon(&config);
  SharedMemoryContext *context = &hermes->context_;
  MetadataManager *mdm = GetMetadataManagerFromContext(context);
  IdList ids1 = AllocateIdList(mdm, KILOBYTES(4));
  FreeIdList(mdm, ids1);
  IdList ids2 = AllocateIdList(mdm, KILOBYTES(1));
  IdList ids3 = AllocateIdList(mdm, KILOBYTES(1));
  IdList ids4 = AllocateIdList(mdm, KILOBYTES(1));
  IdList ids5 = AllocateIdList(mdm, KILOBYTES(1));
  IdList ids6 = AllocateIdList(mdm, KILOBYTES(1));
  IdList ids7 = AllocateIdList(mdm, KILOBYTES(1));

  FreeIdList(mdm, ids5);
  FreeIdList(mdm, ids6);
  FreeIdList(mdm, ids4);
  FreeIdList(mdm, ids3);
  FreeIdList(mdm, ids2);
  FreeIdList(mdm, ids7);

  std::string base_name("xxxxxxxxxxxxxxx");
  hapi::Bucket bucket(std::string(base_name + "bkt"), hermes);

  hapi::Blob data(255, 'z');

  for (int i = 0; i < 10; ++i) {
    bucket.Put(std::string(base_name + std::to_string(i)), data);
  }

  bucket.Destroy();

  hermes->Finalize(true);
}

static void TestEffectiveTarget() {
  using namespace hermes;  // NOLINT(*)

  hermes::Config config = {};
  hermes::InitDefaultConfig(&config);
  config.default_placement_policy = hapi::PlacementPolicy::kRoundRobin;
  config.default_rr_split = 0;
  HermesPtr hermes = hermes::InitHermesDaemon(&config);

  hermes::RoundRobin rr_state;
  rr_state.SetCurrentDeviceIndex(0);

  std::string bucket_name(__func__);
  hapi::Bucket bucket(bucket_name, hermes);
  hapi::Blob data(4 * 1024, 'z');
  std::string blob_name("1");
  bool success = bucket.Put(blob_name, data).Succeeded();
  Assert(success);

  SharedMemoryContext *context = &hermes->context_;
  RpcContext *rpc = &hermes->rpc_;
  MetadataManager *mdm = GetMetadataManagerFromContext(context);

  // Check BlobInfo::effective_target
  BucketID bucket_id = GetBucketId(context, rpc, bucket_name.c_str());
  BlobID blob_id = GetBlobId(context, rpc, blob_name, bucket_id, false);
  BlobInfo *info = GetBlobInfoPtr(mdm, blob_id);
  TargetID expected_target_id = {{1, 0, 0}};
  Assert(info->effective_target.as_int == expected_target_id.as_int);
  ReleaseBlobInfoPtr(mdm);

  // Check Target::effective_blobs
  Target *ram_target = GetTarget(context, 0);
  Assert(ram_target->effective_blobs.length == 1);
  u64 *ids = GetIdsPtr(mdm, ram_target->effective_blobs);
  BlobID effective_blob_id = {};
  effective_blob_id.as_int = ids[0];
  Assert(effective_blob_id.as_int == blob_id.as_int);
  ReleaseIdsPtr(mdm);

  bucket.Destroy();

  hermes->Finalize(true);
}

int main(int argc, char **argv) {
  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  HermesPtr hermes = hapi::InitHermes(NULL, true);

  HERMES_ADD_TEST(TestNullIds);
  HERMES_ADD_TEST(TestGetMapMutex);
  HERMES_ADD_TEST(TestLocalGetNextFreeBucketId, hermes);
  HERMES_ADD_TEST(TestGetOrCreateBucketId, hermes);
  HERMES_ADD_TEST(TestRenameBlob, hermes);
  HERMES_ADD_TEST(TestRenameBucket, hermes);
  HERMES_ADD_TEST(TestBucketRefCounting, hermes);
  HERMES_ADD_TEST(TestMaxNameLength, hermes);
  HERMES_ADD_TEST(TestGetRelativeNodeId);
  HERMES_ADD_TEST(TestDuplicateBlobNames, hermes);
  HERMES_ADD_TEST(TestGetBucketIdFromBlobId, hermes);
  HERMES_ADD_TEST(TestHexStringToU64);

  hermes->Finalize(true);

  HERMES_ADD_TEST(TestSwapBlobsExistInBucket);
  HERMES_ADD_TEST(TestBlobInfoMap);
  HERMES_ADD_TEST(TestMdmViz);
  HERMES_ADD_TEST(TestEffectiveTarget);

  MPI_Finalize();

  printf("SUCCESS!\n");
  return 0;
}
