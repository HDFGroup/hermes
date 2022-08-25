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

#include <numeric>

#include <mpi.h>

#include "hermes.h"
#include "vbucket.h"
#include "metadata_management_internal.h"
#include "buffer_pool_internal.h"
#include "test_utils.h"


namespace hapi = hermes::api;
using HermesPtr = std::shared_ptr<hapi::Hermes>;
using hermes::u8;
using hermes::f32;
using hermes::u64;
using hermes::SharedMemoryContext;
using hermes::RpcContext;
using hermes::BoTask;
using hermes::BufferID;
using hermes::TargetID;
using hermes::BucketID;
using hermes::BlobID;
using hermes::BufferInfo;
using hapi::VBucket;
using hapi::Bucket;

static void TestIsBoFunction() {
  using hermes::IsBoFunction;
  Assert(IsBoFunction("BO::TriggerBufferOrganizer"));
  Assert(IsBoFunction("BO::A"));
  Assert(IsBoFunction("BO::"));
  Assert(!IsBoFunction(""));
  Assert(!IsBoFunction("A"));
  Assert(!IsBoFunction("BO:"));
  Assert(!IsBoFunction("BO:A"));
  Assert(!IsBoFunction("TriggerBufferOrganizer"));
}

static void TestBackgroundFlush() {
  HermesPtr hermes = hermes::InitHermesDaemon();
  const int io_size = KILOBYTES(4);
  const int iters = 4;
  const int total_bytes = io_size * iters;
  std::string final_destination = "./test_background_flush.out";
  std::string bkt_name = "background_flush";
  hapi::Bucket bkt(bkt_name, hermes);
  hapi::VBucket vbkt(bkt_name, hermes);

  hapi::PersistTrait persist_trait(final_destination, {}, false);
  vbkt.Attach(&persist_trait);

  std::vector<u8> data(iters);
  std::iota(data.begin(), data.end(), 'a');

  for (int i = 0; i < iters; ++i) {
    hapi::Blob blob(io_size, data[i]);
    std::string blob_name = std::to_string(i);
    bkt.Put(blob_name, blob);

    persist_trait.offset_map.emplace(blob_name, i * io_size);
    vbkt.Link(blob_name, bkt_name);
  }

  vbkt.Destroy();
  bkt.Destroy();

  hermes->Finalize(true);

  // NOTE(chogan): Verify that file is on disk with correct data
  FILE *fh = fopen(final_destination.c_str(), "r");
  Assert(fh);

  std::vector<u8> result(total_bytes, 0);
  size_t bytes_read = fread(result.data(), 1, total_bytes, fh);
  Assert(bytes_read == total_bytes);

  for (int i = 0; i < iters; ++i) {
    for (int j = 0; j < io_size; ++j) {
      int index = i * io_size + j;
      Assert(result[index] == data[i]);
    }
  }

  Assert(fclose(fh) == 0);
  Assert(std::remove(final_destination.c_str()) == 0);
}

static void PutThenMove(HermesPtr hermes, bool move_up) {
  SharedMemoryContext *context = &hermes->context_;
  RpcContext *rpc = &hermes->rpc_;

  std::string bkt_name =
    "BoMove" + (move_up ? std::string("Up") : std::string("Down"));
  hapi::Bucket bkt(bkt_name, hermes);

  size_t blob_size = KILOBYTES(20);
  std::string blob_name = "1";
  hapi::Blob blob(blob_size, (move_up ? 'z' : 'a'));
  hapi::Status status = bkt.Put(blob_name, blob);
  Assert(status.Succeeded());

  BucketID bucket_id = {};
  bucket_id.as_int = bkt.GetId();
  BlobID old_blob_id = GetBlobId(context, rpc, blob_name, bucket_id, false);
  Assert(!IsNullBlobId(old_blob_id));
  std::vector<BufferID> old_buffer_ids = GetBufferIdList(context, rpc,
                                                         old_blob_id);
  Assert(old_buffer_ids.size() > 0);
  std::vector<BufferInfo> old_buffer_info = GetBufferInfo(context, rpc,
                                                          old_buffer_ids);
  Assert(old_buffer_info.size() == old_buffer_ids.size());

  f32 old_access_score = ComputeBlobAccessScore(context, old_buffer_info);
  Assert(old_access_score == (move_up ? 0 : 1));

  BufferID src = old_buffer_ids[0];
  hermes::BufferHeader *header = GetHeaderByBufferId(context, src);
  Assert(header);

  std::vector<TargetID> targets = LocalGetNodeTargets(context);
  hermes::PlacementSchema schema;
  int target_index = move_up ? 0 : (targets.size() - 1);
  schema.push_back(std::pair(header->used, targets[target_index]));

  std::vector<BufferID> destinations = hermes::GetBuffers(context, schema);
  Assert(destinations.size());

  hermes::BoMoveList moves;
  moves.push_back(std::pair(src, destinations));
  std::string internal_blob_name = MakeInternalBlobName(blob_name, bucket_id);
  hermes::BoMove(context, rpc, moves, old_blob_id, bucket_id,
                 internal_blob_name);

  BlobID new_blob_id = GetBlobId(context, rpc, blob_name, bucket_id, false);
  Assert(!IsNullBlobId(new_blob_id));
  std::vector<BufferID> new_buffer_ids = GetBufferIdList(context, rpc,
                                                         new_blob_id);
  Assert(new_buffer_ids.size() > 0);
  Assert(new_buffer_ids != old_buffer_ids);
  std::vector<BufferInfo> new_buffer_info = GetBufferInfo(context, rpc,
                                                          new_buffer_ids);
  Assert(new_buffer_info.size() == new_buffer_ids.size());
  Assert(new_buffer_info != old_buffer_info);

  f32 new_access_score = ComputeBlobAccessScore(context, new_buffer_info);
  if (move_up) {
    Assert(old_access_score < new_access_score);
  } else {
    Assert(new_access_score < old_access_score);
  }

  hapi::Blob retrieved_blob;
  size_t retrieved_blob_size = bkt.Get(blob_name, retrieved_blob);
  Assert(retrieved_blob_size == blob_size);
  retrieved_blob.resize(retrieved_blob_size);
  bkt.Get(blob_name, retrieved_blob);
  Assert(blob == retrieved_blob);

  bkt.Destroy();
}

static void TestBoMove() {
  hermes::Config config = {};
  hermes::InitDefaultConfig(&config);
  config.default_placement_policy = hapi::PlacementPolicy::kRoundRobin;
  HermesPtr hermes = hermes::InitHermesDaemon(&config);

  hermes::RoundRobinState rr_state;
  size_t num_devices = rr_state.GetNumDevices();

  // Force Blob to RAM, then call BoMove to move a Buffer to the lowest tier and
  // ensure that the access score has changed appropriately.
  rr_state.SetCurrentDeviceIndex(0);
  PutThenMove(hermes, false);

  // Force Blob to lowest Tier, then call BoMove to move a Buffer to the RAM
  // tier and ensure that the access score has changed appropriately.
  rr_state.SetCurrentDeviceIndex(num_devices - 1);
  PutThenMove(hermes, true);

  hermes->Finalize(true);
}

void TestOrganizeBlob() {
  hermes::Config config = {};
  hermes::InitDefaultConfig(&config);
  config.default_placement_policy = hapi::PlacementPolicy::kRoundRobin;
  HermesPtr hermes = hermes::InitHermesDaemon(&config);
  SharedMemoryContext *context = &hermes->context_;
  RpcContext *rpc = &hermes->rpc_;

  // Put a Blob in RAM
  hermes::RoundRobinState rr_state;
  rr_state.SetCurrentDeviceIndex(0);
  size_t blob_size = KILOBYTES(20);
  hapi::Bucket bkt("Organize", hermes);
  std::string blob_name = "1";
  hapi::Blob blob(blob_size, 'x');
  hapi::Status status = bkt.Put(blob_name, blob);
  Assert(status.Succeeded());

  // Call OrganizeBlob with 0 importance, meaning the BORG should move all
  // buffers to the lowest tier, resulting in an access score of 0
  f32 epsilon = 0.000001;
  f32 custom_importance = 0;
  bkt.OrganizeBlob(blob_name, epsilon, custom_importance);

  // Wait for organization to complete
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // Get access score and make sure it's 0
  BucketID bucket_id = {};
  bucket_id.as_int = bkt.GetId();
  BlobID blob_id = GetBlobId(context, rpc, blob_name, bucket_id, false);
  Assert(!IsNullBlobId(blob_id));
  hermes::MetadataManager *mdm = GetMetadataManagerFromContext(context);
  std::vector<BufferID> buffer_ids = LocalGetBufferIdList(mdm, blob_id);
  std::vector<BufferInfo> buffer_info = GetBufferInfo(context, rpc, buffer_ids);
  f32 access_score = ComputeBlobAccessScore(context, buffer_info);
  Assert(access_score == 0);

  // Get the Blob and compare it with the original
  hapi::Blob retrieved_blob;
  size_t retrieved_blob_size = bkt.Get(blob_name, retrieved_blob);
  Assert(retrieved_blob_size == blob_size);
  retrieved_blob.resize(retrieved_blob_size);
  bkt.Get(blob_name, retrieved_blob);
  Assert(blob == retrieved_blob);

  bkt.Destroy();

  hermes->Finalize(true);
}

static void TestWriteOnlyBucket() {
  HermesPtr hermes = hermes::InitHermesDaemon(getenv("HERMES_CONF"));
  std::string bkt_name = "WriteOnly";
  VBucket vbkt(bkt_name, hermes);
  Bucket bkt(bkt_name, hermes);

  hapi::WriteOnlyTrait trait;
  vbkt.Attach(&trait);

  const size_t kBlobSize = KILOBYTES(4);
  hapi::Blob blob(kBlobSize);
  std::iota(blob.begin(), blob.end(), 0);

  const int kIters = 128;
  for (int i = 0; i < kIters; ++i) {
    std::string blob_name = "b" + std::to_string(i);
    bkt.Put(blob_name, blob);
    vbkt.Link(blob_name, bkt_name);
  }

  vbkt.Destroy();
  bkt.Destroy();
  hermes->Finalize(true);
}

void TestMinThresholdViolation() {
  hermes::Config config = {};
  InitDefaultConfig(&config);

  size_t cap = MEGABYTES(1);
  config.capacities[0] = cap;
  config.capacities[1] = cap;
  config.capacities[2] = cap;
  config.capacities[3] = cap;

  for (int i = 0; i < config.num_devices; ++i) {
    config.num_slabs[i] = 1;
    config.desired_slab_percentages[i][0] = 1.0;
  }

  f32 min = 0.25f;
  f32 max = 0.75f;
  config.bo_capacity_thresholds[0] = {0, max};
  config.bo_capacity_thresholds[1] = {min, max};
  config.bo_capacity_thresholds[2] = {0, max};

  HermesPtr hermes = hermes::InitHermesDaemon(&config);


  hermes::RoundRobinState rr_state;
  rr_state.SetCurrentDeviceIndex(2);
  hapi::Context ctx;
  ctx.policy = hapi::PlacementPolicy::kRoundRobin;
  Bucket bkt(__func__, hermes, ctx);
  // Blob is big enough to exceed minimum capacity of Target 1
  const size_t kBlobSize = (min * cap) + KILOBYTES(4);
  hapi::Blob blob(kBlobSize, 'q');
  Assert(bkt.Put("1", blob).Succeeded());

  // Let the BORG run. It should move enough data from Target 2 to Target 1 to
  // fill > the minimum capacity threshold
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // Check remaining capacities
  std::vector<TargetID> targets = {{1, 1, 1}, {1, 2, 2}};
  std::vector<u64> capacities =
    GetRemainingTargetCapacities(&hermes->context_, &hermes->rpc_, targets);
  Assert(capacities[0] == cap - kBlobSize + KILOBYTES(4));
  Assert(capacities[1] == cap - KILOBYTES(4));

  bkt.Destroy();
  hermes->Finalize(true);
}

void TestMaxThresholdViolation() {
  hermes::Config config = {};
  InitDefaultConfig(&config);

  size_t cap = MEGABYTES(1);
  config.capacities[0] = cap;
  config.capacities[1] = cap;
  config.capacities[2] = cap;
  config.capacities[3] = cap;

  for (int i = 0; i < config.num_devices; ++i) {
    config.num_slabs[i] = 1;
    config.desired_slab_percentages[i][0] = 1.0;
  }

  f32 min = 0.0f;
  f32 max = 0.75f;
  config.bo_capacity_thresholds[0] = {min, max};
  config.bo_capacity_thresholds[1] = {min, max};
  config.bo_capacity_thresholds[2] = {min, max};

  HermesPtr hermes = hermes::InitHermesDaemon(&config);

  hermes::RoundRobinState rr_state;
  rr_state.SetCurrentDeviceIndex(1);
  hapi::Context ctx;
  ctx.policy = hapi::PlacementPolicy::kRoundRobin;
  Bucket bkt(__func__, hermes, ctx);
  // Exceed maximum capacity of Target 1 by 4KiB
  const size_t kBlobSize = (max * MEGABYTES(1)) + KILOBYTES(4);
  hapi::Blob blob(kBlobSize, 'q');
  Assert(bkt.Put("1", blob).Succeeded());

  // Let the BORG run. It should move 4KiB from Target 1 to 2
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // Check remaining capacities
  std::vector<TargetID> targets = {{1, 1, 1}, {1, 2, 2}};
  std::vector<u64> capacities =
    GetRemainingTargetCapacities(&hermes->context_, &hermes->rpc_, targets);
  Assert(capacities[0] == cap - kBlobSize + KILOBYTES(4));
  Assert(capacities[1] == cap - KILOBYTES(4));

  bkt.Destroy();
  hermes->Finalize(true);
}

int main(int argc, char *argv[]) {
  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  HERMES_ADD_TEST(TestIsBoFunction);
  HERMES_ADD_TEST(TestBackgroundFlush);
  HERMES_ADD_TEST(TestBoMove);
  HERMES_ADD_TEST(TestOrganizeBlob);
  HERMES_ADD_TEST(TestWriteOnlyBucket);
  HERMES_ADD_TEST(TestMinThresholdViolation);
  HERMES_ADD_TEST(TestMaxThresholdViolation);

  MPI_Finalize();

  return 0;
}
