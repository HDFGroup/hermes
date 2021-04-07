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

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include <mpi.h>

#include "hermes.h"
#include "bucket.h"
#include "buffer_pool_internal.h"
#include "metadata_management_internal.h"
#include "utils.h"
#include "test_utils.h"

/**
 * @file buffer_pool_test.cc
 *
 * Tests the functionality of the BufferPool
 */

namespace hapi = hermes::api;
using HermesPtr = std::shared_ptr<hapi::Hermes>;

static void GetSwapConfig(hermes::Config *config) {
  InitDefaultConfig(config);
  // NOTE(chogan): Make capacities small so that a Put of 1MB will go to swap
  // space. After metadata, this configuration gives us 1 4KB RAM buffer.
  config->capacities[0] = KILOBYTES(32);
  config->capacities[1] = 8;
  config->capacities[2] = 8;
  config->capacities[3] = 8;
  config->desired_slab_percentages[0][0] = 1;
  config->desired_slab_percentages[0][1] = 0;
  config->desired_slab_percentages[0][2] = 0;
  config->desired_slab_percentages[0][3] = 0;
  config->arena_percentages[hermes::kArenaType_BufferPool] = 0.5;
  config->arena_percentages[hermes::kArenaType_MetaData] = 0.5;
}

static void TestGetBuffers() {
  using namespace hermes;  // NOLINT(*)

  HermesPtr hermes = InitHermesDaemon();

  SharedMemoryContext *context = &hermes->context_;
  BufferPool *pool = GetBufferPoolFromContext(context);
  TargetID ram_target = testing::DefaultRamTargetId();
  i32 block_size = pool->block_sizes[ram_target.bits.device_id];

  {
    // A request smaller than the block size should return 1 buffer
    PlacementSchema schema{std::make_pair(block_size / 2, ram_target)};
    std::vector<BufferID> ret = GetBuffers(context, schema);
    Assert(ret.size() == 1);
    LocalReleaseBuffers(context, ret);
  }

  {
    // A request larger than the available space should return no buffers
    PlacementSchema schema{std::make_pair(GIGABYTES(3), ram_target)};
    std::vector<BufferID> ret = GetBuffers(context, schema);
    Assert(ret.size() == 0);
  }

  {
    // Get all buffers on the device
    UpdateGlobalSystemViewState(context, &hermes->rpc_);
    std::vector<u64> global_state = GetGlobalDeviceCapacities(context,
                                                              &hermes->rpc_);
    PlacementSchema schema{
      std::make_pair(global_state[ram_target.bits.device_id], ram_target)};
    std::vector<BufferID> ret = GetBuffers(context, schema);
    Assert(ret.size());
    // The next request should fail
    PlacementSchema failed_schema{std::make_pair(1, ram_target)};
    std::vector<BufferID> failed_request = GetBuffers(context, failed_schema);
    Assert(failed_request.size() == 0);
    LocalReleaseBuffers(context, ret);
  }

  hermes->Finalize(true);
}

static void TestGetBandwidths() {
  using namespace hermes;  // NOLINT(*)

  HermesPtr hermes = InitHermesDaemon();

  std::vector<f32> bandwidths = GetBandwidths(&hermes->context_);
  Config config;
  InitDefaultConfig(&config);
  for (size_t i = 0; i < bandwidths.size(); ++i) {
    Assert(bandwidths[i] == config.bandwidths[i]);
    Assert(bandwidths.size() == (size_t)config.num_devices);
  }

  hermes->Finalize(true);
}

static hapi::Status ForceBlobToSwap(hapi::Hermes *hermes, hermes::u64 id,
                                    hapi::Blob &blob, const char *blob_name,
                                    hapi::Context &ctx) {
  using namespace hermes;  // NOLINT(*)
  PlacementSchema schema;
  schema.push_back({blob.size(), testing::DefaultRamTargetId()});
  Blob internal_blob = {};
  internal_blob.data = blob.data();
  internal_blob.size = blob.size();
  hermes::BucketID bucket_id = {};
  bucket_id.as_int = id;
  hapi::Status result = PlaceBlob(&hermes->context_, &hermes->rpc_, schema,
                                  internal_blob, blob_name, bucket_id, ctx);

  return result;
}

/**
 * Fills out @p config to represent one `Device` (RAM) with 2, 4 KB buffers.
 */
static void MakeTwoBufferRAMConfig(hermes::Config *config) {
  InitDefaultConfig(config);
  config->num_devices = 1;
  config->num_targets = 1;
  config->capacities[0] = KILOBYTES(36);
  config->desired_slab_percentages[0][0] = 1;
  config->desired_slab_percentages[0][1] = 0;
  config->desired_slab_percentages[0][2] = 0;
  config->desired_slab_percentages[0][3] = 0;
  config->arena_percentages[hermes::kArenaType_BufferPool] = 0.5;
  config->arena_percentages[hermes::kArenaType_MetaData] = 0.5;
}

static void TestBlobOverwrite() {
  using namespace hermes;  // NOLINT(*)
  hermes::Config config = {};
  MakeTwoBufferRAMConfig(&config);
  HermesPtr hermes = InitHermesDaemon(&config);
  SharedMemoryContext *context = &hermes->context_;
  DeviceID ram_id = 0;
  int slab_index = 0;
  std::atomic<u32> *buffers_available =
    GetAvailableBuffersArray(context, ram_id);
  Assert(buffers_available[slab_index] == 2);

  hapi::Context ctx;
  ctx.policy = hapi::PlacementPolicy::kRandom;
  hapi::Bucket bucket("overwrite", hermes, ctx);

  std::string blob_name("1");
  size_t blob_size = KILOBYTES(2);
  hapi::Blob blob(blob_size, '1');
  hapi::Status status = bucket.Put(blob_name, blob);
  Assert(status.Succeeded());

  Assert(buffers_available[slab_index] == 1);

  // NOTE(chogan): Overwrite the data
  hapi::Blob new_blob(blob_size, '2');
  Assert(bucket.Put(blob_name, new_blob).Succeeded());

  Assert(buffers_available[slab_index] == 1);

  bucket.Destroy();

  hermes->Finalize(true);
}

static void TestSwap() {
  hermes::Config config = {};
  GetSwapConfig(&config);
  HermesPtr hermes = hermes::InitHermesDaemon(&config);

  hapi::Context ctx;
  ctx.policy = hapi::PlacementPolicy::kRandom;
  hapi::Bucket bucket(std::string("swap_bucket"), hermes, ctx);
  size_t data_size = MEGABYTES(1);
  hapi::Blob data(data_size, 'x');
  std::string blob_name("swap_blob");
  hapi::Status status = ForceBlobToSwap(hermes.get(), bucket.GetId(), data,
                                        blob_name.c_str(), ctx);
  Assert(status == hermes::BLOB_IN_SWAP_PLACE);
  // NOTE(chogan): The Blob is in the swap space, but the API behaves as normal.
  Assert(bucket.ContainsBlob(blob_name));

  hapi::Blob get_result;
  size_t blob_size = bucket.Get(blob_name, get_result);
  get_result.resize(blob_size);
  blob_size = bucket.Get(blob_name, get_result);

  Assert(get_result == data);

  bucket.Destroy();

  hermes->Finalize(true);
}

static void TestBufferOrganizer() {
  hermes::Config config = {};
  GetSwapConfig(&config);
  HermesPtr hermes = hermes::InitHermesDaemon(&config);

  hapi::Context ctx;
  ctx.policy = hapi::PlacementPolicy::kRandom;
  hapi::Bucket bucket(std::string("bo_bucket"), hermes, ctx);

  // NOTE(chogan): Fill our single buffer with a blob.
  hapi::Blob data1(KILOBYTES(4), 'x');
  std::string blob1_name("bo_blob1");
  hapi::Status status = bucket.Put(blob1_name, data1);
  Assert(status.Succeeded());
  Assert(bucket.ContainsBlob(blob1_name));

  // NOTE(chogan): Force a second Blob to the swap space.
  hapi::Blob data2(KILOBYTES(4), 'y');
  std::string blob2_name("bo_blob2");
  status = ForceBlobToSwap(hermes.get(), bucket.GetId(), data2,
                           blob2_name.c_str(), ctx);
  Assert(status == hermes::BLOB_IN_SWAP_PLACE);
  Assert(bucket.BlobIsInSwap(blob2_name));

  // NOTE(chogan): Delete the first blob, which will make room for the second,
  // and the buffer organizer should move it from swap space to the hierarchy.
  bucket.DeleteBlob(blob1_name);

  // NOTE(chogan): Give the BufferOrganizer time to finish.
  std::this_thread::sleep_for(std::chrono::seconds(3));

  Assert(bucket.ContainsBlob(blob2_name));
  Assert(!bucket.BlobIsInSwap(blob2_name));

  hapi::Blob get_result;
  size_t blob_size = bucket.Get(blob2_name, get_result);
  get_result.resize(blob_size);
  blob_size = bucket.Get(blob2_name, get_result);

  Assert(get_result == data2);

  bucket.Destroy();

  hermes->Finalize(true);
}

static void TestBufferingFiles(hermes::Config *config,
                               size_t *expected_capacities) {
  const int kNumBlockDevices = 3;

  int num_slabs[] = {
    config->num_slabs[1],
    config->num_slabs[2],
    config->num_slabs[3]
  };

  HermesPtr hermes = hermes::InitHermesDaemon(config);
  hermes->Finalize(true);

  for (int i = 0; i < kNumBlockDevices; ++i) {
    size_t device_total_capacity = 0;
    for (int j = 0; j < num_slabs[i]; ++j) {
      std::string buffering_filename = ("device" + std::to_string(i + 1) +
                                        "_slab" + std::to_string(j) +
                                        ".hermes");

      struct stat st = {};
      Assert(stat(buffering_filename.c_str(), &st) == 0);
      device_total_capacity += st.st_size;
    }
    Assert(device_total_capacity == expected_capacities[i]);
  }
}

static void TestBufferingFileCorrectness() {
  using namespace hermes;  // NOLINT(*)
  {
    Config config = {};
    InitDefaultConfig(&config);
    size_t expected_capacities[] = {
      config.capacities[1],
      config.capacities[2],
      config.capacities[3],
    };
    TestBufferingFiles(&config, expected_capacities);
  }

  {
    Config config = {};
    InitDefaultConfig(&config);
    config.capacities[1] = KILOBYTES(4) * 5;
    config.capacities[2] = KILOBYTES(4) * 5;
    config.capacities[3] = KILOBYTES(4) * 5;
    size_t expected_capacities[] = {
      KILOBYTES(4),
      KILOBYTES(4),
      KILOBYTES(4)
    };
    TestBufferingFiles(&config, expected_capacities);
  }
}

int main(int argc, char **argv) {
  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  TestGetBuffers();
  TestGetBandwidths();
  TestBlobOverwrite();
  TestSwap();
  TestBufferOrganizer();
  TestBufferingFileCorrectness();

  MPI_Finalize();

  return 0;
}
